import heapq
from itertools import count

from pm4py.objects.petri_net.semantics import execute, is_enabled


class PrefixAlignmentRecalculator:
    """Calculate prefix-alignment costs without targeting the model's final marking."""

    MAX_EXPLORED_STATES = 100_000

    def calculate_cost(self, activities, model):
        net = model["net"]
        initial_marking = model["initial_marking"]
        ordered_places = sorted(net.places, key=lambda place: place.name)
        activity_sequence = tuple(activities)

        def state_key(activity_index, marking):
            return activity_index, tuple(
                marking.get(place, 0) for place in ordered_places
            )

        queue = []
        insertion_order = count()
        initial_state = (0, initial_marking)
        initial_key = state_key(*initial_state)
        lowest_cost_by_state = {initial_key: 0}
        heapq.heappush(queue, (0, next(insertion_order), initial_state))

        explored_states = 0
        while queue:
            cost, _, (activity_index, marking) = heapq.heappop(queue)
            current_key = state_key(activity_index, marking)
            if cost != lowest_cost_by_state.get(current_key):
                continue

            explored_states += 1
            if explored_states > self.MAX_EXPLORED_STATES:
                raise RuntimeError(
                    "Prefix alignment search exceeded "
                    f"{self.MAX_EXPLORED_STATES} states; the Petri net may be unbounded"
                )

            if activity_index == len(activity_sequence):
                return float(cost)

            self._add_search_state(
                queue,
                lowest_cost_by_state,
                insertion_order,
                activity_index + 1,
                marking,
                cost + 1,
                ordered_places,
            )

            for transition in net.transitions:
                if not is_enabled(transition, net, marking):
                    continue

                next_marking = execute(transition, net, marking)
                transition_label = transition.label
                if transition_label is None:
                    model_move_cost = 0
                else:
                    model_move_cost = 1

                self._add_search_state(
                    queue,
                    lowest_cost_by_state,
                    insertion_order,
                    activity_index,
                    next_marking,
                    cost + model_move_cost,
                    ordered_places,
                )

                if transition_label == activity_sequence[activity_index]:
                    self._add_search_state(
                        queue,
                        lowest_cost_by_state,
                        insertion_order,
                        activity_index + 1,
                        next_marking,
                        cost,
                        ordered_places,
                    )

        raise RuntimeError("No prefix alignment could be found for the observed events")

    def _add_search_state(
        self,
        queue,
        lowest_cost_by_state,
        insertion_order,
        activity_index,
        marking,
        cost,
        ordered_places,
    ):
        marking_key = tuple(marking.get(place, 0) for place in ordered_places)
        state_key = activity_index, marking_key
        if cost >= lowest_cost_by_state.get(state_key, float("inf")):
            return

        lowest_cost_by_state[state_key] = cost
        heapq.heappush(
            queue,
            (cost, next(insertion_order), (activity_index, marking)),
        )
