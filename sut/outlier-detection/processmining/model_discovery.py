from collections import Counter, defaultdict
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from processmining.data_models import DirectlyFollowsData, ProcessEvent

class ModelDiscovery:
    def create_directly_follows_data(
        self, events: list[ProcessEvent]
    ) -> DirectlyFollowsData:
        traces_by_case = defaultdict(list)
        for event in events:
            traces_by_case[event.case].append((event.sequence, event.activity))

        directly_follows_graph = Counter()
        activities_occurrences = Counter()
        start_activities = Counter()
        end_activities = Counter()

        for trace_events in traces_by_case.values():
            activity_sequence = [
                activity
                for _, activity in sorted(trace_events, key=lambda event: event[0])
            ]
            if not activity_sequence:
                continue

            activities_occurrences.update(activity_sequence)
            start_activities.update([activity_sequence[0]])
            end_activities.update([activity_sequence[-1]])
            directly_follows_graph.update(zip(activity_sequence, activity_sequence[1:]))

        return DirectlyFollowsData(
            directly_follows_graph=directly_follows_graph,
            activities=sorted(activities_occurrences),
            activities_occurrences=activities_occurrences,
            start_activities=start_activities,
            end_activities=end_activities,
        )

    def discover_heuristics_miner(self, directly_follows_data: DirectlyFollowsData):
        return heuristics_miner.apply_dfg(
            directly_follows_data.directly_follows_graph,
            activities=directly_follows_data.activities,
            activities_occurrences=directly_follows_data.activities_occurrences,
            start_activities=directly_follows_data.start_activities,
            end_activities=directly_follows_data.end_activities,
        )
