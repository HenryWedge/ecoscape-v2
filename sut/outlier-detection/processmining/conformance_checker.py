from pybeamline.algorithms.conformance.prefix_alignments import (
    prefix_alignments_conformance,
)
from pybeamline.bevent import BEvent


class PrefixAlignmentState:
    """Incremental prefix-alignment state for one case and one model."""

    def __init__(self, case, alignment_operator):
        self.case = case
        self.alignment_operator = alignment_operator

    def add_event(self, activity):
        self.alignment_operator.transform(BEvent(activity, self.case))
        case_state = self.alignment_operator.get_trace_conformance(self.case)
        return case_state.alignment[-1].cost


class ConformanceChecker:

    def create_prefix_alignment_state(self, case, model):
        net = model["net"]
        log_cost_function = {
            transition.label: 1
            for transition in net.transitions
            if transition.label is not None
        }
        model_cost_function = {
            transition: (0 if transition.label is None else 1)
            for transition in net.transitions
        }
        alignment_operator = prefix_alignments_conformance(
            net,
            model["initial_marking"],
            model["final_marking"],
            revert_num=None,
            log_cost_fun=log_cost_function,
            model_cost_fun=model_cost_function,
        )
        return PrefixAlignmentState(case, alignment_operator)

    def calculate_alignment_cost(self, activities, model):
        alignment_state = self.create_prefix_alignment_state("case", model)
        score = 0.0
        for activity in activities:
            score = alignment_state.add_event(activity)
        return score
