from pm4py.algo.conformance.alignments.petri_net import algorithm as alignments
from pm4py.algo.conformance.alignments.petri_net.variants import state_equation_a_star
from pm4py.objects.log.obj import Event, Trace

class ConformanceChecker:

    def calculate_alignment_cost(self, activities, model):
        trace = Trace([Event({"concept:name": activity}) for activity in activities])
        net = model["net"]
        parameters = {
            state_equation_a_star.Parameters.PARAM_TRACE_COST_FUNCTION: [1] * len(trace),
            state_equation_a_star.Parameters.PARAM_MODEL_COST_FUNCTION: {
                transition: (0 if transition.label is None else 1)
                for transition in net.transitions
            },
            state_equation_a_star.Parameters.PARAM_SYNC_COST_FUNCTION: {
                transition: 0 for transition in net.transitions
            },
        }
        result = alignments.apply_trace(
            trace,
            net,
            model["initial_marking"],
            model["final_marking"],
            parameters=parameters,
        )
        return result["cost"]
