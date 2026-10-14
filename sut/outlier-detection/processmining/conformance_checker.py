import traceback

from pybeamline.algorithms.conformance.prefix_alignments import (
    prefix_alignments_conformance,
)
from pybeamline.bevent import BEvent


class PrefixAlignmentState:
    """Incremental prefix-alignment state for one case and one model."""

    def __init__(self, case, alignment_operator):
        self.case = case
        self.alignment_operator = alignment_operator
        self.last_successful_event = None

    def _get_alignment_entry_diagnostics(self, alignment_entry):
        return {
            "move": repr(getattr(alignment_entry, "alignment_step", None)),
            "marking": repr(getattr(alignment_entry, "marking", None)),
            "cost": repr(getattr(alignment_entry, "cost", None)),
        }

    def _get_alignment_diagnostics(self):
        try:
            case_state = self.alignment_operator.get_trace_conformance(self.case)
            if case_state is None:
                return {"state_available": False}

            alignment = getattr(case_state, "alignment", None)
            return {
                "state_available": True,
                "alignment_entries": [
                    self._get_alignment_entry_diagnostics(entry)
                    for entry in alignment[-5:]
                ]
                if alignment
                else [],
            }
        except Exception as diagnostic_error:
            return {
                "inspection_error": (
                    f"{type(diagnostic_error).__name__}: {diagnostic_error}"
                )
            }

    def _get_failure_frame_diagnostics(self, error):
        diagnostic_frames = []
        traceback_frame = error.__traceback__
        while traceback_frame is not None:
            frame = traceback_frame.tb_frame
            local_variables = frame.f_locals
            filename = frame.f_code.co_filename

            if "prefix_alignments_conformance.py" in filename or "semantics.py" in filename:
                frame_diagnostics = {
                    "file": filename,
                    "function": frame.f_code.co_name,
                    "line": traceback_frame.tb_lineno,
                }

                if "model_move" in local_variables:
                    frame_diagnostics["model_move"] = repr(
                        local_variables["model_move"]
                    )
                if "alignment_state" in local_variables:
                    alignment_state = local_variables["alignment_state"]
                    frame_diagnostics["alignment_state"] = {
                        "step": repr(getattr(alignment_state, "alignment_step", None)),
                        "marking": repr(getattr(alignment_state, "marking", None)),
                        "cost": repr(getattr(alignment_state, "cost", None)),
                    }
                if "m" in local_variables:
                    frame_diagnostics["marking_argument"] = repr(local_variables["m"])
                if "t" in local_variables:
                    transition = local_variables["t"]
                    frame_diagnostics["transition"] = {
                        "name": repr(getattr(transition, "name", None)),
                        "label": repr(getattr(transition, "label", None)),
                    }
                if "a" in local_variables:
                    arc = local_variables["a"]
                    frame_diagnostics["arc"] = {
                        "source": repr(getattr(arc, "source", None)),
                        "target": repr(getattr(arc, "target", None)),
                        "weight": repr(getattr(arc, "weight", None)),
                    }
                if "pn" in local_variables:
                    petri_net = local_variables["pn"]
                    frame_diagnostics["petri_net"] = {
                        "name": repr(getattr(petri_net, "name", None)),
                        "place_count": len(getattr(petri_net, "places", ())),
                        "transition_count": len(getattr(petri_net, "transitions", ())),
                    }

                diagnostic_frames.append(frame_diagnostics)

            traceback_frame = traceback_frame.tb_next

        return diagnostic_frames

    def add_event(self, activity, sequence=None, model_version=None):
        event_details = (
            f"case={self.case!r}, sequence={sequence}, activity={activity!r}, "
            f"model_version={model_version}"
        )
        diagnostics_before = self._get_alignment_diagnostics()

        try:
            self.alignment_operator.transform(BEvent(activity, self.case))
        except Exception as error:
            print(
                "Prefix alignment transform failed: "
                f"{event_details}, before={diagnostics_before!r}, "
                f"after={self._get_alignment_diagnostics()!r}, "
                f"last_successful_event={self.last_successful_event!r}, "
                f"failure_frame_locals={self._get_failure_frame_diagnostics(error)!r}, "
                f"traceback={traceback.format_exc()!r}"
            )
            raise

        case_state = self.alignment_operator.get_trace_conformance(self.case)
        alignment = getattr(case_state, "alignment", None)
        latest_alignment_entry = alignment[-1] if alignment else None
        latest_marking = getattr(latest_alignment_entry, "marking", None)
        self.last_successful_event = {
            "sequence": sequence,
            "activity": activity,
            "model_version": model_version,
        }
        if latest_alignment_entry is not None and latest_marking is None:
            print(
                "Prefix alignment marking became None after successful transform: "
                f"{event_details}, before={diagnostics_before!r}, "
                f"after={self._get_alignment_diagnostics()!r}, "
                f"last_successful_event={self.last_successful_event!r}"
            )
        return latest_alignment_entry.cost


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
