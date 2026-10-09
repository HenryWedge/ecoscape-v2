import pickle
import base64
from processmining.conformance_checker import ConformanceChecker
import json

class ConformanceState:
    def __init__(self):
        self.models = {}
        self.traces = {}

    def _deserialize_model(self, encoded_model):
        return pickle.loads(base64.b64decode(encoded_model))

    @classmethod
    def from_bytes(cls, serialized_state):
        return pickle.loads(serialized_state) if serialized_state is not None else cls()

    def to_bytes(self):
        return pickle.dumps(self)

    def add_window(self, events):
        updated_cases = set()
        for event in events:
            if event.model_update is not None:
                self.add_model(event.model_update)

            model_version = event.model_version
            case = event.case
            if case not in self.traces:
                self.traces[case] = {
                    "activities": [],
                    "model_version": model_version,
                }
            self.traces[case]["activities"].append(event.activity)
            updated_cases.add(case)

        scores = []
        for trace_case in sorted(updated_cases):
            trace = self.traces[trace_case]
            scores.append(
                {
                    "case": trace_case,
                    "score": ConformanceChecker().calculate_alignment_cost(
                        trace["activities"], self.models[trace["model_version"]]
                    ),
                }
            )
        return [
            json.dumps(
                {
                    "window": {
                        "first_event": events[0].sequence,
                        "last_event": events[-1].sequence,
                    },
                    "scores": scores,
                },
                separators=(",", ":"),
            )
        ]

    def add_model(self, model_update):
        self.models[model_update.version] = self._deserialize_model(model_update.model)
