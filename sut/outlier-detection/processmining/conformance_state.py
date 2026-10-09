import pickle
import base64
from processmining.conformance_checker import ConformanceChecker
import json
CONFORMANCE_WINDOW_SIZE = 10

class ConformanceState:
    def __init__(self):
        self.models = {}
        self.current_model_version = None
        self.traces = {}
        self.conformance_count = 0

    def _deserialize_model(self, encoded_model):
        return pickle.loads(base64.b64decode(encoded_model))

    @classmethod
    def from_bytes(cls, serialized_state):
        return pickle.loads(serialized_state) if serialized_state is not None else cls()

    def to_bytes(self):
        return pickle.dumps(self)

    def add_event(self, event):
        if self.current_model_version is None:
            return []

        case = event["case"]
        if case not in self.traces:
            self.traces[case] = {
                "activities": [],
                "model_version": self.current_model_version,
            }
        self.traces[case]["activities"].append(event["activity"])

        self.conformance_count += 1
        if self.conformance_count < CONFORMANCE_WINDOW_SIZE:
            return []

        self.conformance_count = 0
        scores = []
        for trace_case in sorted(self.traces):
            trace = self.traces[trace_case]
            scores.append(
                {
                    "case": trace_case,
                    "score": ConformanceChecker().calculate_alignment_cost(
                        trace["activities"], self.models[trace["model_version"]]
                    ),
                }
            )
        return [json.dumps(scores, separators=(",", ":"))]

    def add_model(self, model_update):
        version = model_update["version"]
        self.models[version] = self._deserialize_model(model_update["model"])
        self.current_model_version = version
