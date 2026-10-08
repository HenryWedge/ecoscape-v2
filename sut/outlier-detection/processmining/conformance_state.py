import pickle
import base64
from processmining.conformance_checker import ConformanceChecker
import json
CONFORMANCE_WINDOW_SIZE = 10

class ConformanceState:
    def __init__(self):
        self.pending_events = {}
        self.next_sequence = 1
        self.progress = 0
        self.models = {}
        self.effective_model_version = None
        self.first_model_version = None
        self.model_effective_after = {}
        self.traces = {}
        self.case_models = {}
        self.conformance_count = 0

    def _deserialize_model(self, encoded_model):
        return pickle.loads(base64.b64decode(encoded_model))

    @classmethod
    def from_bytes(cls, serialized_state):
        return pickle.loads(serialized_state) if serialized_state is not None else cls()

    def to_bytes(self):
        return pickle.dumps(self)

    def add_event(self, event):
        self.pending_events[event["sequence"]] = event
        return self._drain()

    def apply_control(self, control):
        if control["type"] == "model":
            version = control["version"]
            self.models[version] = self._deserialize_model(control["model"])
            self.model_effective_after[version] = control["effective_after"]
            if self.first_model_version is None:
                self.first_model_version = version
            for case, pinned_version in self.case_models.items():
                if pinned_version is None:
                    self.case_models[case] = self.first_model_version
        else:
            self.progress = max(self.progress, control["sequence"])

        return self._drain()

    def _drain(self):
        results = []
        while (
                self.next_sequence <= self.progress
                and self.next_sequence in self.pending_events
        ):
            event = self.pending_events.pop(self.next_sequence)
            sequence = self.next_sequence
            for version in sorted(self.model_effective_after):
                if self.model_effective_after[version] < sequence:
                    self.effective_model_version = version

            case = event["case"]
            if case not in self.traces:
                self.traces[case] = []
                self.case_models[case] = (
                        self.effective_model_version or self.first_model_version
                )
            self.traces[case].append(event["activity"])

            self.conformance_count += 1
            if self.conformance_count == CONFORMANCE_WINDOW_SIZE:
                self.conformance_count = 0
                if self.effective_model_version is not None:
                    scores = []
                    for trace_case in sorted(self.traces):
                        version = self.case_models[trace_case]
                        if version is not None:
                            scores.append(
                                {
                                    "case": trace_case,
                                    "score": ConformanceChecker().calculate_alignment_cost(
                                        self.traces[trace_case], self.models[version]
                                    ),
                                }
                            )
                    results.append(json.dumps(scores, separators=(",", ":")))

            self.next_sequence += 1

        return results
