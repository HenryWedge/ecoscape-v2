"""Finite-file PyFlink pipeline for process discovery and conformance scoring."""

import argparse
import base64
import csv
import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path
from processmining.conformance_checker import ConformanceChecker
from processmining.model_discovery import ModelDiscovery
from processmining.conformance_state import ConformanceState

from pyflink.common import Configuration, Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import KeyedCoProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

DISCOVERY_WINDOW_SIZE = 30

DEFAULT_MAX_APPROX_ERROR = 0.05


def _serialize_model(model):
    return base64.b64encode(pickle.dumps(model)).decode("ascii")




def read_ordered_events(input_path):
    events = []
    with open(input_path, newline="", encoding="utf-8") as input_file:
        reader = csv.DictReader(input_file)
        if reader.fieldnames is None or not {"case", "activity", "timestamp"}.issubset(
            reader.fieldnames
        ):
            raise ValueError("CSV must contain case, activity, and timestamp columns")

        for row_number, row in enumerate(reader, start=2):
            case = (row.get("case") or "").strip()
            activity = (row.get("activity") or "").strip()
            timestamp_text = (row.get("timestamp") or "").strip()
            if not case or not activity or not timestamp_text:
                raise ValueError(f"CSV row {row_number} has an empty required field")

            timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            events.append(
                {"case": case, "activity": activity, "timestamp": timestamp}
            )

    # Python's sort is stable, so equal timestamps retain CSV row order.
    return sorted(events, key=lambda event: event["timestamp"])


def _apply_lossy_counting(state, activity, max_approx_error):
    width = math.ceil(1.0 / max_approx_error)
    state["discovery_event_count"] += 1
    bucket = math.ceil(state["discovery_event_count"] / width)

    if activity in state["discovery_lossy_counts"]:
        state["discovery_lossy_counts"][activity][0] += 1
    else:
        state["discovery_lossy_counts"][activity] = [1, bucket - 1]

    if state["discovery_event_count"] % width == 0:
        state["discovery_lossy_counts"] = {
            name: counter
            for name, counter in state["discovery_lossy_counts"].items()
            if counter[0] + counter[1] > bucket
        }

def _finish_lossy_counting_window(state, max_approx_error):
    width = math.ceil(1.0 / max_approx_error)
    bucket = math.ceil(state["discovery_event_count"] / width)
    state["discovery_lossy_counts"] = {
        name: counter
        for name, counter in state["discovery_lossy_counts"].items()
        if counter[0] + counter[1] > bucket
    }

class DiscoveryOperator(KeyedProcessFunction):
    def __init__(self, max_approx_error):
        self.max_approx_error = max_approx_error
        self.discovery_state = None

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("discovery-state", Types.PICKLED_BYTE_ARRAY())
        self.discovery_state = runtime_context.get_state(descriptor)

    def process_element(self, event, context):
        stored_state = self.discovery_state.value()
        state = pickle.loads(stored_state) if stored_state is not None else None
        if state is None:
            state = {
                "discovery_event_count": 0,
                "discovery_lossy_counts": {},
                "discovery_events": [],
                "model_version": 0,
            }

        _apply_lossy_counting(state, event["activity"], self.max_approx_error)
        state["discovery_events"].append(event)
        sequence = event["sequence"]
        controls = []

        if len(state["discovery_events"]) == DISCOVERY_WINDOW_SIZE:
            _finish_lossy_counting_window(state, self.max_approx_error)
            retained_activities = set(state["discovery_lossy_counts"])
            filtered_events = [
                item
                for item in state["discovery_events"]
                if item["activity"] in retained_activities
            ]
            if filtered_events:
                model = ModelDiscovery().discover_heuristics_miner(filtered_events)
                state["model_version"] += 1
                model_snapshot = {
                    "net": model[0],
                    "initial_marking": model[1],
                    "final_marking": model[2],
                }
                controls.append(json.dumps(
                    {
                        "type": "model",
                        "version": state["model_version"],
                        "effective_after": sequence,
                        "model": _serialize_model(model_snapshot),
                    },
                    separators=(",", ":"),
                ))
            state["discovery_events"] = []
            state["discovery_event_count"] = 0
            state["discovery_lossy_counts"] = {}

        self.discovery_state.update(pickle.dumps(state))
        # The progress marker lets conformance wait for discovery's decision at
        # each model boundary, even when the branches execute at different speeds.
        controls.append(
            json.dumps(
                {"type": "progress", "sequence": sequence}, separators=(",", ":")
            )
        )
        yield from controls


class ConformanceOperator(KeyedCoProcessFunction):
    def __init__(self):
        self.conformance_state = None

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "conformance-state", Types.PICKLED_BYTE_ARRAY()
        )
        self.conformance_state = runtime_context.get_state(descriptor)

    def process_element1(self, event, context):
        state = ConformanceState.from_bytes(self.conformance_state.value())
        results = state.add_event(event)
        self.conformance_state.update(state.to_bytes())
        yield from results

    def process_element2(self, control, context):
        control = json.loads(control)
        state = ConformanceState.from_bytes(self.conformance_state.value())
        results = state.apply_control(control)
        self.conformance_state.update(state.to_bytes())
        yield from results


def run_pipeline(input_path, output_path, checkpoint_dir, max_approx_error):
    events = read_ordered_events(input_path)
    serializable_events = [
        {
            "sequence": sequence,
            "case": event["case"],
            "activity": event["activity"],
            "timestamp": event["timestamp"].isoformat(),
        }
        for sequence, event in enumerate(events, start=1)
    ]
    configuration = Configuration()
    configuration.set_string("state.backend.type", "hashmap")
    configuration.set_string("execution.checkpointing.dir", checkpoint_dir)
    environment = StreamExecutionEnvironment.get_execution_environment(configuration)
    environment.set_parallelism(1)
    environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    environment.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)

    source = environment.from_collection(serializable_events)
    keyed_source = source.key_by(lambda _event: "global", key_type=Types.STRING())
    control_stream = keyed_source.process(
        DiscoveryOperator(max_approx_error),
        output_type=Types.STRING(),
    )
    scores = (
        keyed_source.connect(
            control_stream.key_by(lambda _control: "global", key_type=Types.STRING())
        )
        .process(
            ConformanceOperator(),
            output_type=Types.STRING(),
        )
    )

    output_file_config = (
        OutputFileConfig.builder()
        .with_part_prefix("scores")
        .with_part_suffix(".jsonl")
        .build()
    )
    sink = (
        FileSink.for_row_format(output_path, Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(output_file_config)
        .build()
    )
    scores.sink_to(sink)
    environment.execute("conformance-checking-outlier-detection")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="input.csv", help="Input CSV file")
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory for JSONL part files"
    )
    parser.add_argument(
        "--checkpoint-dir",
        default="file:///tmp/outlier-detection-checkpoints",
        help="Flink checkpoint storage URI accessible to the runtime",
    )
    parser.add_argument(
        "--max-approx-error",
        type=float,
        default=DEFAULT_MAX_APPROX_ERROR,
        help="Lossy-counting approximation error (default: 0.05)",
    )
    args = parser.parse_args()
    if not 0 < args.max_approx_error <= 1:
        parser.error("--max-approx-error must be greater than 0 and at most 1")
    if not Path(args.input).is_file():
        parser.error(f"input CSV does not exist: {args.input}")

    run_pipeline(
        args.input,
        args.output,
        args.checkpoint_dir,
        args.max_approx_error,
    )


if __name__ == "__main__":
    main()
