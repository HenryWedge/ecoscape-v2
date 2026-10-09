"""Finite-file PyFlink pipeline for process discovery and conformance scoring."""

import argparse
import base64
import csv
import json
import math
import pickle
from datetime import datetime, timezone
from pathlib import Path
from processmining.model_discovery import ModelDiscovery
from processmining.conformance_state import ConformanceState

from pyflink.common import Configuration, Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    WindowFunction,
)
from pyflink.datastream.state import ValueStateDescriptor

DISCOVERY_WINDOW_SIZE = 10
CONFORMANCE_WINDOW_SIZE = 2

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
                {
                    "case": case,
                    "activity": activity,
                    "timestamp": timestamp,
                }
            )

    # Python's sort is stable, so equal timestamps retain CSV row order.
    ordered_events = sorted(events, key=lambda event: event["timestamp"])
    for sequence, event in enumerate(ordered_events):
        event["sequence"] = sequence
    return ordered_events


def _filter_discovery_events(events, max_approx_error):
    width = math.ceil(1.0 / max_approx_error)
    counts = {}
    for event_number, event in enumerate(events, start=1):
        bucket = math.ceil(event_number / width)
        activity = event["activity"]
        if activity in counts:
            counts[activity][0] += 1
        else:
            counts[activity] = [1, bucket - 1]

        if event_number % width == 0:
            counts = {
                name: counter
                for name, counter in counts.items()
                if counter[0] + counter[1] > bucket
            }

    final_bucket = math.ceil(len(events) / width)
    counts = {
        name: counter
        for name, counter in counts.items()
        if counter[0] + counter[1] > final_bucket
    }
    retained_activities = set(counts)
    return [event for event in events if event["activity"] in retained_activities]


class CollectDiscoveryWindow(WindowFunction):
    def apply(self, key, window, events):
        yield list(events)


class DiscoverAndTagWindowOperator(KeyedProcessFunction):
    def __init__(self, max_approx_error):
        self.max_approx_error = max_approx_error

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("current-discovery-model", Types.STRING())
        self.current_discovery_model = runtime_context.get_state(descriptor)

    def process_element(self, events, context):
        current_model_json = self.current_discovery_model.value()
        tagged_events = []

        if current_model_json is not None:
            current_model = json.loads(current_model_json)
            for event_number, event in enumerate(events):
                tagged_event = dict(event)
                tagged_event["model_version"] = current_model["version"]
                if event_number == 0:
                    tagged_event["model_update"] = current_model
                tagged_events.append(tagged_event)

        filtered_events = _filter_discovery_events(events, self.max_approx_error)
        if filtered_events:
            model = ModelDiscovery().discover_heuristics_miner(filtered_events)
            model_update = {
                "version": events[-1]["sequence"] // DISCOVERY_WINDOW_SIZE + 1,
                "model": _serialize_model(
                    {
                        "net": model[0],
                        "initial_marking": model[1],
                        "final_marking": model[2],
                    }
                ),
            }
            self.current_discovery_model.update(
                json.dumps(model_update, separators=(",", ":"))
            )

        yield from tagged_events


class CollectConformanceWindow(WindowFunction):
    def apply(self, key, window, events):
        yield list(events)


class ConformanceOperator(KeyedProcessFunction):
    def __init__(self):
        self.conformance_state = None

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor(
            "conformance-state", Types.PICKLED_BYTE_ARRAY()
        )
        self.conformance_state = runtime_context.get_state(descriptor)

    def process_element(self, event_window, context):
        state = ConformanceState.from_bytes(self.conformance_state.value())
        results = state.add_window(event_window)
        self.conformance_state.update(state.to_bytes())
        yield from results


def run_pipeline(input_path, output_path, checkpoint_dir, max_approx_error):
    events = read_ordered_events(input_path)
    serializable_events = [
        {
            "case": event["case"],
            "activity": event["activity"],
            "timestamp": event["timestamp"].isoformat(),
            "sequence": event["sequence"],
        }
        for event in events
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

    discovery_windows = keyed_source.count_window(DISCOVERY_WINDOW_SIZE).apply(
        CollectDiscoveryWindow(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    accepted_events = discovery_windows.key_by(
        lambda _events: "global", key_type=Types.STRING()
    ).process(
        DiscoverAndTagWindowOperator(max_approx_error),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    conformance_windows = (
        accepted_events
        .key_by(lambda _event: "global", key_type=Types.STRING())
        .count_window(CONFORMANCE_WINDOW_SIZE)
        .apply(
            CollectConformanceWindow(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )
    scores = conformance_windows.key_by(
        lambda _window: "global", key_type=Types.STRING()
    ).process(
        ConformanceOperator(),
        output_type=Types.STRING(),
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
