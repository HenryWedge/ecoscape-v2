"""Finite-file PyFlink pipeline for process discovery and conformance scoring."""

import argparse
import base64
import json
import math
import pickle
from pathlib import Path
from processmining.model_discovery import ModelDiscovery
from processmining.conformance_checker import ConformanceChecker
from processmining.input import FileInput
from processmining.data_models import DiscoveredModel, DiscoveryWindow, ProcessEvent

from pyflink.common import Configuration, Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import (
    KeyedBroadcastProcessFunction,
    KeyedProcessFunction,
    WindowFunction,
    MapFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

DISCOVERY_WINDOW_SIZE = 10
DEFAULT_MAX_APPROX_ERROR = 0.05
MODEL_UPDATE_OUTPUT_TAG = OutputTag(
    "model-updates", Types.PICKLED_BYTE_ARRAY()
)


def _serialize_model(model):
    return base64.b64encode(pickle.dumps(model)).decode("ascii")

def _filter_discovery_events(events, max_approx_error):
    width = math.ceil(1.0 / max_approx_error)
    counts = {}
    for event_number, event in enumerate(events, start=1):
        bucket = math.ceil(event_number / width)
        activity = event.activity
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
    return [event for event in events if event.activity in retained_activities]


class CreateDirectlyFollowsWindowOperator(WindowFunction):
    def __init__(self, max_approx_error):
        self.max_approx_error = max_approx_error

    def apply(self, key, window, events):
        events = list(events)
        filtered_events = _filter_discovery_events(events, self.max_approx_error)
        directly_follows_data = None
        if filtered_events:
            directly_follows_data = ModelDiscovery().create_directly_follows_data(
                filtered_events
            )
        yield DiscoveryWindow(
            events=events,
            directly_follows_data=directly_follows_data,
        )


class CreateProcessEventOperator(MapFunction):
    def map(self, event_data):
        case, activity, timestamp, sequence = event_data
        return ProcessEvent(
            case=case,
            activity=activity,
            timestamp=timestamp,
            sequence=sequence,
        )


class DiscoverAndTagWindowOperator(KeyedProcessFunction):
    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("current-discovery-model", Types.STRING())
        self.current_discovery_model = runtime_context.get_state(descriptor)

    def process_element(self, discovery_window, context):
        events = discovery_window.events
        current_model_json = self.current_discovery_model.value()
        tagged_events = []

        if current_model_json is not None:
            for event in events:
                tagged_event = ProcessEvent(
                    case=event.case,
                    activity=event.activity,
                    timestamp=event.timestamp,
                    sequence=event.sequence,
                )
                tagged_events.append(tagged_event)

        directly_follows_data = discovery_window.directly_follows_data
        if directly_follows_data is not None:
            model = ModelDiscovery().discover_heuristics_miner(directly_follows_data)
            model_update = DiscoveredModel(
                version=events[-1].sequence // DISCOVERY_WINDOW_SIZE + 1,
                model=_serialize_model(
                    {
                        "net": model[0],
                        "initial_marking": model[1],
                        "final_marking": model[2],
                    }
                ),
            )
            self.current_discovery_model.update(
                json.dumps(model_update.__dict__, separators=(",", ":"))
            )

        for tagged_event in tagged_events:
            yield tagged_event
        if directly_follows_data is not None:
            yield MODEL_UPDATE_OUTPUT_TAG, model_update


class ConformanceOperator(KeyedBroadcastProcessFunction):
    def __init__(self, models_descriptor, latest_model_descriptor):
        self.models_descriptor = models_descriptor
        self.latest_model_descriptor = latest_model_descriptor
        self.models = {}

    def open(self, runtime_context):
        alignment_state_descriptor = ValueStateDescriptor(
            "case-prefix-alignment", Types.PICKLED_BYTE_ARRAY()
        )
        model_version_descriptor = ValueStateDescriptor(
            "case-model-version", Types.INT()
        )
        self.alignment_state = runtime_context.get_state(alignment_state_descriptor)
        self.model_version_state = runtime_context.get_state(model_version_descriptor)

    def process_broadcast_element(self, model_update, context):
        context.get_broadcast_state(self.models_descriptor).put(
            model_update.version, model_update.model
        )
        context.get_broadcast_state(self.latest_model_descriptor).put(
            "latest", model_update.version
        )
        self.models.pop(model_update.version, None)

    def process_element(self, event, context):
        model_version = self.model_version_state.value()
        if model_version is None:
            model_version = context.get_broadcast_state(
                self.latest_model_descriptor
            ).get("latest")
            if model_version is None:
                raise RuntimeError(
                    "No discovered model is available for conformance checking"
                )
            self.model_version_state.update(model_version)

        encoded_model = context.get_broadcast_state(self.models_descriptor).get(
            model_version
        )
        if model_version not in self.models:
            if encoded_model is None:
                raise RuntimeError(
                    f"Discovered model {model_version} is not available"
                )
            self.models[model_version] = pickle.loads(
                base64.b64decode(encoded_model)
            )

        alignment_state = self.alignment_state.value()
        if alignment_state is None:
            alignment_state = ConformanceChecker().create_prefix_alignment_state(
                event.case, self.models[model_version]
            )

        score = alignment_state.add_event(event.activity)
        self.alignment_state.update(alignment_state)
        yield json.dumps(
            {"case": event.case, "event": event.sequence, "score": score},
            separators=(",", ":"),
        )


class CollectLatestScoresOperator(KeyedProcessFunction):
    def open(self, runtime_context):
        descriptor = MapStateDescriptor(
            "latest-scores", Types.STRING(), Types.STRING()
        )
        self.latest_scores = runtime_context.get_map_state(descriptor)

    def process_element(self, score_json, context):
        score = json.loads(score_json)
        self.latest_scores.put(score["case"], score_json)

        scores = [
            json.loads(self.latest_scores.get(case))
            for case in sorted(self.latest_scores.keys())
        ]
        yield json.dumps({"scores": scores}, separators=(",", ":"))


def run_pipeline(input_path, output_path, checkpoint_dir, max_approx_error):
    events = FileInput().read_ordered_events(input_path)
    event_data = [
        (event.case, event.activity, event.timestamp, event.sequence)
        for event in events
    ]
    configuration = Configuration()
    configuration.set_string("state.backend.type", "hashmap")
    configuration.set_string("execution.checkpointing.dir", checkpoint_dir)
    environment = StreamExecutionEnvironment.get_execution_environment(configuration)
    environment.set_parallelism(4)
    environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    environment.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)

    source = environment.from_collection(event_data).set_parallelism(1).map(
        CreateProcessEventOperator(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    ).set_parallelism(1)

    keyed_source = source.key_by(lambda _event: "global", key_type=Types.STRING())

    directly_follows_windows = keyed_source.count_window(
        DISCOVERY_WINDOW_SIZE
    ).apply(
        CreateDirectlyFollowsWindowOperator(max_approx_error),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )
    discovery_outputs = directly_follows_windows.key_by(
        lambda _window: "global", key_type=Types.STRING()
    ).process(
        DiscoverAndTagWindowOperator(),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    )

    accepted_events = discovery_outputs
    model_updates = discovery_outputs.get_side_output(MODEL_UPDATE_OUTPUT_TAG)
    models_descriptor = MapStateDescriptor(
        "discovered-models", Types.INT(), Types.STRING()
    )
    latest_model_descriptor = MapStateDescriptor(
        "latest-model-version", Types.STRING(), Types.INT()
    )

    scores = accepted_events.key_by(
        lambda event: event.case, key_type=Types.STRING()
    ).connect(
        model_updates.broadcast(models_descriptor, latest_model_descriptor)
    ).process(
        ConformanceOperator(models_descriptor, latest_model_descriptor),
        output_type=Types.STRING(),
    )

    cumulative_scores = scores.key_by(
        lambda _score: "global", key_type=Types.STRING()
    ).process(
        CollectLatestScoresOperator(),
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
    cumulative_scores.sink_to(sink)
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
