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

from pyflink.common import Configuration, Types, WatermarkStrategy, Time, Duration
from pyflink.common.serialization import Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
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
from pyflink.datastream.state import (
    ListStateDescriptor,
    MapStateDescriptor,
    ValueStateDescriptor,
)

DISCOVERY_WINDOW_SIZE = 20
DEFAULT_MAX_APPROX_ERROR = 0.05
MODEL_UPDATE_OUTPUT_TAG = OutputTag(
    "model-updates", Types.PICKLED_BYTE_ARRAY()
)

class CreateDirectlyFollowsWindowOperator(WindowFunction):
    def __init__(self, max_approx_error):
        self.max_approx_error = max_approx_error

    def apply(self, key, window, events):
        events = sorted(events, key=lambda event: event.timestamp)
        filtered_events = self._filter_discovery_events(events, self.max_approx_error)
        directly_follows_data = None
        if filtered_events:
            directly_follows_data = ModelDiscovery().create_directly_follows_data(
                filtered_events
            )
        yield DiscoveryWindow(
            events=events,
            directly_follows_data=directly_follows_data,
        )

    def _filter_discovery_events(self, events, max_approx_error):
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


class CreateProcessEventOperator(MapFunction):
    def map(self, event_data):
        case, activity, timestamp, sequence = event_data
        print(f"Before key_by: sequence={sequence}, case={case}")
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

    def _serialize_model(self, model):
        return base64.b64encode(pickle.dumps(model)).decode("ascii")

    def process_element(self, discovery_window, context):
        events = discovery_window.events
        current_model_json = self.current_discovery_model.value()
        tagged_events = []

        if current_model_json is not None:
            current_model_version = json.loads(current_model_json)["version"]
            for event in events:
                tagged_event = ProcessEvent(
                    case=event.case,
                    activity=event.activity,
                    timestamp=event.timestamp,
                    sequence=event.sequence,
                    model_version=current_model_version,
                )
                tagged_events.append(tagged_event)

        directly_follows_data = discovery_window.directly_follows_data
        if directly_follows_data is not None:
            model = ModelDiscovery().discover_heuristics_miner(directly_follows_data)
            model_update = DiscoveredModel(
                version=events[-1].sequence // DISCOVERY_WINDOW_SIZE + 1,
                model=self._serialize_model(
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
    def __init__(self, models_descriptor):
        self.models_descriptor = models_descriptor
        self.models = {}

    def open(self, runtime_context):
        alignment_state_descriptor = ValueStateDescriptor(
            "case-prefix-alignment", Types.PICKLED_BYTE_ARRAY()
        )
        model_version_descriptor = ValueStateDescriptor(
            "case-model-version", Types.INT()
        )
        pending_events_descriptor = ListStateDescriptor(
            "pending-conformance-events", Types.PICKLED_BYTE_ARRAY()
        )
        self.alignment_state = runtime_context.get_state(alignment_state_descriptor)
        self.model_version_state = runtime_context.get_state(model_version_descriptor)
        self.pending_events = runtime_context.get_list_state(
            pending_events_descriptor
        )

    def process_broadcast_element(self, model_update, context):
        context.get_broadcast_state(self.models_descriptor).put(
            model_update.version, model_update.model
        )
        self.models.pop(model_update.version, None)

    def _get_model(self, model_version, context):
        if model_version not in self.models:
            encoded_model = context.get_broadcast_state(self.models_descriptor).get(
                model_version
            )
            if encoded_model is None:
                return None
            self.models[model_version] = pickle.loads(
                base64.b64decode(encoded_model)
            )
        return self.models[model_version]

    def _score_event(self, event, model):
        alignment_state = self.alignment_state.value()
        if alignment_state is None:
            alignment_state = ConformanceChecker().create_prefix_alignment_state(
                event.case, model
            )

        score = alignment_state.add_event(event.activity)
        self.alignment_state.update(alignment_state)
        print(event.sequence)
        return json.dumps(
            {"case": event.case, "event": event.sequence, "score": score},
            separators=(",", ":"),
        )

    def process_element(self, event, context):
        model_version = self.model_version_state.value()
        if model_version is None:
            model_version = event.model_version
            if model_version is None:
                return
            self.model_version_state.update(model_version)

        self.pending_events.add(event)
        model = self._get_model(model_version, context)
        if model is None:
            return

        pending_events = list(self.pending_events.get())
        self.pending_events.clear()
        for event in pending_events:
            yield self._score_event(event, model)


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


def _get_configuration(checkpoint_dir):
    configuration = Configuration()
    configuration.set_string("state.backend.type", "hashmap")
    configuration.set_string("execution.checkpointing.dir", checkpoint_dir)
    return configuration

def _get_environment(configuration):
    environment = StreamExecutionEnvironment.get_execution_environment(configuration)
    environment.set_parallelism(1)
    environment.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    environment.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
    return environment

def _get_sink(output_path):
    output_file_config = (
        OutputFileConfig.builder()
        .with_part_prefix("scores")
        .with_part_suffix(".jsonl")
        .build()
    )
    return (
        FileSink.for_row_format(output_path, Encoder.simple_string_encoder("UTF-8"))
        .with_output_file_config(output_file_config)
        .build()
    )

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[2].timestamp())

def run_pipeline(input_path, output_path, checkpoint_dir, max_approx_error):
    events = FileInput().read_ordered_events(input_path)
    event_data = [
        (event.case, event.activity, event.timestamp, event.sequence)
        for event in events
    ]
    configuration = _get_configuration(checkpoint_dir)
    environment = _get_environment(configuration)

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(EventTimestampAssigner())
    )

    discovery_results = (
        environment.from_collection(event_data)
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda _event: _event[0])
        .map(CreateProcessEventOperator(), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda _event: "global", key_type=Types.STRING())
        .count_window(DISCOVERY_WINDOW_SIZE)
        .apply(CreateDirectlyFollowsWindowOperator(max_approx_error), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda _window: "global", key_type=Types.STRING())
        .process(DiscoverAndTagWindowOperator(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    model_updates = discovery_results.get_side_output(MODEL_UPDATE_OUTPUT_TAG)
    models_descriptor = MapStateDescriptor("discovered-models", Types.INT(), Types.STRING())

    (
        discovery_results
        .set_parallelism(4)
        .key_by(lambda event: event.case, key_type=Types.STRING())
        .connect(model_updates.broadcast(models_descriptor))
        .process(ConformanceOperator(models_descriptor), output_type=Types.STRING())
        .key_by(lambda _score: "global", key_type=Types.STRING())
        .process(CollectLatestScoresOperator(), output_type=Types.STRING())
        .sink_to(_get_sink(output_path))
    )
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
