"""Finite-file PyFlink pipeline for process discovery and conformance scoring."""

import argparse
import base64
import hashlib
import json
import math
import pickle
from pathlib import Path
from processmining.model_discovery import ModelDiscovery
from processmining.conformance_checker import ConformanceChecker
from processmining.input import FileInput
from processmining.data_models import (
    CaseConformanceState,
    DiscoveredModel,
    DiscoveryWindow,
    ProcessEvent,
)

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
    ProcessFunction,
    WindowFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import (
    MapStateDescriptor,
)

DISCOVERY_WINDOW_SIZE = 20
CONFORMANCE_SHARD_COUNT = 4
DEFAULT_MAX_APPROX_ERROR = 0.05
CONFORMANCE_EVENTS_OUTPUT_TAG = OutputTag(
    "conformance-events", Types.PICKLED_BYTE_ARRAY()
)
MODEL_UPDATE_OUTPUT_TAG = OutputTag(
    "model-updates", Types.PICKLED_BYTE_ARRAY()
)


def _get_case_shard(case):
    case_digest = hashlib.sha256(case.encode("utf-8")).digest()
    return int.from_bytes(case_digest[:4], byteorder="big") % CONFORMANCE_SHARD_COUNT


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


class SplitDiscoveryAndConformanceEvents(ProcessFunction):
    def process_element(self, event_data, context):
        case, activity, timestamp, sequence = event_data
        discovery_event = ProcessEvent(
            case=case,
            activity=activity,
            timestamp=timestamp,
            sequence=sequence
        )
        yield discovery_event

        model_version = sequence // DISCOVERY_WINDOW_SIZE
        if model_version > 0:
            yield (
                CONFORMANCE_EVENTS_OUTPUT_TAG,
                ProcessEvent(
                    case=case,
                    activity=activity,
                    timestamp=timestamp,
                    sequence=sequence,
                    model_version=model_version,
                ),
            )


class DiscoverModelFromWindowOperator(KeyedProcessFunction):
    def _serialize_model(self, model):
        return base64.b64encode(pickle.dumps(model)).decode("ascii")

    def process_element(self, discovery_window, context):
        events = discovery_window.events
        directly_follows_data = discovery_window.directly_follows_data
        if directly_follows_data is None:
            return

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
            timestamp=int(events[-1].timestamp.timestamp() * 1000),
        )
        yield MODEL_UPDATE_OUTPUT_TAG, model_update


class ConformanceOperator(KeyedBroadcastProcessFunction):
    def __init__(self, models_descriptor):
        self.models_descriptor = models_descriptor

    def open(self, runtime_context):
        case_states_descriptor = MapStateDescriptor(
            "shard-case-conformance-states",
            Types.STRING(),
            Types.PICKLED_BYTE_ARRAY(),
        )
        self.case_states = runtime_context.get_map_state(case_states_descriptor)

    def _reprocess_pending_events(self, case_state, encoded_model):
        model = pickle.loads(base64.b64decode(encoded_model))

        if case_state.alignment_state is None:
            first_event = case_state.pending_events[0]
            case_state.alignment_state = ConformanceChecker().create_prefix_alignment_state(
                first_event.case, model
            )

        scores = []
        for event in case_state.pending_events:
            score = case_state.alignment_state.add_event(event.activity)
            print(event.sequence)
            scores.append(
                json.dumps(
                    {"case": event.case, "event": event.sequence, "score": score},
                    separators=(",", ":"),
                )
            )

        case_state.pending_events.clear()
        return scores

    def process_broadcast_element(self, model_update, context):
        context.get_broadcast_state(self.models_descriptor).put(
            model_update.version, model_update.model
        )

    def process_element(self, event, context):
        case_state = self.case_states.get(event.case)
        if case_state is None:
            if event.model_version is None:
                return
            case_state = CaseConformanceState(
                model_version=event.model_version,
                alignment_state=None,
                pending_events=[],
            )

        case_state.pending_events.append(event)
        self.case_states.put(event.case, case_state)

        event_timestamp = context.timestamp()
        if event_timestamp is not None:
            context.timer_service().register_event_time_timer(event_timestamp + 1)

        broadcast_models = context.get_broadcast_state(self.models_descriptor)
        for case, pending_case_state in list(self.case_states.items()):
            if not pending_case_state.pending_events:
                continue

            encoded_model = broadcast_models.get(pending_case_state.model_version)
            if encoded_model is None:
                continue

            for score in self._reprocess_pending_events(
                pending_case_state, encoded_model
            ):
                yield score
            self.case_states.put(case, pending_case_state)

    def on_timer(self, timestamp, context):
        broadcast_models = context.get_broadcast_state(self.models_descriptor)
        for case, pending_case_state in list(self.case_states.items()):
            if not pending_case_state.pending_events:
                continue

            encoded_model = broadcast_models.get(pending_case_state.model_version)
            if encoded_model is None:
                continue

            for score in self._reprocess_pending_events(
                pending_case_state, encoded_model
            ):
                yield score
            self.case_states.put(case, pending_case_state)


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
        return int(value[2].timestamp() * 1000)


class ModelUpdateTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, model_update, record_timestamp) -> int:
        return model_update.timestamp

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

    split_events = (
        environment.from_collection(event_data)
        .assign_timestamps_and_watermarks(watermark_strategy)
        .process(SplitDiscoveryAndConformanceEvents(),output_type=Types.PICKLED_BYTE_ARRAY())
    )

    discovery_results = (
        split_events
        .key_by(lambda _event: "global", key_type=Types.STRING())
        .count_window(DISCOVERY_WINDOW_SIZE)
        .apply(CreateDirectlyFollowsWindowOperator(max_approx_error), output_type=Types.PICKLED_BYTE_ARRAY())
        .key_by(lambda _window: "global", key_type=Types.STRING())
        .process(DiscoverModelFromWindowOperator(), output_type=Types.PICKLED_BYTE_ARRAY())
    )

    model_updates = (
        discovery_results.get_side_output(MODEL_UPDATE_OUTPUT_TAG)
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(ModelUpdateTimestampAssigner())
        )
    )
    models_descriptor = MapStateDescriptor("discovered-models", Types.INT(), Types.STRING())

    (
        split_events.get_side_output(CONFORMANCE_EVENTS_OUTPUT_TAG)
        .key_by(
            lambda event: _get_case_shard(event.case),
            key_type=Types.INT(),
        )
        .connect(model_updates.broadcast(models_descriptor))
        .process(
            ConformanceOperator(models_descriptor),
            output_type=Types.STRING(),
        )
        .set_parallelism(CONFORMANCE_SHARD_COUNT)
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
