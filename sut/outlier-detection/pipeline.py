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
    KeyedProcessFunction,
    MapFunction,
    ProcessFunction,
    WindowFunction,
)
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import (
    MapStateDescriptor,
)

DISCOVERY_WINDOW_SIZE = 20
DEFAULT_MAX_APPROX_ERROR = 0.05
CONFORMANCE_EVENTS_OUTPUT_TAG = OutputTag(
    "conformance-events", Types.PICKLED_BYTE_ARRAY()
)
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


class SplitDiscoveryAndConformanceEvents(ProcessFunction):
    def process_element(self, event_data, context):
        case, activity, timestamp, sequence = event_data
        discovery_event = ProcessEvent(
            case=case,
            activity=activity,
            timestamp=timestamp,
            sequence=sequence,
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
        )
        yield MODEL_UPDATE_OUTPUT_TAG, model_update


class CreateConformanceRecordOperator(MapFunction):
    def __init__(self, record_type):
        self.record_type = record_type

    def map(self, record):
        return self.record_type, record


class ConformanceOperator(KeyedProcessFunction):
    def open(self, runtime_context):
        models_descriptor = MapStateDescriptor(
            "conformance-models", Types.INT(), Types.STRING()
        )
        cases_descriptor = MapStateDescriptor(
            "conformance-cases", Types.STRING(), Types.PICKLED_BYTE_ARRAY()
        )
        self.models = runtime_context.get_map_state(models_descriptor)
        self.cases = runtime_context.get_map_state(cases_descriptor)

    def _reprocess_pending_events(self, case_state, encoded_model):
        model = pickle.loads(base64.b64decode(encoded_model))
        alignment_state = case_state.alignment_state
        if alignment_state is None:
            first_event = case_state.pending_events[0]
            alignment_state = ConformanceChecker().create_prefix_alignment_state(
                first_event.case, model
            )

        scores = []
        for event in case_state.pending_events:
            score = alignment_state.add_event(event.activity)
            print(event.sequence)
            scores.append(
                json.dumps(
                    {"case": event.case, "event": event.sequence, "score": score},
                    separators=(",", ":"),
                )
            )

        case_state.alignment_state = alignment_state
        case_state.pending_events.clear()
        return scores

    def process_element(self, record, context):
        record_type, value = record
        if record_type == "model":
            self.models.put(value.version, value.model)
            for case in list(self.cases.keys()):
                case_state = self.cases.get(case)
                if case_state.model_version == value.version:
                    for score in self._reprocess_pending_events(case_state, value.model):
                        yield score
                    self.cases.put(case, case_state)
            return

        event = value
        case_state = self.cases.get(event.case)
        if case_state is None:
            if event.model_version is None:
                return
            case_state = CaseConformanceState(
                model_version=event.model_version,
                alignment_state=None,
                pending_events=[],
            )

        case_state.pending_events.append(event)
        encoded_model = self.models.get(case_state.model_version)
        if encoded_model is not None:
            for score in self._reprocess_pending_events(case_state, encoded_model):
                yield score
        self.cases.put(event.case, case_state)


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

    model_updates = discovery_results.get_side_output(MODEL_UPDATE_OUTPUT_TAG)

    conformance_records = (
        split_events.get_side_output(
            CONFORMANCE_EVENTS_OUTPUT_TAG
        )
        .map(
            CreateConformanceRecordOperator("event"),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .union(
            model_updates.map(
                CreateConformanceRecordOperator("model"),
                output_type=Types.PICKLED_BYTE_ARRAY(),
            )
        )
    )

    (
        conformance_records
        .key_by(lambda _record: "global", key_type=Types.STRING())
        .process(ConformanceOperator(), output_type=Types.STRING())
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
