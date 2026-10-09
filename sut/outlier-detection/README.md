# Conformance Checking Outlier Detection

A finite-file PyFlink pipeline uses PM4Py for process discovery and conformance
checking. Discovery and conformance checking run in one ordered pipeline, so
each discovery window is processed before the following window.

## Input

CSV input has one event per row and the columns `case`, `activity`, and
`timestamp`. Timestamps should be ISO-8601. Events are globally sorted by
timestamp before processing; rows with equal timestamps retain their CSV order.

```csv
case,activity,timestamp
case-1,Register,2026-10-08T09:00:00Z
case-1,Review,2026-10-08T09:05:00Z
case-2,Register,2026-10-08T09:06:00Z
```

## Process discovery

- Count events globally in independent tumbling windows of 30 events using
  Flink's count-window operator.
- Use the first complete discovery window as the learning phase. Its events are
  not scored; its model is applied starting with the next discovery window.
- Apply lossy-counting filtering with `max_approx_error` (default `0.05`).
- Filter activity labels from each discovery window using their estimated
  frequencies before mining.
- Mine a process model from each completed window using the Heuristics Miner.
- Emit each model update with a version.

## Conformance checking

- Score only events in complete discovery windows after the learning phase.
- Count accepted events globally in independent tumbling windows of 10 events
  using Flink's count-window operator.
- Retain each case's full activity trace and recalculate its cumulative prefix
  alignment cost whenever a conformance window completes.
- Pin each case to the model attached to its first accepted event. Later model
  updates do not change that trace's model.
- Emit one JSON object per completed conformance window, with scores only for
  cases that had accepted events in that window. For example:

  ```json
  {"window":{"first_event":30,"last_event":39},"scores":[{"case":"case-1","score":0},{"case":"case-2","score":3}]}
  ```

- Alignment uses unit cost for log and model moves and zero cost for synchronous
  moves.
- Cases have no completion event. Traces and model versions are retained in
  managed state for the duration of the job.
- Incomplete final discovery and conformance windows are not processed by their
  respective window functions.
- The model learned from a discovery window is used starting with the next
  discovery window. Model and event ordering is therefore deterministic.

## State and delivery

- Use Flink managed state with the HashMap state backend.
- Enable exactly-once checkpointing. Configure the checkpoint directory to a
  location accessible and durable for the Flink runtime.
- Write output using Flink's filesystem sink as JSONL part files under the
  configured output directory.

## Run

Install the dependencies in `requirements.txt`, then run:

```bash
python pipeline.py --input input.csv --output output \
  --checkpoint-dir file:///tmp/outlier-detection-checkpoints
```

Optional arguments include `--max-approx-error` (default `0.05`). The sample
`input.csv` has 58 events, so it does not contain a complete second discovery
window and produces no scores. At least 60 events are needed for one complete
scored discovery window after the initial learning window.
The pipeline targets `apache-flink==2.3.0` and configures the HashMap state
backend and checkpoint directory through Flink's `Configuration` API.
