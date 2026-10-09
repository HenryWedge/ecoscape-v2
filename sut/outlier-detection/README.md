# Conformance Checking Outlier Detection

A finite-file PyFlink pipeline uses PM4Py for process discovery and conformance
checking. Discovery and conformance checking run in separate branches of the
same Flink job. Model updates are applied in arrival order without synchronizing
the branches by event sequence.

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

- Count events globally in independent tumbling windows of 30 events.
- Apply lossy-counting filtering with `max_approx_error` (default `0.05`).
- Filter activity labels from each discovery window using their estimated
  frequencies before mining.
- Mine a process model from each completed window using the Heuristics Miner.
- Emit each model update with a version.

## Conformance checking

- Treat events processed before the first model update as a learning phase and
  drop them from conformance checking.
- Count accepted events globally in independent tumbling windows of 10 events.
- Retain each case's full activity trace and recalculate its prefix alignment
  cost whenever a conformance window completes.
- Pin each case to the model that is current when its first accepted event is
  processed. Later model updates do not change that trace's model.
- Emit one JSON array per completed conformance window with an entry for every
  case seen so far, for example:

  ```json
  [{"case":"case-1","score":0},{"case":"case-2","score":3}]
  ```

- Emit no score output before the first model update. Alignment uses unit cost
  for log and model moves and zero cost for synchronous moves.
- Cases have no completion event. Traces and model versions are retained in
  managed state for the duration of the job.
- Incomplete final discovery and conformance windows are not processed.
- Discovery and conformance are not synchronized by event sequence. The model
  current when an event is processed by conformance depends on the arrival order
  of events and model updates.

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
`input.csv` contains enough events for complete discovery and conformance
windows.
The pipeline targets `apache-flink==2.3.0` and configures the HashMap state
backend and checkpoint directory through Flink's `Configuration` API.
