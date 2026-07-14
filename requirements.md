# Ecoscape — Requirements

Based on the Python reference implementation in `ecoscape-python/experiment_runner/`.

---

## 1. Core Concepts

### 1.1 Experiment
An experiment is a single run of the benchmark lifecycle: deploy SUT → start load → evaluate → inject chaos → evaluate under chaos → cleanup. The total score is an aggregate of per-SLO violation scores.

### 1.2 SLO (Service Level Objective)
A named objective composed of:
- A **PromQL query** that returns a numeric SLI (Service Level Indicator)
- A **threshold** value
- A **direction** (`isBiggerBetter`: true = higher values are better, false = lower values are better)
- A **weight** for aggregation

### 1.3 SLI (Service Level Indicator)
The raw time-series metric value returned by Prometheus at each evaluation tick (1-second intervals).

### 1.4 Violation Score
A normalized score `[0, 1]` measuring how severely an SLO was violated:
- For `isBiggerBetter`: `violation = 1 - (value / threshold)` when `value < threshold`
- For `!isBiggerBetter`: `violation = 1 - (threshold / value)` when `value > threshold`
- The final SLO score is the **mean** over all ticks.

### 1.5 Ecoscape Score
A weighted sum of individual SLO violation scores. Weights must sum to 1.0.

---

## 2. Functional Requirements

### FR-1: Experiment Lifecycle
The system MUST execute experiments in the following order:
1. **Deploy SUT** — Apply Kubernetes manifests for the system under test
2. **Apply infra constraints** — Apply infrastructure configuration (e.g. resource limits, network policies)
3. **Start load** — Deploy load generators
4. **Pre-evaluation phase** — Evaluate SLOs with only monitor sinks active for `evalDelay` seconds
5. **Pre-chaos evaluation** — Evaluate with all sinks active for `chaosDelay` seconds
6. **Inject chaos** — Apply chaos manifests (e.g. Chaos Mesh experiments)
7. **Chaos evaluation** — Evaluate with all sinks for `duration` seconds
8. **Remove SUT & chaos** — Delete manifests
9. **Stop load** — Remove load generators
10. **Remove infra constraints** — Delete infrastructure manifests

### FR-2: Configurable Timing
All experiment timing parameters MUST be configurable:
- `duration` — length of the chaos evaluation phase (seconds)
- `loadDelay` — delay before starting load generation (seconds)
- `evalDelay` — delay before full evaluation starts (seconds)
- `chaosDelay` — delay before chaos injection (seconds)
- `repetitions` — number of times to repeat the entire experiment

### FR-3: Mode Support
The system MUST support these execution modes:
- **FullExperimentRun** — deploys system, starts load, applies infra, injects chaos
- **ExperimentRun** — starts load, applies infra, injects chaos (SUT already deployed)
- **LoadTest** — only starts load (no deploy, no infra, no chaos)
- **SystemDeployer** — only deploys the system (no load, no infra, no chaos)

### FR-4: SLO Evaluation Pipeline
Each SLO evaluation tick (every 1 second) MUST:
1. Execute the configured PromQL query
2. Pass the resulting value to every configured Sink
3. Each sink processes the value independently

### FR-5: Sink Types
The system MUST support the following sink types:
- **Violation Score Sink** — Accumulates normalized violation scores, reports the mean at the end
- **Value Printer Sink** — Prints each SLI value to stdout in real time
- **SLI Storage Sink** — Collects all raw SLI values, exports to CSV after experiment ends
- **Visualizer Sink** — Plots SLI values in real time using matplotlib (with threshold line)

Each sink has a `isMonitorSink` flag: monitor sinks receive data during the pre-evaluation phase; all sinks receive data during the evaluation phases.

### FR-6: Score Aggregation
At the end of all repetitions, the system MUST:
1. Collect the final score from each Violation Score Sink
2. Compute a weighted aggregate score
3. Print the aggregate score

### FR-7: Experiment Identification
Each experiment run MUST generate a random 5-character lowercase ID used for:
- Associating stored CSV result files (`{experiment_id}-{slo_name}.csv`)
- Logging

### FR-8: Graceful Shutdown
On keyboard interrupt (Ctrl+C), the system MUST:
- Delete the SUT
- Delete chaos resources
- Execute end hooks on all sinks
- Stop load
- Delete infra constraints

### FR-9: Repetition Support
When `repetitions > 1`, the system MUST:
- Wait 60 seconds between repetitions
- Execute the full experiment lifecycle for each repetition

---

## 3. Kubernetes Requirements

### KR-1: Dynamic Client
The system MUST use the Kubernetes dynamic client (`kubernetes.dynamic.DynamicClient`) to apply and delete arbitrary Kubernetes resources without type-specific code.

### KR-2: Resource Lifecycle
For any directory of YAML manifests, the system MUST support:
- **Create/Upsert**: Apply each manifest — create if not found, patch (merge) if it exists
- **Delete**: Delete each manifest by name

### KR-3: In-Cluster Operation
The system MUST authenticate via `config.load_incluster_config()` (service account token mounted in the Pod).

### KR-4: RBAC Requirements
The operator's ServiceAccount requires the following ClusterRole permissions:
- **API Groups**: `""` (core), `apps`, `kafka.strimzi.io`, `monitoring.coreos.com`, `chaos-mesh.org`
- **Verbs**: `get`, `list`, `watch`, `create`, `update`, `patch`, `delete`
- **Resources**: `*` (all resources within those API groups)

### KR-5: ConfigMap-based Configuration
Experiment configuration and K8s manifests MUST be supplied as ConfigMaps mounted into the Pod:
| Mount Path | ConfigMap | Contents |
|---|---|---|
| `/app/experiment_runner/experiment_config` | `experiment-config` | YAML config files (slos.yaml, experiment_duration.yaml, experiment_directory.yaml) |
| `/app/build/sut` | `sut-config` | SUT Kubernetes manifests |
| `/app/build/load` | `load-config` | Load generator manifests |
| `/app/build/monitor` | `monitor-config` | Monitoring manifests (ServiceMonitor, PodMonitor) |
| `/app/build/chaos` | `chaos-config` | Chaos experiment manifests |
| `/app/build/infra` | `infra-config` | Infrastructure constraint manifests |

### KR-6: Directory-based Manifest Organization
The directories MUST follow this naming convention (configurable via `experiment_directory.yaml`):
- `infra` — Infrastructure constraint manifests
- `sut` — System under test manifests
- `load` — Load generator manifests
- `chaos` — Chaos injection manifests
- `monitor` — Monitoring manifests (with optional `monitor/podmonitor` subdirectory)

---

## 4. Monitoring & Metrics Requirements

### MR-1: Prometheus Connection
The system MUST connect to a Prometheus server via its HTTP API using the `prometheus_api_client` library. The Prometheus URL is configured in `slos.yaml`.

### MR-2: Custom PromQL Queries
Each SLO MUST support an arbitrary PromQL query string. The query result MUST be a single numeric value (instant vector with one sample).

### MR-3: Metric Categories (from reference implementation)
The reference implementation demonstrates these metric types:
- **Energy consumption** — Kepler container DRAM + package joules (rate)
- **Processing latency** — Average latency from the SUT
- **Accuracy** — Average processing accuracy from the SUT
- **Kafka consumer lag** — Kafka broker incoming messages per topic

### MR-4: Empty Result Handling
If a Prometheus query returns no results, the SLO value MUST be `-1` (sentinel for "no data").

---

## 5. Configuration Requirements

### CR-1: YAML Configuration Sources
All configuration MUST be provided as YAML files:

**`experiment_duration.yaml`**
```yaml
infraDelay: <int>
loadDelay: <int>
evalDelay: <int>
chaosDelay: <int>
repetitions: <int>
duration: <int>
```

**`experiment_directory.yaml`**
```yaml
infra: <string>
sut: <string>
load: <string>
chaos: <string>
monitor: <string>
```

**`slos.yaml`**
```yaml
prometheus:
  url: <string>
slos:
  - name: <string>
    query: <string (PromQL)>
    threshold: <float>
    isBiggerBetter: <bool>
    weight: <float>
```

### CR-2: CLI Overrides (Optional)
The system SHOULD support overriding timing and directory parameters via command-line flags:
- `--duration`, `--load_delay`, `--eval_delay`, `--repetitions`
- `--dir_load`, `--dir_sut`, `--dir_sut_patch`, `--dir_infra`, `--dir_infra_patch`, `--dir_monitor`

---

## 6. Output Requirements

### OR-1: Console Output
The system MUST print:
- Start/end markers for each phase
- Current SLI values (via Value Printer Sink) in real time
- Individual SLO scores at the end
- The aggregated Ecoscape score at the end

### OR-2: CSV Result Files
The system MUST write raw SLI values to CSV files at `result/{experiment_id}-{slo_name}.csv` (one value per line).

### OR-3: Visualization (Optional)
The system MAY provide real-time matplotlib plotting with:
- SLI values plotted as a line
- Threshold shown as a horizontal red line
- Plot saved to `result/{slo_name}-plot.png` on experiment end

---

## 7. Non-Functional Requirements

### NFR-1: Security
- The K8s client MUST disable SSL verification (`verify_ssl = False`) when running in-cluster with self-signed certs (or make this configurable)
- RBAC MUST follow least-privilege for the specific API groups used

### NFR-2: Environment
- Runtime: Python 3.12 (Debian Bookworm base)
- The system MUST run as a Kubernetes Deployment (single Pod, single shot)

### NFR-3: Dependencies
Required Python packages:
- `kubernetes ~= 33.1.0`
- `prometheus-api-client ~= 0.5.7`
- `PyYAML` (for config parsing)
- `numpy` (for CSV export)
- `matplotlib` (optional, for real-time visualization)
- `requests` (for direct Prometheus HTTP probes)

### NFR-4: Error Handling
- Missing directories SHOULD be handled gracefully (log warning, skip)
- Prometheus query failures MUST NOT crash the experiment (return sentinel value `-1`)
- Keyboard interrupt MUST trigger a clean shutdown

---

## 8. Migration to Go / Kubernetes Operator

When porting to Go as a Kubernetes Operator, the following mappings apply:

| Python Component | Go Operator Equivalent |
|---|---|
| `K8sClient` (dynamic) | `controller-runtime` client (with unstructured/dynamic) |
| `EcoscapeClient` | Reconciler or Controller logic |
| `Scenario.run()` | Reconcile loop or Job controller |
| `SloSink` | Interface/strategy for result processing |
| `PrometheusConnect` | `prometheus-client` or direct HTTP client |
| YAML config files | ConfigMap watcher or typed Go config structs |
| Directories of YAML | `client.Apply()` with `server-side apply` |
| `load_incluster_config()` | `ctrl.GetConfigOrDie()` / `manager.Options` |
| `Mode` | Feature gates or controller mode flags |
| Random experiment ID | `metav1.ObjectMeta.GenerateName` or UUID |

### Operator-Specific Considerations
- **CRD**: The experiment configuration should become a custom resource (e.g., `ExperimentRunner` or `BenchmarkSuite`)
- **Reconciliation**: The operator should reconcile on the CR, executing the experiment lifecycle
- **Status**: Experiment progress, current phase, and final score should be reported in the CR status
- **Idempotency**: Each phase should be safely re-entrant (handle partial deployment, timeouts, failures)
- **Leader election**: For multi-replica deployments
- **Metrics**: The operator itself should expose Prometheus metrics for experiment duration, SLO scores, etc.
