# 🌿 Ecoscape

Ecoscape is a benchmark framework for evaluating **remediation strategies in Kubernetes**.
With Ecoscape, you can run reproducible experiments where a system is observed under load, chaos is injected in a controlled way, and the impact is measured via SLOs/SLIs.

## Core Concepts

- **Topology**: Describes zones (for example `cloud`, `berlin`, `munich`) including resource and network characteristics.
- **ChaosPhase**: Defines reusable faults (for example delay, loss, partition, CPU stress) that are injected during the measurement phase.
- **Experiment**: Orchestrates the lifecycle (deploy, load, pre-chaos measurement, chaos measurement, cleanup) and references Topology + ChaosPhase.

## Lifecycle

An `Experiment` runs through a deterministic lifecycle so benchmark runs are reproducible and comparable:

1. **Preparation**: Ecoscape resolves `Topology` and `ChaosPhase`, validates references, and prepares namespaces/resources.
2. **Deploy**: Manifests from `spec.manifests` (`infra`, `sut`, `monitor`, `load`) are applied.
3. **Topology Stabilization**: Topology constraints are activated, then Ecoscape waits for `spec.duration.topologyDelay`.
4. **Load Start**: Load generation begins and the system warms up for `spec.duration.loadDelay`.
5. **Pre-Chaos Measurement**: Baseline metrics are captured for `spec.duration.chaosDelay`.
6. **Chaos Injection + Measurement**: Faults from `ChaosPhase` are injected while SLO/SLI data is collected for `spec.duration.measurementDuration`.
7. **Evaluation**: Prometheus queries in `spec.slos[]` are evaluated and aggregated into experiment results.
8. **Cleanup / Repeat**: Temporary resources are cleaned up; the run either pauses (`pauseBetweenRepetitions`) and repeats, or finishes.

## Prerequisites

- Kubernetes cluster (for example Minikube or Kind)
- `kubectl`
- `helm`
- Go/Make (for local builds)
- Installed CRDs + running Ecoscape controller
- Prometheus + Chaos Mesh in the cluster

## Install from Helm

Install the Ecoscape operator directly from GHCR (OCI registry):

```bash
helm install ecoscape-operator oci://ghcr.io/henrywedge/charts/ecoscape-operator \
  --version 0.2.0 \
  --namespace ecoscape-system \
  --create-namespace
```

Uninstall:

```bash
helm uninstall ecoscape-operator --namespace ecoscape
```

## Quick Start

### 1) Install dependencies (Prometheus + Chaos Mesh)

```bash
bash sut/dependencies/install.sh
```

### 2) Install CRDs and start the controller

Option A (local development, recommended):

```bash
make install
make run
```

Option B (manifest deployment):

```bash
kubectl apply -f dist/install.yaml
```

> Note: For option B, the controller image must be reachable by your cluster.

### 3) Apply the example resources

```bash
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/topology.yaml
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/chaosphase-stress-cloud.yaml
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/cm-sut.yaml
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/cm-load.yaml
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/cm-monitor.yaml
kubectl apply -f sut/edge-sensor-pipeline/ecoscape/experiment.yaml
```

## Set Up a Topology

A topology is a dedicated CRD object (`kind: Topology`), for example in
`sut/edge-sensor-pipeline/ecoscape/topology.yaml`.

Important fields:

- `spec.zones`: zones and resource limits (`cpu`, `memory`, `disk`)
- `spec.links`: connections between zones (`latency`, `jitter`, optional `bandwidth`, `packetLoss`)

Minimal example:

```yaml
apiVersion: ecoscape.cau-se.de/v1alpha1
kind: Topology
metadata:
  name: two-zone-edge
  namespace: default
spec:
  zones:
    - name: cloud
      cpu: { limit: "4000m" }
      memory: { limit: "8Gi" }
    - name: munich
      cpu: { limit: "2000m" }
      memory: { limit: "2Gi" }
  links:
    - zones: [cloud, munich]
      latency: "50ms"
      jitter: "5ms"
```

## Set Up a ChaosPhase

A ChaosPhase is a reusable fault template (`kind: ChaosPhase`), for example
`sut/edge-sensor-pipeline/ecoscape/chaosphase-stress-cloud.yaml`.

Important fields:

- `spec.topologyRef.name`: name of the referenced Topology
- `spec.faults[]`: list of faults (for example `NetworkDelay`, `NetworkLoss`, `NetworkPartition`, `CPUStress`, `MemoryStress`, `Custom`)

Example (CPU stress in `cloud`):

```yaml
apiVersion: ecoscape.cau-se.de/v1alpha1
kind: ChaosPhase
metadata:
  name: esp-stress-cloud
  namespace: default
spec:
  topologyRef:
    name: two-zone-edge
  faults:
    - type: CPUStress
      zones: [cloud]
      workers: 2
      load: 80
```

## Set Up an Experiment

An experiment (`kind: Experiment`) ties together Topology, ChaosPhase, manifests, and SLOs, for example
`sut/edge-sensor-pipeline/ecoscape/experiment.yaml`.

Important fields:

- `spec.topologyRef.name`
- `spec.chaosPhaseRef.name`
- `spec.duration`: timing (`loadDelay`, `chaosDelay`, `topologyDelay`, `measurementDuration`, `repetitions`)
- `spec.manifests.*.configMapRef.name`: ConfigMap references (`sut`, `load`, `monitor`, optional `infra`)
- `spec.prometheus.url`
- `spec.slos[]`: PromQL + thresholds + weighting

Example:

```yaml
apiVersion: ecoscape.cau-se.de/v1alpha1
kind: Experiment
metadata:
  name: edge-sensor-pipeline-partition
  namespace: default
spec:
  topologyRef:
    name: two-zone-edge
  chaosPhaseRef:
    name: esp-stress-cloud
  duration:
    loadDelay: 30
    chaosDelay: 30
    topologyDelay: 15
    measurementDuration: 90
    repetitions: 3
    pauseBetweenRepetitions: 90
  manifests:
    sut:
      configMapRef:
        name: edge-sensor-pipeline-sut
    load:
      configMapRef:
        name: edge-sensor-pipeline-load
    monitor:
      configMapRef:
        name: edge-sensor-pipeline-monitor
  prometheus:
    url: "http://localhost:9090"
  slos:
    - name: stream-lag-munich
      query: 'redis_stream_group_lag{zone="munich"}'
      threshold: 50
      isBiggerBetter: false
      weight: 1.0
```

## Configurable Parameters

The benchmark is configured primarily through three custom resources:

- `Experiment`
- `Topology`
- `ChaosPhase`

### 1) Experiment (`kind: Experiment`)

| Field | Type | Required | Default | Description | Example |
|---|---|---:|---|---|---|
| `spec.topologyRef.name` | string | No | - | References the topology used for this run. | `two-zone-edge` |
| `spec.chaosPhaseRef.name` | string | No | - | References the ChaosPhase injected during chaos measurement. | `esp-stress-cloud` |
| `spec.prometheus.url` | string | Yes | - | Prometheus base URL used for SLO queries. | `http://localhost:9090` |
| `spec.prometheus.queryTimeoutSeconds` | int | No | `10` | Timeout per Prometheus query in seconds. | `10` |
| `spec.duration.loadDelay` | int | No | - | Delay before load generation starts (seconds). | `30` |
| `spec.duration.evalDelay` | int | No | - | Delay before full SLO evaluation starts (seconds). | `10` |
| `spec.duration.topologyDelay` | int | No | - | Stabilization delay after topology becomes active (seconds). | `15` |
| `spec.duration.chaosDelay` | int | No | - | Pre-chaos measurement window before chaos injection (seconds). | `30` |
| `spec.duration.measurementDuration` | int | No | - | Chaos measurement duration (seconds). | `90` |
| `spec.duration.repetitions` | int | No | `1` | Number of full experiment repetitions. | `8` |
| `spec.duration.pauseBetweenRepetitions` | int | No | `60` | Pause between repetitions (seconds). | `90` |
| `spec.duration.measurementSampleInterval` | int | No | `5` | Sampling interval for persisted measurements (seconds). | `5` |
| `spec.manifests.sut.configMapRef.name` | string | No | - | ConfigMap containing SUT manifests. | `edge-sensor-pipeline-sut` |
| `spec.manifests.load.configMapRef.name` | string | No | - | ConfigMap containing load manifests. | `edge-sensor-pipeline-load` |
| `spec.manifests.monitor.configMapRef.name` | string | No | - | ConfigMap containing monitoring manifests. | `edge-sensor-pipeline-monitor` |
| `spec.manifests.infra.configMapRef.name` | string | No | - | ConfigMap containing infrastructure manifests. | `edge-sensor-pipeline-infra` |

#### SLO entries (`spec.slos[]`)

| Field | Type | Required | Default | Description | Example |
|---|---|---:|---|---|---|
| `name` | string | Yes | - | Unique SLO name. | `stream-lag-munich` |
| `displayName` | string | No | `""` | Human-readable SLO label. | `Munich Stream Lag` |
| `description` | string | No | `""` | Additional explanation for the SLO. | `Consumer lag in Munich zone` |
| `query` | string | Yes | - | PromQL query returning one numeric value. | `redis_stream_group_lag{zone="munich"}` |
| `threshold` | float | Yes | - | SLO threshold value. | `50` |
| `thresholdDirection` | enum | No | `LessThanOrEqual` | Threshold comparator. One of: `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`. | `LessThanOrEqual` |
| `isBiggerBetter` | bool | Yes | `false` | Whether higher metric values are better. | `false` |
| `weight` | float (0..1) | Yes | `0` | Weight in aggregate score (weights should sum to `1.0`). | `0.5` |

### 2) Topology (`kind: Topology`)

| Field | Type | Required | Default | Description | Example |
|---|---|---:|---|---|---|
| `spec.active` | bool | No | `false` | Whether topology resources are currently provisioned. | `true` |
| `spec.zones[].name` | string | Yes | - | Zone identifier; used for namespace naming. | `munich` |
| `spec.zones[].cpu.limit` | string | No | - | CPU limit for zone quota. | `"2000m"` |
| `spec.zones[].memory.limit` | string | No | - | Memory limit for zone quota. | `"2Gi"` |
| `spec.zones[].disk.capacity` | string | No | - | Ephemeral storage quota for a zone. | `"8Gi"` |
| `spec.links[].zones` | string[2] | Yes | - | Exactly two zones forming a symmetric link. | `[cloud, munich]` |
| `spec.links[].latency` | string | No | - | Added link latency. | `"50ms"` |
| `spec.links[].jitter` | string | No | - | Latency variance (effective with latency). | `"5ms"` |
| `spec.links[].bandwidth` | string | No | - | Maximum link bandwidth. | `"10Mbps"` |
| `spec.links[].packetLoss` | string | No | - | Packet loss percentage as plain number string. | `"10"` |

### 3) ChaosPhase (`kind: ChaosPhase`)

| Field | Type | Required | Default | Description | Example |
|---|---|---:|---|---|---|
| `spec.topologyRef.name` | string | Yes | - | Topology used to resolve zone names/namespaces. | `two-zone-edge` |
| `spec.faults[].type` | enum | Yes | - | Fault type. One of: `NetworkDelay`, `NetworkBandwidth`, `NetworkLoss`, `NetworkPartition`, `CPUStress`, `MemoryStress`, `Custom`. | `CPUStress` |
| `spec.faults[].zones` | string[] | Depends | - | Target zones. Network faults use 2 zones; stress faults use one or more. | `[cloud]` |
| `spec.faults[].latency` | string | For `NetworkDelay` | - | Added delay. | `"500ms"` |
| `spec.faults[].jitter` | string | For `NetworkDelay` | - | Delay jitter. | `"100ms"` |
| `spec.faults[].bandwidth` | string | For `NetworkBandwidth` | - | Bandwidth cap. | `"5Mbps"` |
| `spec.faults[].packetLoss` | string | For `NetworkLoss` | - | Packet loss percentage. | `"50"` |
| `spec.faults[].workers` | int | For stress faults | - | Number of stress workers. | `2` |
| `spec.faults[].load` | int | For `CPUStress` | - | CPU load per worker (0-100). | `80` |
| `spec.faults[].size` | string | For `MemoryStress` | - | Memory allocation size per worker. | `"128MiB"` |
| `spec.faults[].manifest` | string (YAML) | For `Custom` | - | Raw Chaos Mesh manifest (must include `metadata.namespace`). | `apiVersion: ...` |

## Run an Experiment and View Results

With plain `kubectl`:

```bash
kubectl get experiments
kubectl describe experiment edge-sensor-pipeline-partition
```

With the plugin (`kubectl-ecoscape`):

```bash
make build-plugin
./bin/kubectl-ecoscape run -f sut/edge-sensor-pipeline/ecoscape/experiment.yaml --wait
./bin/kubectl-ecoscape list
./bin/kubectl-ecoscape results <experiment-id>
```

## Cleanup

```bash
kubectl delete experiment edge-sensor-pipeline-partition
kubectl delete chaosphase esp-stress-cloud
kubectl delete topology two-zone-edge

kubectl delete -f sut/edge-sensor-pipeline/ecoscape/cm-sut.yaml
kubectl delete -f sut/edge-sensor-pipeline/ecoscape/cm-load.yaml
kubectl delete -f sut/edge-sensor-pipeline/ecoscape/cm-monitor.yaml
```

Remove dependencies:

```bash
bash sut/dependencies/uninstall.sh
```
