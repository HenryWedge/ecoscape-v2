package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=LessThan;LessThanOrEqual;GreaterThan;GreaterThanOrEqual
type ThresholdDirection string

const (
	ThresholdLT  ThresholdDirection = "LessThan"
	ThresholdLTE ThresholdDirection = "LessThanOrEqual"
	ThresholdGT  ThresholdDirection = "GreaterThan"
	ThresholdGTE ThresholdDirection = "GreaterThanOrEqual"
)

// +kubebuilder:validation:Enum=Deploying;WaitingForTopology;TopologyDelay;LoadDelay;PreChaosMeasurement;ApplyingChaos;ChaosMeasurement;Cleanup;Pause
type ExperimentSubPhase string

const (
	SubPhaseDeploying           ExperimentSubPhase = "Deploying"
	SubPhaseWaitingForTopology  ExperimentSubPhase = "WaitingForTopology"
	SubPhaseTopologyDelay       ExperimentSubPhase = "TopologyDelay"
	SubPhaseLoadDelay           ExperimentSubPhase = "LoadDelay"
	SubPhasePreChaosMeasurement ExperimentSubPhase = "PreChaosMeasurement"
	SubPhaseApplyingChaos       ExperimentSubPhase = "ApplyingChaos"
	SubPhaseChaosMeasurement    ExperimentSubPhase = "ChaosMeasurement"
	SubPhaseCleanup             ExperimentSubPhase = "Cleanup"
	SubPhasePause               ExperimentSubPhase = "Pause"
)

type DurationConfig struct {
	// Delay before starting load generation (seconds).
	// +optional
	LoadDelay *int32 `json:"loadDelay,omitempty"`
	// Delay before full SLO evaluation starts (seconds).
	// +optional
	EvalDelay *int32 `json:"evalDelay,omitempty"`
	// Stabilization delay after the Topology becomes Active (seconds).
	// Use this to give SUT and load pods time to fully start up under the new
	// network conditions before the pre-chaos evaluation window begins.
	// +optional
	TopologyDelay *int32 `json:"topologyDelay,omitempty"`
	// Delay before chaos injection (seconds).
	// +optional
	ChaosDelay *int32 `json:"chaosDelay,omitempty"`
	// Duration of the chaos evaluation phase (seconds).
	// +optional
	MeasurementDuration *int32 `json:"measurementDuration,omitempty"`
	// Number of experiment repetitions.
	// +optional
	// +kubebuilder:default=1
	Repetitions int32 `json:"repetitions,omitempty"`
	// Pause between repetitions in seconds.
	// +optional
	// +kubebuilder:default=60
	PauseBetweenRepetitions int32 `json:"pauseBetweenRepetitions,omitempty"`
	// How often (in seconds) an individual SLI value is written to the
	// measurements ConfigMap. Aggregates (mean, min, max, violations) are
	// always computed from every measurement regardless of this setting.
	// Defaults to 5 (one sample every 5 seconds).
	// +optional
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	MeasurementSampleInterval *int32 `json:"measurementSampleInterval,omitempty"`
}

type ManifestRef struct {
	// Reference to a ConfigMap containing Kubernetes manifests.
	ConfigMapRef corev1.LocalObjectReference `json:"configMapRef"`
}

type ManifestsConfig struct {
	// System under test manifests.
	// +optional
	Sut *ManifestRef `json:"sut,omitempty"`
	// Load generator manifests.
	// +optional
	Load *ManifestRef `json:"load,omitempty"`
	// Infrastructure constraint manifests.
	// +optional
	Infra *ManifestRef `json:"infra,omitempty"`
	// Monitoring manifests (ServiceMonitor, PodMonitor, etc.).
	// +optional
	Monitor *ManifestRef `json:"monitor,omitempty"`
}

type PrometheusConfig struct {
	// Prometheus server URL.
	URL string `json:"url"`
	// Query timeout in seconds.
	// +optional
	// +kubebuilder:default=10
	QueryTimeoutSeconds *int32 `json:"queryTimeoutSeconds,omitempty"`
}

type SLOConfig struct {
	// Unique name for this SLO.
	Name string `json:"name"`
	// Human-readable display name.
	// +optional
	DisplayName string `json:"displayName,omitempty"`
	// Description of what this SLO measures.
	// +optional
	Description string `json:"description,omitempty"`
	// PromQL query returning a single numeric SLI value.
	Query string `json:"query"`
	// Threshold value for the SLO.
	// +kubebuilder:validation:Type=number
	Threshold float64 `json:"threshold"`
	// Comparison direction for the threshold.
	// +optional
	// +kubebuilder:default=LessThanOrEqual
	ThresholdDirection ThresholdDirection `json:"thresholdDirection,omitempty"`
	// Whether higher values are better for this metric.
	// +kubebuilder:default=false
	IsBiggerBetter bool `json:"isBiggerBetter"`
	// Weight for the aggregate score (0.0 = observing only). Weights should sum to 1.0.
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=0
	Weight float64 `json:"weight"`
}

// ExperimentSpec defines the desired state of Experiment.
type ExperimentSpec struct {
	// Timing configuration for experiment phases.
	// +optional
	Duration DurationConfig `json:"duration,omitempty"`
	// References to ConfigMaps containing Kubernetes manifests.
	// +optional
	Manifests ManifestsConfig `json:"manifests,omitempty"`
	// Reference to a Topology object (same namespace) that defines the edge zones
	// for this experiment. If set, the experiment waits for the Topology to reach
	// phase Active before deploying the SUT.
	// +optional
	TopologyRef *corev1.LocalObjectReference `json:"topologyRef,omitempty"`
	// Reference to a ChaosPhase object (same namespace) defining the faults to
	// inject during the chaos measurement window.
	// +optional
	ChaosPhaseRef *corev1.LocalObjectReference `json:"chaosPhaseRef,omitempty"`
	// Prometheus connection configuration.
	Prometheus PrometheusConfig `json:"prometheus"`
	// Service Level Objective definitions.
	// +kubebuilder:validation:MinItems=1
	SLOs []SLOConfig `json:"slos"`
}

type SLOResult struct {
	// SLO name (matches SLOConfig.Name).
	Name string `json:"name"`
	// Normalized violation score [0, 1].
	ViolationScore float64 `json:"violationScore"`
	// Mean SLI value over all evaluations.
	MeanSli float64 `json:"meanSli"`
	// Minimum SLI value observed.
	// +optional
	MinSli *float64 `json:"minSli,omitempty"`
	// Maximum SLI value observed.
	// +optional
	MaxSli *float64 `json:"maxSli,omitempty"`
	// Total number of SLO evaluations.
	Evaluations int32 `json:"evaluations"`
	// Number of threshold violations.
	// +optional
	Violations int32 `json:"violations,omitempty"`
}

type RepetitionResult struct {
	// Repetition index (1-based).
	Repetition int32 `json:"repetition"`
	// Per-SLO results for this repetition.
	SloResults []SLOResult `json:"sloResults"`
	// Weighted aggregate score for this repetition.
	AggregateScore float64 `json:"aggregateScore"`
}

// SLOMeasurementState holds running aggregate data for one SLO during the
// current measurement window. It is stored in the CR status so that no
// in-memory state is needed between reconcile calls.
type SLOMeasurementState struct {
	// SLO name (matches SLOConfig.Name).
	Name string `json:"name"`
	// Running sum of all observed SLI values.
	Sum float64 `json:"sum"`
	// Number of observations recorded so far.
	Count int32 `json:"count"`
	// Number of threshold violations recorded so far.
	Violations int32 `json:"violations"`
	// Minimum observed SLI value.
	Min float64 `json:"min"`
	// Maximum observed SLI value.
	Max float64 `json:"max"`
	// MinSet is true once at least one value has been recorded (so Min=0 is not
	// mistaken for "no data").
	MinSet bool `json:"minSet"`
	// Individual measurement values recorded during this window.
	// Cleared after each repetition once written to the measurements ConfigMap.
	// +optional
	Values []float64 `json:"values,omitempty"`
}

// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed;Cancelled
type ExperimentPhase string

const (
	PhasePending   ExperimentPhase = "Pending"
	PhaseRunning   ExperimentPhase = "Running"
	PhaseSucceeded ExperimentPhase = "Succeeded"
	PhaseFailed    ExperimentPhase = "Failed"
	PhaseCancelled ExperimentPhase = "Cancelled"
)

// ExperimentStatus defines the observed state of Experiment.
type ExperimentStatus struct {
	// Current phase of the experiment.
	// +optional
	Phase ExperimentPhase `json:"phase,omitempty"`
	// Current sub-phase within a running repetition.
	// +optional
	SubPhase ExperimentSubPhase `json:"subPhase,omitempty"`
	// Timestamp when the current sub-phase started.
	// +optional
	SubPhaseStartTime *metav1.Time `json:"subPhaseStartTime,omitempty"`
	// Total duration the current sub-phase should run (seconds).
	// +optional
	SubPhaseDuration int32 `json:"subPhaseDuration,omitempty"`
	// Elapsed time in the current sub-phase (seconds, updated each reconcile).
	// +optional
	SubPhaseElapsed int32 `json:"subPhaseElapsed,omitempty"`
	// Running measurement aggregates for the current repetition.
	// +optional
	MeasurementState []SLOMeasurementState `json:"measurementState,omitempty"`
	// Random 5-character experiment identifier.
	// +optional
	ExperimentID string `json:"experimentId,omitempty"`
	// Name of the Topology used in this experiment run (set if topologyRef was provided).
	// +optional
	TopologyRef string `json:"topologyRef,omitempty"`
	// Timestamp when the experiment started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`
	// Timestamp when the experiment completed.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`
	// Number of successfully completed repetitions.
	// +optional
	RepetitionsCompleted int32 `json:"repetitionsCompleted,omitempty"`
	// Names of ConfigMaps created by the operator for manifest content.
	// +optional
	ManifestConfigMaps []string `json:"manifestConfigMaps,omitempty"`
	// Per-SLO results for the latest completed repetition.
	// +optional
	SloResults []SLOResult `json:"sloResults,omitempty"`
	// Weighted aggregate score of the latest completed repetition.
	// +optional
	AggregateScore *float64 `json:"aggregateScore,omitempty"`
	// Standard conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=experiments,scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="SubPhase",type="string",JSONPath=".status.subPhase"
// +kubebuilder:printcolumn:name="Repetitions",type="integer",JSONPath=".status.repetitionsCompleted"
// +kubebuilder:printcolumn:name="Score",type="number",JSONPath=".status.aggregateScore"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Experiment is the Schema for the experiments API.
type Experiment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ExperimentSpec   `json:"spec,omitempty"`
	Status ExperimentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ExperimentList contains a list of Experiment.
type ExperimentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Experiment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Experiment{}, &ExperimentList{})
}
