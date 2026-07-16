package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZoneCPUSpec defines compute limits for a zone.
// Maps to ResourceQuota limits.cpu.
type ZoneCPUSpec struct {
	// Maximum CPU available in the zone (e.g. "500m", "2").
	Limit string `json:"limit"`
}

// ZoneMemorySpec defines memory limits for a zone.
// Maps to ResourceQuota limits.memory.
type ZoneMemorySpec struct {
	// Maximum memory available in the zone (e.g. "512Mi", "2Gi").
	Limit string `json:"limit"`
}

// ZoneDiskSpec defines ephemeral storage limits for a zone.
// Maps to ResourceQuota limits.ephemeral-storage.
// Note: I/O latency simulation (IOChaos) is not part of v1 because our
// primary SUT (Redis) is in-memory and would not be meaningfully affected.
type ZoneDiskSpec struct {
	// Maximum ephemeral storage available in the zone (e.g. "8Gi", "100Gi").
	Capacity string `json:"capacity"`
}

// ZoneSpec describes a single edge zone.
// The controller creates a dedicated namespace named "<topology-name>-<zone-name>"
// and applies ResourceQuota constraints derived from the resource fields below.
type ZoneSpec struct {
	// Unique name of the zone within this topology (e.g. "berlin", "cloud").
	// The resulting namespace will be "<topology-name>-<zone-name>".
	Name string `json:"name"`

	// CPU resource limits for this zone.
	// +optional
	CPU *ZoneCPUSpec `json:"cpu,omitempty"`

	// Memory resource limits for this zone.
	// +optional
	Memory *ZoneMemorySpec `json:"memory,omitempty"`

	// Disk / ephemeral storage limits for this zone.
	// +optional
	Disk *ZoneDiskSpec `json:"disk,omitempty"`
}

// ZoneLinkSpec describes a symmetric network link between exactly two zones.
// The controller translates each non-empty field into a dedicated NetworkChaos
// object (one per Chaos Mesh action type: delay, bandwidth, loss).
// Links are symmetric: the same conditions apply in both directions.
type ZoneLinkSpec struct {
	// Exactly two zone names that this link connects.
	// Both names must appear in spec.zones[].name.
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=2
	Zones []string `json:"zones"`

	// Additional round-trip latency on this link (e.g. "30ms").
	// Maps to NetworkChaos action: delay.
	// +optional
	Latency string `json:"latency,omitempty"`

	// Latency jitter / variance (e.g. "5ms"). Only effective when Latency is set.
	// +optional
	Jitter string `json:"jitter,omitempty"`

	// Maximum bandwidth on this link (e.g. "50Mbps", "1Mbps").
	// Maps to NetworkChaos action: bandwidth.
	// +optional
	Bandwidth string `json:"bandwidth,omitempty"`

	// Packet loss percentage as a plain number string (e.g. "5" for 5%).
	// Maps to NetworkChaos action: loss.
	// +optional
	PacketLoss string `json:"packetLoss,omitempty"`
}

// TopologySpec defines the desired state of a Topology.
type TopologySpec struct {
	// List of edge zones. Each zone gets its own namespace and ResourceQuota.
	// +kubebuilder:validation:MinItems=1
	Zones []ZoneSpec `json:"zones"`

	// Symmetric network links between zone pairs.
	// If a pair is not listed, no network chaos is applied between those zones.
	// +optional
	Links []ZoneLinkSpec `json:"links,omitempty"`
}

// TopologyPhase represents the lifecycle phase of a Topology.
// +kubebuilder:validation:Enum=Pending;Applied;Failed
type TopologyPhase string

const (
	TopologyPhasePending TopologyPhase = "Pending"
	TopologyPhaseApplied TopologyPhase = "Applied"
	TopologyPhaseFailed  TopologyPhase = "Failed"
)

// ZoneStatus captures the observed state of a single zone.
type ZoneStatus struct {
	// Zone name (matches ZoneSpec.Name).
	Name string `json:"name"`

	// Namespace created for this zone.
	Namespace string `json:"namespace"`

	// Name of the ResourceQuota applied in the zone namespace.
	// +optional
	ResourceQuotaRef string `json:"resourceQuotaRef,omitempty"`
}

// TopologyStatus defines the observed state of a Topology.
type TopologyStatus struct {
	// Current lifecycle phase.
	// +optional
	Phase TopologyPhase `json:"phase,omitempty"`

	// ObservedGeneration is the .metadata.generation that was last successfully reconciled.
	// Used to avoid redundant reconcile runs when only the status subresource changes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Per-zone observed state.
	// +optional
	Zones []ZoneStatus `json:"zones,omitempty"`

	// Names of all NetworkChaos objects created for link conditions.
	// Stored here so the controller can clean them up on deletion.
	// +optional
	NetworkChaosRefs []string `json:"networkChaosRefs,omitempty"`

	// Standard Kubernetes conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=topologies,scope=Namespaced,shortName=top
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Topology describes a multi-zone edge computing environment.
// The controller translates zone resource profiles into Kubernetes ResourceQuotas
// and zone link conditions into Chaos Mesh NetworkChaos objects, providing a
// single declarative surface for edge topology simulation.
type Topology struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TopologySpec   `json:"spec,omitempty"`
	Status TopologyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TopologyList contains a list of Topology.
type TopologyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Topology `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Topology{}, &TopologyList{})
}
