package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:validation:Enum=NetworkDelay;NetworkBandwidth;NetworkLoss;NetworkPartition;CPUStress;MemoryStress;Custom
type FaultType string

const (
	FaultNetworkDelay     FaultType = "NetworkDelay"
	FaultNetworkBandwidth FaultType = "NetworkBandwidth"
	FaultNetworkLoss      FaultType = "NetworkLoss"
	FaultNetworkPartition FaultType = "NetworkPartition"
	FaultCPUStress        FaultType = "CPUStress"
	FaultMemoryStress     FaultType = "MemoryStress"
	FaultCustom           FaultType = "Custom"
)

// FaultSpec describes a single fault to inject during the chaos phase.
// Which fields are relevant depends on the Type field.
type FaultSpec struct {
	// Type of fault to inject.
	Type FaultType `json:"type"`

	// Zone names from the referenced Topology.
	// For network faults: exactly 2 zones defining the link endpoints.
	// For stress faults: one or more zones; one chaos object is created per zone.
	// Not used for Custom.
	// +optional
	Zones []string `json:"zones,omitempty"`

	// Additional latency to add on the link (e.g. "200ms"). Used with NetworkDelay.
	// +optional
	Latency string `json:"latency,omitempty"`

	// Jitter for latency variance (e.g. "50ms"). Used with NetworkDelay.
	// +optional
	Jitter string `json:"jitter,omitempty"`

	// Maximum bandwidth on the link (e.g. "10Mbps"). Used with NetworkBandwidth.
	// +optional
	Bandwidth string `json:"bandwidth,omitempty"`

	// Packet loss percentage as a plain number string (e.g. "20" for 20%).
	// Used with NetworkLoss.
	// +optional
	PacketLoss string `json:"packetLoss,omitempty"`

	// Number of CPU or memory worker threads.
	// Used with CPUStress and MemoryStress.
	// +optional
	Workers int32 `json:"workers,omitempty"`

	// CPU load percentage per worker (0–100). Used with CPUStress.
	// +optional
	Load int32 `json:"load,omitempty"`

	// Memory allocation size per worker (e.g. "256MiB"). Used with MemoryStress.
	// +optional
	Size string `json:"size,omitempty"`

	// Raw Chaos Mesh manifest in YAML format. Used with Custom.
	// metadata.namespace must be set explicitly in the manifest —
	// no automatic namespace injection is performed.
	// +optional
	Manifest string `json:"manifest,omitempty"`
}

// ChaosPhaseSpec defines the desired fault set for a ChaosPhase.
type ChaosPhaseSpec struct {
	// Reference to the Topology whose zone namespaces are used to resolve
	// zone names to Kubernetes namespaces for all fault types.
	// Required.
	TopologyRef corev1.LocalObjectReference `json:"topologyRef"`

	// Faults to inject during the experiment3's chaos measurement window.
	// +kubebuilder:validation:MinItems=1
	Faults []FaultSpec `json:"faults"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=chaosphases,scope=Namespaced,shortName=cp
// +kubebuilder:printcolumn:name="Topology",type="string",JSONPath=".spec.topologyRef.name"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ChaosPhase defines a reusable, named set of faults to inject during an
// experiment3's chaos measurement window. It acts as a template: no controller
// reconciles it directly. The experiment3 controller reads it at runtime and
// materialises temporary Chaos Mesh objects for the duration of the measurement
// window, cleaning them up afterwards.
type ChaosPhase struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ChaosPhaseSpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ChaosPhaseList contains a list of ChaosPhase.
type ChaosPhaseList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChaosPhase `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ChaosPhase{}, &ChaosPhaseList{})
}
