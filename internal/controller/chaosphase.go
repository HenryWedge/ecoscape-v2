package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/log"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

// applyChaosPhase reads the given ChaosPhase and materialises temporary Chaos Mesh
// objects for every declared fault. It returns a slice of "<namespace>/<name>"
// references that must be passed to cleanupChaosPhase after the measurement window.
func (r *ExperimentReconciler) applyChaosPhase(
	ctx context.Context,
	cp *experimentv1alpha1.ChaosPhase,
	experimentID string,
) ([]string, error) {
	logger := log.FromContext(ctx)
	topologyName := cp.Spec.TopologyRef.Name
	var refs []string

	for i, fault := range cp.Spec.Faults {
		var created []string
		var err error

		switch fault.Type {
		case experimentv1alpha1.FaultNetworkDelay:
			created, err = r.applyNetworkFault(ctx, topologyName, experimentID, fault, "delay", buildChaosPhasDelay(fault))
		case experimentv1alpha1.FaultNetworkBandwidth:
			created, err = r.applyNetworkFault(ctx, topologyName, experimentID, fault, "bandwidth", buildChaosPhasesBandwidth(fault))
		case experimentv1alpha1.FaultNetworkLoss:
			created, err = r.applyNetworkFault(ctx, topologyName, experimentID, fault, "loss", buildChaosPhaseLoss(fault))
		case experimentv1alpha1.FaultNetworkPartition:
			created, err = r.applyNetworkFault(ctx, topologyName, experimentID, fault, "partition", nil)
		case experimentv1alpha1.FaultCPUStress:
			created, err = r.applyStressFault(ctx, topologyName, experimentID, fault, "cpu", buildChaosPhaseStressCPU(fault))
		case experimentv1alpha1.FaultMemoryStress:
			created, err = r.applyStressFault(ctx, topologyName, experimentID, fault, "memory", buildChaosPhaseStressMemory(fault))
		case experimentv1alpha1.FaultCustom:
			created, err = r.applyCustomFault(ctx, fault)
		default:
			logger.Info("unknown fault type, skipping", "type", fault.Type, "index", i)
			continue
		}

		if err != nil {
			return refs, fmt.Errorf("fault[%d] (%s): %w", i, fault.Type, err)
		}
		refs = append(refs, created...)
	}

	return refs, nil
}

// cleanupChaosPhase deletes all Chaos Mesh objects previously created by applyChaosPhase.
// Refs have the format "<group>/<kind>/<namespace>/<name>".
// NotFound errors are silently ignored — cleanup is best-effort.
func (r *ExperimentReconciler) cleanupChaosPhase(ctx context.Context, refs []string) {
	logger := log.FromContext(ctx)
	for _, ref := range refs {
		parts := strings.SplitN(ref, "/", 4)
		if len(parts) != 4 {
			logger.Info("skipping malformed chaos ref", "ref", ref)
			continue
		}
		group, kind, ns, name := parts[0], parts[1], parts[2], parts[3]
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   group,
			Version: "v1alpha1",
			Kind:    kind,
		})
		obj.SetNamespace(ns)
		obj.SetName(name)
		if err := r.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete chaos object", "ref", ref)
		}
	}
}

// ── Network faults ──────────────────────────────────────────────────────────

// applyNetworkFault creates a single NetworkChaos object between the two zones
// declared in the fault. Zones are sorted alphabetically for stable naming.
// actionSpec is the action-specific sub-object (e.g. "delay", "bandwidth", "loss");
// for partition it is nil.
func (r *ExperimentReconciler) applyNetworkFault(
	ctx context.Context,
	topologyName, experimentID string,
	fault experimentv1alpha1.FaultSpec,
	action string,
	actionSpec map[string]interface{},
) ([]string, error) {
	if len(fault.Zones) != 2 {
		return nil, fmt.Errorf("network fault requires exactly 2 zones, got %d", len(fault.Zones))
	}

	sorted := []string{fault.Zones[0], fault.Zones[1]}
	sort.Strings(sorted)
	nsA := zoneName(topologyName, sorted[0])
	nsB := zoneName(topologyName, sorted[1])

	name := fmt.Sprintf("cp-%s-%s-%s-%s", experimentID, sorted[0], sorted[1], action)

	spec := map[string]interface{}{
		"action":    action,
		"mode":      "all",
		"direction": "both",
		"selector": map[string]interface{}{
			"namespaces": []interface{}{nsA},
		},
		"target": map[string]interface{}{
			"mode": "all",
			"selector": map[string]interface{}{
				"namespaces": []interface{}{nsB},
			},
		},
	}
	if actionSpec != nil {
		spec[action] = actionSpec
	}

	obj := buildNetworkChaos(nsA, name, spec)
	if err := r.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create NetworkChaos %s/%s: %w", nsA, name, err)
	}
	return []string{chaosMeshGroup + "/NetworkChaos/" + nsA + "/" + name}, nil
}

// ── Stress faults ───────────────────────────────────────────────────────────

// applyStressFault creates one StressChaos object per zone namespace.
func (r *ExperimentReconciler) applyStressFault(
	ctx context.Context,
	topologyName, experimentID string,
	fault experimentv1alpha1.FaultSpec,
	variant string,
	stressors map[string]interface{},
) ([]string, error) {
	if len(fault.Zones) == 0 {
		return nil, fmt.Errorf("stress fault requires at least one zone")
	}

	var refs []string
	for _, zone := range fault.Zones {
		ns := zoneName(topologyName, zone)
		name := fmt.Sprintf("cp-%s-%s-%s", experimentID, zone, variant)

		spec := map[string]interface{}{
			"mode": "all",
			"selector": map[string]interface{}{
				"namespaces": []interface{}{ns},
			},
			"stressors": stressors,
		}

		obj := buildStressChaos(ns, name, spec)
		if err := r.Create(ctx, obj); err != nil {
			return refs, fmt.Errorf("create StressChaos %s/%s: %w", ns, name, err)
		}
		refs = append(refs, chaosMeshGroup+"/StressChaos/"+ns+"/"+name)
	}
	return refs, nil
}

// ── Custom fault ────────────────────────────────────────────────────────────

// applyCustomFault parses the raw YAML in fault.Manifest and creates the object
// as-is. metadata.namespace must be set in the manifest; if it is empty the
// function returns an error rather than injecting a default.
func (r *ExperimentReconciler) applyCustomFault(
	ctx context.Context,
	fault experimentv1alpha1.FaultSpec,
) ([]string, error) {
	if fault.Manifest == "" {
		return nil, fmt.Errorf("custom fault has empty manifest")
	}

	obj, err := parseManifest(fault.Manifest)
	if err != nil {
		return nil, fmt.Errorf("parse custom manifest: %w", err)
	}
	if obj.GetNamespace() == "" {
		return nil, fmt.Errorf("custom fault manifest must set metadata.namespace explicitly")
	}

	if err := r.Create(ctx, obj); err != nil {
		return nil, fmt.Errorf("create custom chaos object %s/%s: %w", obj.GetNamespace(), obj.GetName(), err)
	}

	group := obj.GroupVersionKind().Group
	kind := obj.GroupVersionKind().Kind
	ref := group + "/" + kind + "/" + obj.GetNamespace() + "/" + obj.GetName()
	return []string{ref}, nil
}

// ── Unstructured builders ───────────────────────────────────────────────────

const chaosMeshGroup = "chaos-mesh.org"

func buildNetworkChaos(namespace, name string, spec map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": chaosMeshGroup + "/v1alpha1",
			"kind":       "NetworkChaos",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		},
	}
}

func buildStressChaos(namespace, name string, spec map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": chaosMeshGroup + "/v1alpha1",
			"kind":       "StressChaos",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		},
	}
}

// ── Spec builders ───────────────────────────────────────────────────────────

func buildChaosPhasDelay(fault experimentv1alpha1.FaultSpec) map[string]interface{} {
	d := map[string]interface{}{
		"latency":     fault.Latency,
		"correlation": "50",
	}
	if fault.Jitter != "" {
		d["jitter"] = fault.Jitter
	}
	return d
}

func buildChaosPhasesBandwidth(fault experimentv1alpha1.FaultSpec) map[string]interface{} {
	return map[string]interface{}{
		"rate":   strings.ToLower(fault.Bandwidth),
		"limit":  int64(100000),
		"buffer": int64(10000),
	}
}

func buildChaosPhaseLoss(fault experimentv1alpha1.FaultSpec) map[string]interface{} {
	return map[string]interface{}{
		"loss":        fault.PacketLoss,
		"correlation": "50",
	}
}

func buildChaosPhaseStressCPU(fault experimentv1alpha1.FaultSpec) map[string]interface{} {
	return map[string]interface{}{
		"cpu": map[string]interface{}{
			"workers": int64(fault.Workers),
			"load":    int64(fault.Load),
		},
	}
}

func buildChaosPhaseStressMemory(fault experimentv1alpha1.FaultSpec) map[string]interface{} {
	return map[string]interface{}{
		"memory": map[string]interface{}{
			"workers": int64(fault.Workers),
			"size":    fault.Size,
		},
	}
}
