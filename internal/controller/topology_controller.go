package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	ecosv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

const (
	topologyFinalizer = "topology.ecoscape.cau-se.de/finalizer"

	// Label keys applied to zone namespaces.
	labelEdgeZone     = "ecoscape.cau-se.de/edge-zone"
	labelTopologyName = "ecoscape.cau-se.de/topology"
	labelZoneName     = "ecoscape.cau-se.de/zone"

	// networkChaosAPIVersion is the Chaos Mesh group/version for NetworkChaos.
	networkChaosAPIVersion = "chaos-mesh.org/v1alpha1"
	networkChaosKind       = "NetworkChaos"
)

// networkChaosGVR is used for unstructured list/delete operations.
var networkChaosGVR = schema.GroupVersionResource{
	Group:    "chaos-mesh.org",
	Version:  "v1alpha1",
	Resource: "networkchaoses",
}

// TopologyReconciler reconciles Topology objects.
type TopologyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=topologies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=topologies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=topologies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=resourcequotas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos-mesh.org,resources=networkchaoses,verbs=get;list;watch;create;update;patch;delete

func (r *TopologyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	topology := &ecosv1alpha1.Topology{}
	if err := r.Get(ctx, req.NamespacedName, topology); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion via finalizer.
	if !topology.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, topology)
	}

	// Ensure finalizer is present so we can clean up on deletion.
	if !controllerutil.ContainsFinalizer(topology, topologyFinalizer) {
		controllerutil.AddFinalizer(topology, topologyFinalizer)
		if err := r.Update(ctx, topology); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Skip reconciliation if the observed state already matches the desired state.
	// Status updates increment resourceVersion but not Generation, so this guard
	// prevents the infinite reconcile loop that would otherwise result.
	if topology.Status.ObservedGeneration == topology.Generation {
		if topology.Spec.Active && topology.Status.Phase == ecosv1alpha1.TopologyPhaseActive {
			return ctrl.Result{}, nil
		}
		if !topology.Spec.Active && topology.Status.Phase == ecosv1alpha1.TopologyPhaseInactive {
			return ctrl.Result{}, nil
		}
	}

	if topology.Spec.Active {
		if err := r.reconcileApply(ctx, topology); err != nil {
			logger.Error(err, "failed to apply topology")
			r.setPhase(ctx, topology, ecosv1alpha1.TopologyPhaseFailed, err.Error())
			return ctrl.Result{}, err
		}
	} else {
		if err := r.reconcileDeactivate(ctx, topology); err != nil {
			logger.Error(err, "failed to deactivate topology")
			r.setPhase(ctx, topology, ecosv1alpha1.TopologyPhaseFailed, err.Error())
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// ── Apply ──────────────────────────────────────────────────────────────────

func (r *TopologyReconciler) reconcileApply(ctx context.Context, topology *ecosv1alpha1.Topology) error {
	logger := log.FromContext(ctx)

	// 1. Zones → Namespace + ResourceQuota (always ensured, even when inactive).
	zoneStatuses, err := r.reconcileZones(ctx, topology)
	if err != nil {
		return err
	}

	// 2. Links → NetworkChaos objects.
	var allChaosRefs []string
	for i, link := range topology.Spec.Links {
		if len(link.Zones) != 2 {
			logger.Info("skipping link with wrong number of zones", "index", i, "zones", link.Zones)
			continue
		}
		refs, err := r.ensureLinkChaos(ctx, topology.Name, link)
		if err != nil {
			return fmt.Errorf("link %v: %w", link.Zones, err)
		}
		allChaosRefs = append(allChaosRefs, refs...)
	}

	topology.Status.Zones = zoneStatuses
	topology.Status.NetworkChaosRefs = allChaosRefs
	topology.Status.Phase = ecosv1alpha1.TopologyPhaseActive
	topology.Status.ObservedGeneration = topology.Generation
	if err := r.Status().Update(ctx, topology); err != nil {
		return fmt.Errorf("status update: %w", err)
	}

	logger.Info("topology applied", "zones", len(zoneStatuses), "networkChaosObjects", len(allChaosRefs))
	return nil
}

// ── Deactivate ─────────────────────────────────────────────────────────────

// reconcileDeactivate removes NetworkChaos objects for all links but keeps
// zone namespaces and ResourceQuotas intact so that workloads can be deployed
// into zone namespaces before the topology is activated. It transitions the
// topology to phase Inactive.
func (r *TopologyReconciler) reconcileDeactivate(ctx context.Context, topology *ecosv1alpha1.Topology) error {
	logger := log.FromContext(ctx)
	logger.Info("deactivating topology — removing NetworkChaos, keeping namespaces")

	// Ensure namespaces and quotas exist so the SUT can be deployed into them
	// even while the topology is Inactive.
	zoneStatuses, err := r.reconcileZones(ctx, topology)
	if err != nil {
		return err
	}

	// Delete only the NetworkChaos objects; namespaces and ResourceQuotas stay.
	for _, ref := range topology.Status.NetworkChaosRefs {
		parts := strings.SplitN(ref, "/", 2)
		if len(parts) != 2 {
			continue
		}
		ns, name := parts[0], parts[1]
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "chaos-mesh.org",
			Version: "v1alpha1",
			Kind:    networkChaosKind,
		})
		obj.SetNamespace(ns)
		obj.SetName(name)
		if err := r.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete NetworkChaos", "name", name, "namespace", ns)
		}
	}

	topology.Status.Zones = zoneStatuses
	topology.Status.NetworkChaosRefs = nil
	topology.Status.Phase = ecosv1alpha1.TopologyPhaseInactive
	topology.Status.ObservedGeneration = topology.Generation
	if err := r.Status().Update(ctx, topology); err != nil {
		return fmt.Errorf("status update: %w", err)
	}

	logger.Info("topology deactivated", "zones", len(zoneStatuses))
	return nil
}

// reconcileZones ensures that a namespace and optional ResourceQuota exist for
// every zone declared in the topology spec. It returns the observed zone status
// slice and is called by both reconcileApply and reconcileDeactivate so that
// zone namespaces are always present regardless of spec.active.
func (r *TopologyReconciler) reconcileZones(ctx context.Context, topology *ecosv1alpha1.Topology) ([]ecosv1alpha1.ZoneStatus, error) {
	var zoneStatuses []ecosv1alpha1.ZoneStatus

	for _, zone := range topology.Spec.Zones {
		ns := zoneName(topology.Name, zone.Name)

		if err := r.ensureNamespace(ctx, ns, topology.Name, zone.Name); err != nil {
			return nil, fmt.Errorf("namespace %s: %w", ns, err)
		}

		status := ecosv1alpha1.ZoneStatus{
			Name:      zone.Name,
			Namespace: ns,
		}

		if zone.CPU != nil || zone.Memory != nil || zone.Disk != nil {
			quotaName, err := r.ensureResourceQuota(ctx, topology.Name, ns, zone)
			if err != nil {
				return nil, fmt.Errorf("resource quota for zone %s: %w", zone.Name, err)
			}
			status.ResourceQuotaRef = quotaName
		}

		zoneStatuses = append(zoneStatuses, status)
	}

	return zoneStatuses, nil
}

// ── Delete ─────────────────────────────────────────────────────────────────

func (r *TopologyReconciler) reconcileDelete(ctx context.Context, topology *ecosv1alpha1.Topology) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("deleting topology resources")

	// Delete all tracked NetworkChaos objects.
	for _, ref := range topology.Status.NetworkChaosRefs {
		parts := strings.SplitN(ref, "/", 2)
		if len(parts) != 2 {
			continue
		}
		ns, name := parts[0], parts[1]
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "chaos-mesh.org",
			Version: "v1alpha1",
			Kind:    networkChaosKind,
		})
		obj.SetNamespace(ns)
		obj.SetName(name)
		if err := r.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete NetworkChaos", "name", name, "namespace", ns)
		}
	}

	// Delete zone namespaces (this also deletes all resources inside them).
	for _, zone := range topology.Spec.Zones {
		ns := &corev1.Namespace{}
		nsName := zoneName(topology.Name, zone.Name)
		if err := r.Get(ctx, types.NamespacedName{Name: nsName}, ns); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			logger.Error(err, "failed to get namespace", "namespace", nsName)
			continue
		}
		if err := r.Delete(ctx, ns); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete namespace", "namespace", nsName)
		}
	}

	controllerutil.RemoveFinalizer(topology, topologyFinalizer)
	if err := r.Update(ctx, topology); err != nil {
		return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
	}
	return ctrl.Result{}, nil
}

// ── Namespace ──────────────────────────────────────────────────────────────

func (r *TopologyReconciler) ensureNamespace(ctx context.Context, nsName, topologyName, zoneName string) error {
	ns := &corev1.Namespace{}
	err := r.Get(ctx, types.NamespacedName{Name: nsName}, ns)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desired := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
			Labels: map[string]string{
				labelEdgeZone:     "true",
				labelTopologyName: topologyName,
				labelZoneName:     zoneName,
				// Standard topology label used by Kubernetes scheduling features.
				"topology.kubernetes.io/zone": zoneName,
			},
		},
	}

	if errors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}

	// Namespace exists — ensure labels are up to date.
	if ns.Labels == nil {
		ns.Labels = map[string]string{}
	}
	for k, v := range desired.Labels {
		ns.Labels[k] = v
	}
	return r.Update(ctx, ns)
}

// ── ResourceQuota ──────────────────────────────────────────────────────────

func (r *TopologyReconciler) ensureResourceQuota(ctx context.Context, topologyName, namespace string, zone ecosv1alpha1.ZoneSpec) (string, error) {
	quotaName := "ecoscape-edge-quota"
	hard := corev1.ResourceList{}

	if zone.CPU != nil && zone.CPU.Limit != "" {
		qty, err := resource.ParseQuantity(zone.CPU.Limit)
		if err != nil {
			return "", fmt.Errorf("parse cpu limit %q: %w", zone.CPU.Limit, err)
		}
		hard[corev1.ResourceLimitsCPU] = qty
	}
	if zone.Memory != nil && zone.Memory.Limit != "" {
		qty, err := resource.ParseQuantity(zone.Memory.Limit)
		if err != nil {
			return "", fmt.Errorf("parse memory limit %q: %w", zone.Memory.Limit, err)
		}
		hard[corev1.ResourceLimitsMemory] = qty
	}
	if zone.Disk != nil && zone.Disk.Capacity != "" {
		qty, err := resource.ParseQuantity(zone.Disk.Capacity)
		if err != nil {
			return "", fmt.Errorf("parse disk capacity %q: %w", zone.Disk.Capacity, err)
		}
		hard[corev1.ResourceLimitsEphemeralStorage] = qty
	}

	if len(hard) == 0 {
		return "", nil
	}

	quota := &corev1.ResourceQuota{}
	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: quotaName}, quota)
	if err != nil && !errors.IsNotFound(err) {
		return "", err
	}

	desired := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      quotaName,
			Namespace: namespace,
			Labels: map[string]string{
				labelTopologyName: topologyName,
				labelZoneName:     zone.Name,
			},
		},
		Spec: corev1.ResourceQuotaSpec{Hard: hard},
	}

	if errors.IsNotFound(err) {
		return quotaName, r.Create(ctx, desired)
	}
	quota.Spec = desired.Spec
	return quotaName, r.Update(ctx, quota)
}

// ── NetworkChaos ───────────────────────────────────────────────────────────

// ensureLinkChaos creates or updates all NetworkChaos objects for a single link.
// Returns a slice of "<namespace>/<name>" references for status tracking.
func (r *TopologyReconciler) ensureLinkChaos(ctx context.Context, topologyName string, link ecosv1alpha1.ZoneLinkSpec) ([]string, error) {
	zoneA := link.Zones[0]
	zoneB := link.Zones[1]

	// Ensure consistent ordering so the link [A,B] and [B,A] produce the same names.
	sorted := []string{zoneA, zoneB}
	sort.Strings(sorted)
	nsA := zoneName(topologyName, sorted[0])
	nsB := zoneName(topologyName, sorted[1])
	baseName := linkBaseName(topologyName, sorted[0], sorted[1])

	var refs []string

	// delay
	if link.Latency != "" {
		name := baseName + "-delay"
		spec := map[string]interface{}{
			"action":    "delay",
			"mode":      "all",
			"direction": "both",
			"selector": map[string]interface{}{
				"namespaces": []interface{}{nsA},
				"labelSelectors": map[string]interface{}{
					"ecoscape": "true",
				},
			},
			"target": map[string]interface{}{
				"mode": "all",
				"selector": map[string]interface{}{
					"namespaces": []interface{}{nsB},
					"labelSelectors": map[string]interface{}{
						"ecoscape": "true",
					},
				},
			},
			"delay": buildDelay(link.Latency, link.Jitter),
		}
		if err := r.ensureNetworkChaos(ctx, nsA, name, topologyName, spec); err != nil {
			return refs, fmt.Errorf("delay chaos: %w", err)
		}
		refs = append(refs, nsA+"/"+name)
	}

	// bandwidth
	if link.Bandwidth != "" {
		name := baseName + "-bandwidth"
		spec := map[string]interface{}{
			"action":    "bandwidth",
			"mode":      "all",
			"direction": "both",
			"selector": map[string]interface{}{
				"namespaces": []interface{}{nsA},
				"labelSelectors": map[string]interface{}{
					"ecoscape": "true",
				},
			},
			"target": map[string]interface{}{
				"mode": "all",
				"selector": map[string]interface{}{
					"namespaces": []interface{}{nsB},
					"labelSelectors": map[string]interface{}{
						"ecoscape": "true",
					},
				},
			},
			"bandwidth": buildBandwidth(link.Bandwidth),
		}
		if err := r.ensureNetworkChaos(ctx, nsA, name, topologyName, spec); err != nil {
			return refs, fmt.Errorf("bandwidth chaos: %w", err)
		}
		refs = append(refs, nsA+"/"+name)
	}

	// loss
	if link.PacketLoss != "" {
		name := baseName + "-loss"
		spec := map[string]interface{}{
			"action":    "loss",
			"mode":      "all",
			"direction": "both",
			"selector": map[string]interface{}{
				"namespaces": []interface{}{nsA},
				"labelSelectors": map[string]interface{}{
					"ecoscape": "true",
				},
			},
			"target": map[string]interface{}{
				"mode": "all",
				"selector": map[string]interface{}{
					"namespaces": []interface{}{nsB},
					"labelSelectors": map[string]interface{}{
						"ecoscape": "true",
					},
				},
			},
			"loss": map[string]interface{}{
				"loss":        link.PacketLoss,
				"correlation": "50",
			},
		}
		if err := r.ensureNetworkChaos(ctx, nsA, name, topologyName, spec); err != nil {
			return refs, fmt.Errorf("loss chaos: %w", err)
		}
		refs = append(refs, nsA+"/"+name)
	}

	return refs, nil
}

func (r *TopologyReconciler) ensureNetworkChaos(ctx context.Context, namespace, name, topologyName string, spec map[string]interface{}) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "chaos-mesh.org",
		Version: "v1alpha1",
		Kind:    networkChaosKind,
	})
	obj.SetNamespace(namespace)
	obj.SetName(name)

	err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, obj)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		obj = &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": networkChaosAPIVersion,
				"kind":       networkChaosKind,
				"metadata": map[string]interface{}{
					"name":      name,
					"namespace": namespace,
					"labels": map[string]interface{}{
						labelTopologyName: topologyName,
					},
				},
				"spec": spec,
			},
		}
		return r.Create(ctx, obj)
	}

	// Update existing object.
	obj.Object["spec"] = spec
	return r.Update(ctx, obj)
}

// ── Helpers ────────────────────────────────────────────────────────────────

// zoneName returns the namespace name for a zone: "<topology>-<zone>".
func zoneName(topologyName, zone string) string {
	return topologyName + "-" + zone
}

// linkBaseName returns a stable base name for NetworkChaos objects on a link.
// Zones must already be sorted alphabetically before calling.
func linkBaseName(topologyName, zoneA, zoneB string) string {
	// Keep names within Kubernetes 63-char limit for generated suffixes.
	return "top-" + topologyName + "-" + zoneA + "-" + zoneB
}

func buildDelay(latency, jitter string) map[string]interface{} {
	d := map[string]interface{}{
		"latency":     latency,
		"correlation": "50",
	}
	if jitter != "" {
		d["jitter"] = jitter
	}
	return d
}

func buildBandwidth(bandwidth string) map[string]interface{} {
	// Chaos Mesh expects lowercase rate strings like "50mbps", "1mbps".
	rate := strings.ToLower(bandwidth)
	return map[string]interface{}{
		"rate":   rate,
		"limit":  int64(100000),
		"buffer": int64(10000),
	}
}

func (r *TopologyReconciler) setPhase(ctx context.Context, topology *ecosv1alpha1.Topology, phase ecosv1alpha1.TopologyPhase, message string) {
	topology.Status.Phase = phase
	conditionStatus := metav1.ConditionTrue
	if phase == ecosv1alpha1.TopologyPhaseFailed {
		conditionStatus = metav1.ConditionFalse
	}
	cond := metav1.Condition{
		Type:               "Ready",
		Status:             conditionStatus,
		LastTransitionTime: metav1.Now(),
		Reason:             string(phase),
		Message:            message,
	}
	// Replace existing Ready condition.
	for i, c := range topology.Status.Conditions {
		if c.Type == "Ready" {
			topology.Status.Conditions[i] = cond
			_ = r.Status().Update(ctx, topology)
			return
		}
	}
	topology.Status.Conditions = append(topology.Status.Conditions, cond)
	_ = r.Status().Update(ctx, topology)
}

// SetupWithManager registers the controller with the Manager.
func (r *TopologyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ecosv1alpha1.Topology{}).
		Named("topology").
		Complete(r)
}
