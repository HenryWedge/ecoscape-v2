package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

const charset = "abcdefghijklmnopqrstuvwxyz"

func randomID(n int) string {
	bytes := make([]byte, n)
	for i := range bytes {
		bytes[i] = charset[rand.Intn(len(charset))]
	}
	return string(bytes)
}

func derefOr(duration *int32, fallback int32) int32 {
	if duration != nil {
		return *duration
	}
	return fallback
}

// ExperimentReconciler reconciles a Experiment object.
type ExperimentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=experiments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=experiments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=experiments/finalizers,verbs=update
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=topologies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=chaosphases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=chaosphases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ecoscape.cau-se.de,resources=chaosphases/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;services;endpoints;events;namespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings;clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors;podmonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=chaos-mesh.org,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kafka.strimzi.io,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (reconciler *ExperimentReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	experiment := &experimentv1alpha1.Experiment{}
	if err := reconciler.Get(ctx, request.NamespacedName, experiment); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Terminal phases — nothing left to do.
	if experiment.Status.Phase == experimentv1alpha1.PhaseSucceeded ||
		experiment.Status.Phase == experimentv1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
	}

	// All repetitions done — finalize.
	repetitions := experiment.Spec.Duration.Repetitions
	if repetitions == 0 {
		repetitions = 1
	}
	if experiment.Status.RepetitionsCompleted >= repetitions {
		logger.Info("all repetitions completed, finalizing", "repetitions", repetitions)
		completed := metav1.Now()
		experiment.Status.CompletionTime = &completed
		if err := reconciler.setPhase(ctx, experiment, experimentv1alpha1.PhaseSucceeded, ""); err != nil {
			return ctrl.Result{}, fmt.Errorf("set phase Succeeded: %w", err)
		}
		return ctrl.Result{}, nil
	}

	return reconciler.dispatchSubPhase(ctx, experiment)
}

// dispatchSubPhase routes to the appropriate handler based on the current SubPhase.
func (reconciler *ExperimentReconciler) dispatchSubPhase(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	switch experiment.Status.SubPhase {
	case "":
		return reconciler.handleIdle(ctx, experiment)
	case experimentv1alpha1.SubPhaseDeploying:
		return reconciler.handleDeploying(ctx, experiment)
	case experimentv1alpha1.SubPhaseWaitingForTopology:
		return reconciler.handleWaitingForTopology(ctx, experiment)
	case experimentv1alpha1.SubPhaseTopologyDelay:
		return reconciler.handleTopologyDelay(ctx, experiment)
	case experimentv1alpha1.SubPhaseLoadDelay:
		return reconciler.handleTimedWait(ctx, experiment, experimentv1alpha1.SubPhasePreChaosMeasurement,
			derefOr(experiment.Spec.Duration.ChaosDelay, 0))
	case experimentv1alpha1.SubPhasePreChaosMeasurement:
		return reconciler.handleMeasurement(ctx, experiment, experimentv1alpha1.SubPhaseApplyingChaos)
	case experimentv1alpha1.SubPhaseApplyingChaos:
		return reconciler.handleApplyingChaos(ctx, experiment)
	case experimentv1alpha1.SubPhaseChaosMeasurement:
		return reconciler.handleMeasurement(ctx, experiment, experimentv1alpha1.SubPhaseCleanup)
	case experimentv1alpha1.SubPhaseCleanup:
		return reconciler.handleCleanup(ctx, experiment)
	case experimentv1alpha1.SubPhasePause:
		return reconciler.handleTimedWait(ctx, experiment, "", 0)
	default:
		return ctrl.Result{}, fmt.Errorf("unknown sub-phase %q", experiment.Status.SubPhase)
	}
}

// handleIdle initializes a new repetition and transitions to Deploying.
func (reconciler *ExperimentReconciler) handleIdle(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	base := experiment.DeepCopy()

	if experiment.Status.ExperimentID == "" {
		experiment.Status.ExperimentID = randomID(5)
	}
	if experiment.Status.StartTime == nil {
		now := metav1.Now()
		experiment.Status.StartTime = &now
	}
	if experiment.Spec.TopologyRef != nil {
		experiment.Status.TopologyRef = experiment.Spec.TopologyRef.Name
	}

	repetition := experiment.Status.RepetitionsCompleted + 1
	logger.Info("starting repetition", "repetition", repetition,
		"total", experiment.Spec.Duration.Repetitions,
		"experimentID", experiment.Status.ExperimentID)

	experiment.Status.Phase = experimentv1alpha1.PhaseRunning
	experiment.Status.MeasurementState = nil
	experiment.Status.SubPhase = experimentv1alpha1.SubPhaseDeploying
	experiment.Status.SubPhaseStartTime = nil
	experiment.Status.SubPhaseDuration = 0
	experiment.Status.SubPhaseElapsed = 0

	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (idle→deploying): %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// handleDeploying deploys all manifest sets and transitions to the next sub-phase.
func (reconciler *ExperimentReconciler) handleDeploying(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := experiment.GetNamespace()
	experimentID := experiment.Status.ExperimentID

	var ownedConfigMaps []string

	if err := reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Sut, experimentID, "sut", &ownedConfigMaps); err != nil {
		reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment, fmt.Errorf("deploy sut: %w", err))
	}
	if err := reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Infra, experimentID, "infra", &ownedConfigMaps); err != nil {
		reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment, fmt.Errorf("deploy infra: %w", err))
	}
	if err := reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Monitor, experimentID, "monitor", &ownedConfigMaps); err != nil {
		reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment, fmt.Errorf("deploy monitor: %w", err))
	}
	if err := reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Load, experimentID, "load", &ownedConfigMaps); err != nil {
		reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment, fmt.Errorf("deploy load: %w", err))
	}

	// Determine next sub-phase.
	var nextSubPhase experimentv1alpha1.ExperimentSubPhase
	var nextDuration int32
	if experiment.Spec.TopologyRef != nil {
		nextSubPhase = experimentv1alpha1.SubPhaseTopologyDelay
		nextDuration = derefOr(experiment.Spec.Duration.TopologyDelay, 0)
		logger.Info("manifests deployed, waiting topology delay before activating topology")
	} else {
		nextSubPhase = experimentv1alpha1.SubPhaseLoadDelay
		nextDuration = derefOr(experiment.Spec.Duration.LoadDelay, 0)
		logger.Info("manifests deployed, no topology ref, proceeding to load delay")
	}

	base := experiment.DeepCopy()
	experiment.Status.ManifestConfigMaps = ownedConfigMaps
	reconciler.setSubPhase(experiment, nextSubPhase, nextDuration)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (deploying→%s): %w", nextSubPhase, err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// handleTopologyDelay waits for the configured TopologyDelay so that the SUT
// has time to start up before the topology is activated. Once the delay has
// elapsed the topology is activated and the sub-phase transitions to
// WaitingForTopology.
func (reconciler *ExperimentReconciler) handleTopologyDelay(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	elapsed := reconciler.elapsedSeconds(experiment)
	base := experiment.DeepCopy()
	experiment.Status.SubPhaseElapsed = elapsed

	if elapsed >= experiment.Status.SubPhaseDuration {
		logger.Info("topology delay elapsed, activating topology",
			"topology", experiment.Spec.TopologyRef.Name)
		namespace := experiment.GetNamespace()
		if err := reconciler.activateTopology(ctx, namespace, experiment.Spec.TopologyRef.Name); err != nil {
			reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)
			return reconciler.failExperiment(ctx, experiment,
				fmt.Errorf("activate topology: %w", err))
		}
		reconciler.setSubPhase(experiment, experimentv1alpha1.SubPhaseWaitingForTopology, 0)
		if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, fmt.Errorf("status patch (topology delay done): %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	remaining := experiment.Status.SubPhaseDuration - elapsed
	logger.Info("topology delay countdown",
		"elapsed_seconds", elapsed, "remaining_seconds", remaining)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (topology delay): %w", err)
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// handleWaitingForTopology polls the Topology until it reaches Active phase.
func (reconciler *ExperimentReconciler) handleWaitingForTopology(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := experiment.GetNamespace()
	name := experiment.Spec.TopologyRef.Name

	topology := &experimentv1alpha1.Topology{}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, topology)
	if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("get topology %s/%s: %w", namespace, name, err)
	}

	elapsed := reconciler.elapsedSeconds(experiment)
	base := experiment.DeepCopy()
	experiment.Status.SubPhaseElapsed = elapsed

	if err == nil {
		switch topology.Status.Phase {
		case experimentv1alpha1.TopologyPhaseActive:
			logger.Info("topology is Active, proceeding to load delay", "topology", name)
			loadDelay := derefOr(experiment.Spec.Duration.LoadDelay, 0)
			reconciler.setSubPhase(experiment, experimentv1alpha1.SubPhaseLoadDelay, loadDelay)
			if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, fmt.Errorf("status patch (topology active): %w", err)
			}
			return ctrl.Result{Requeue: true}, nil

		case experimentv1alpha1.TopologyPhaseFailed:
			reconciler.deactivateTopology(ctx, namespace, name) //nolint:errcheck
			reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)
			return reconciler.failExperiment(ctx, experiment,
				fmt.Errorf("topology %s/%s reached Failed phase", namespace, name))
		}
	}

	// Topology not yet Active — check timeout (5 minutes).
	if elapsed > 300 {
		reconciler.deactivateTopology(ctx, namespace, name) //nolint:errcheck
		reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment,
			fmt.Errorf("timeout waiting for topology %s/%s to become Active", namespace, name))
	}

	logger.Info("waiting for topology to become Active",
		"topology", name, "elapsed_seconds", elapsed)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (waiting for topology): %w", err)
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// handleTimedWait is a generic timed-wait handler. It counts elapsed seconds
// in the status and transitions to nextSubPhase once SubPhaseDuration is reached.
// When nextSubPhase is "" the handler transitions to idle (i.e. SubPhase = ""),
// which starts the next repetition.
func (reconciler *ExperimentReconciler) handleTimedWait(
	ctx context.Context,
	experiment *experimentv1alpha1.Experiment,
	nextSubPhase experimentv1alpha1.ExperimentSubPhase,
	nextDuration int32,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	elapsed := reconciler.elapsedSeconds(experiment)
	base := experiment.DeepCopy()
	experiment.Status.SubPhaseElapsed = elapsed

	if elapsed >= experiment.Status.SubPhaseDuration {
		logger.Info("timed wait complete, transitioning",
			"subPhase", experiment.Status.SubPhase,
			"next", nextSubPhase)
		reconciler.setSubPhase(experiment, nextSubPhase, nextDuration)
		if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, fmt.Errorf("status patch (timed wait done): %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	remaining := experiment.Status.SubPhaseDuration - elapsed
	logger.Info("timed wait countdown",
		"subPhase", experiment.Status.SubPhase,
		"elapsed_seconds", elapsed,
		"remaining_seconds", remaining)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (timed wait): %w", err)
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// handleMeasurement fires one round of Prometheus queries, updates the
// MeasurementState aggregates in the status, and transitions to nextSubPhase
// once the measurement window is complete.
func (reconciler *ExperimentReconciler) handleMeasurement(
	ctx context.Context,
	experiment *experimentv1alpha1.Experiment,
	nextSubPhase experimentv1alpha1.ExperimentSubPhase,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	elapsed := reconciler.elapsedSeconds(experiment)
	base := experiment.DeepCopy()
	sampleInterval := derefOr(experiment.Spec.Duration.MeasurementSampleInterval, 5)

	if elapsed != 0 && elapsed%sampleInterval != 0 {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Initialize MeasurementState on first entry.
	if len(experiment.Status.MeasurementState) == 0 {
		state := make([]experimentv1alpha1.SLOMeasurementState, len(experiment.Spec.SLOs))
		for i, slo := range experiment.Spec.SLOs {
			state[i] = experimentv1alpha1.SLOMeasurementState{Name: slo.Name}
		}
		experiment.Status.MeasurementState = state
	}

	prometheusClient := newPrometheusClient(
		experiment.Spec.Prometheus.URL,
		derefOr(experiment.Spec.Prometheus.QueryTimeoutSeconds, 10),
	)

	// Fire one query per SLO and update aggregates.
	for i, slo := range experiment.Spec.SLOs {
		value, found, err := prometheusClient.query(ctx, slo.Query)
		if err != nil {
			logger.Error(err, "prometheus query failed", "slo", slo.Name, "query", slo.Query)
			continue
		}
		if !found {
			logger.Info("prometheus query returned no result", "slo", slo.Name)
			continue
		}
		logger.Info("prometheus value", "slo", slo.Name,
			"value", strconv.FormatFloat(value, 'f', 4, 64))
		recordMeasurement(&experiment.Status.MeasurementState[i], value, slo, elapsed, sampleInterval)
	}

	experiment.Status.SubPhaseElapsed = elapsed

	if elapsed >= experiment.Status.SubPhaseDuration {
		logger.Info("measurement window complete, transitioning",
			"subPhase", experiment.Status.SubPhase, "next", nextSubPhase)

		// Write individual measurements to the ConfigMap before clearing state.
		currentPhase := experiment.Status.SubPhase
		if err := reconciler.appendMeasurementsToConfigMap(ctx, experiment.GetNamespace(), experiment, currentPhase); err != nil {
			return reconciler.failExperiment(ctx, experiment,
				fmt.Errorf("write measurements configmap: %w", err))
		}

		// PreChaosMeasurement → skip ApplyingChaos if no ChaosPhaseRef configured.
		actualNext := nextSubPhase
		if nextSubPhase == experimentv1alpha1.SubPhaseApplyingChaos &&
			experiment.Spec.ChaosPhaseRef == nil {
			actualNext = experimentv1alpha1.SubPhaseChaosMeasurement
		}
		measurementDuration := derefOr(experiment.Spec.Duration.MeasurementDuration, 60)
		reconciler.setSubPhase(experiment, actualNext, measurementDuration)
		// Reset measurement state when transitioning to chaos measurement so
		// pre-chaos and chaos windows are tracked independently.
		if actualNext == experimentv1alpha1.SubPhaseChaosMeasurement {
			experiment.Status.MeasurementState = nil
		}
		if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, fmt.Errorf("status patch (measurement done): %w", err)
		}
		return ctrl.Result{Requeue: true}, nil
	}

	remaining := experiment.Status.SubPhaseDuration - elapsed
	logger.Info("measurement countdown",
		"subPhase", experiment.Status.SubPhase,
		"elapsed_seconds", elapsed,
		"remaining_seconds", remaining)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (measurement): %w", err)
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// handleApplyingChaos applies the ChaosPhase and immediately transitions to
// ChaosMeasurement.
func (reconciler *ExperimentReconciler) handleApplyingChaos(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := experiment.GetNamespace()

	cp := &experimentv1alpha1.ChaosPhase{}
	if err := reconciler.Get(ctx, types.NamespacedName{
		Namespace: namespace,
		Name:      experiment.Spec.ChaosPhaseRef.Name,
	}, cp); err != nil {
		reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment,
			fmt.Errorf("get ChaosPhase %q: %w", experiment.Spec.ChaosPhaseRef.Name, err))
	}

	refs, err := reconciler.applyChaosPhase(ctx, cp, experiment.Status.ExperimentID)
	if err != nil {
		reconciler.cleanupChaosPhase(ctx, refs)
		reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)
		return reconciler.failExperiment(ctx, experiment,
			fmt.Errorf("apply chaos phase: %w", err))
	}

	logger.Info("chaos phase applied, starting chaos measurement",
		"refs", refs)

	measurementDuration := derefOr(experiment.Spec.Duration.MeasurementDuration, 60)
	base := experiment.DeepCopy()
	experiment.Status.ManifestConfigMaps = append(experiment.Status.ManifestConfigMaps, refs...)
	experiment.Status.MeasurementState = nil
	reconciler.setSubPhase(experiment, experimentv1alpha1.SubPhaseChaosMeasurement, measurementDuration)
	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (applying chaos): %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// handleCleanup tears down all resources for the current repetition, writes
// the result to the ConfigMap, increments RepetitionsCompleted, and either
// transitions to Pause or back to idle for the next repetition.
func (reconciler *ExperimentReconciler) handleCleanup(ctx context.Context, experiment *experimentv1alpha1.Experiment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	namespace := experiment.GetNamespace()
	repetition := experiment.Status.RepetitionsCompleted + 1

	// Separate chaos refs from manifest refs before cleanup.
	reconciler.cleanupChaosPhase(ctx, experiment.Status.ManifestConfigMaps)
	reconciler.cleanup(ctx, namespace, experiment.Status.ManifestConfigMaps, experiment)

	if experiment.Spec.TopologyRef != nil {
		logger.Info("deactivating topology", "topology", experiment.Spec.TopologyRef.Name)
		if err := reconciler.deactivateTopology(ctx, namespace, experiment.Spec.TopologyRef.Name); err != nil {
			logger.Error(err, "failed to deactivate topology")
		}
	}

	// Build RepetitionResult from current MeasurementState.
	result := buildRepetitionResult(repetition, experiment.Spec.SLOs, experiment.Status.MeasurementState)

	if err := reconciler.appendRepetitionToConfigMap(ctx, namespace, experiment, result); err != nil {
		return reconciler.failExperiment(ctx, experiment,
			fmt.Errorf("append repetition to configmap: %w", err))
	}

	logger.Info("repetition complete",
		"repetition", repetition,
		"aggregateScore", result.AggregateScore)

	repetitions := experiment.Spec.Duration.Repetitions
	if repetitions == 0 {
		repetitions = 1
	}
	pauseBetweenRepetitions := experiment.Spec.Duration.PauseBetweenRepetitions

	base := experiment.DeepCopy()
	experiment.Status.RepetitionsCompleted = repetition
	experiment.Status.SloResults = result.SloResults
	experiment.Status.AggregateScore = float64Ptr(result.AggregateScore)
	experiment.Status.ManifestConfigMaps = nil
	experiment.Status.MeasurementState = nil

	if repetition < repetitions && pauseBetweenRepetitions > 0 {
		reconciler.setSubPhase(experiment, experimentv1alpha1.SubPhasePause, pauseBetweenRepetitions)
	} else {
		reconciler.setSubPhase(experiment, "", 0)
	}

	if err := reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base)); err != nil {
		return ctrl.Result{}, fmt.Errorf("status patch (cleanup): %w", err)
	}
	return ctrl.Result{Requeue: true}, nil
}

// setSubPhase updates the SubPhase fields on the experiment (does not patch).
func (reconciler *ExperimentReconciler) setSubPhase(
	experiment *experimentv1alpha1.Experiment,
	subPhase experimentv1alpha1.ExperimentSubPhase,
	duration int32,
) {
	now := metav1.Now()
	experiment.Status.SubPhase = subPhase
	experiment.Status.SubPhaseStartTime = &now
	experiment.Status.SubPhaseDuration = duration
	experiment.Status.SubPhaseElapsed = 0
}

// elapsedSeconds returns the number of whole seconds since SubPhaseStartTime.
func (reconciler *ExperimentReconciler) elapsedSeconds(experiment *experimentv1alpha1.Experiment) int32 {
	if experiment.Status.SubPhaseStartTime == nil {
		return 0
	}
	return int32(time.Since(experiment.Status.SubPhaseStartTime.Time).Seconds())
}

// failExperiment sets the experiment phase to Failed and returns the error so
// the reconciler propagates it for backoff retry.
func (reconciler *ExperimentReconciler) failExperiment(ctx context.Context, experiment *experimentv1alpha1.Experiment, err error) (ctrl.Result, error) {
	log.FromContext(ctx).Error(err, "experiment failed")
	if setErr := reconciler.setPhase(ctx, experiment, experimentv1alpha1.PhaseFailed, err.Error()); setErr != nil {
		log.FromContext(ctx).Error(setErr, "failed to set phase to Failed")
	}
	return ctrl.Result{}, err
}

func (reconciler *ExperimentReconciler) setPhase(ctx context.Context, experiment *experimentv1alpha1.Experiment, phase experimentv1alpha1.ExperimentPhase, message string) error {
	base := experiment.DeepCopy()
	experiment.Status.Phase = phase
	condition := metav1.Condition{
		Type:               "Completed",
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             string(phase),
		Message:            message,
	}
	if phase == experimentv1alpha1.PhaseSucceeded || phase == experimentv1alpha1.PhaseFailed {
		experiment.Status.Conditions = append(experiment.Status.Conditions, condition)
	}
	return reconciler.Status().Patch(ctx, experiment, client.MergeFrom(base))
}

// recordMeasurementAggregate updates the running aggregates (Sum, Count,
// Min, Max, Violations) for a single SLOMeasurementState. It is called on
// every reconcile during a measurement window.
func recordMeasurementAggregate(state *experimentv1alpha1.SLOMeasurementState, value float64, slo experimentv1alpha1.SLOConfig) {
	state.Sum += value
	state.Count++
	if !state.MinSet || value < state.Min {
		state.Min = value
		state.MinSet = true
	}
	if value > state.Max {
		state.Max = value
	}

	if slo.IsBiggerBetter {
		if value < slo.Threshold {
			state.Violations++
		}
	} else {
		if value > slo.Threshold {
			state.Violations++
		}
	}
}

// recordMeasurement updates aggregates and, if the current elapsed second
// falls on a sample boundary, also appends the value to Values[] for the
// time-series ConfigMap.
func recordMeasurement(state *experimentv1alpha1.SLOMeasurementState, value float64, slo experimentv1alpha1.SLOConfig, elapsed, sampleInterval int32) {
	if elapsed%sampleInterval == 0 {
		recordMeasurementAggregate(state, value, slo)
		state.Values = append(state.Values, value)
	}
}

// buildRepetitionResult computes a RepetitionResult from the accumulated
// MeasurementState for one repetition.
func buildRepetitionResult(
	repetition int32,
	slos []experimentv1alpha1.SLOConfig,
	states []experimentv1alpha1.SLOMeasurementState,
) experimentv1alpha1.RepetitionResult {
	// Build a lookup so we can find the weight for each SLO by name.
	sloByName := make(map[string]experimentv1alpha1.SLOConfig, len(slos))
	for _, slo := range slos {
		sloByName[slo.Name] = slo
	}

	sloResults := make([]experimentv1alpha1.SLOResult, len(states))
	var aggregateScore float64

	for i, state := range states {
		var meanSli float64
		var violationScore float64
		if state.Count > 0 {
			meanSli = state.Sum / float64(state.Count)
			slo := sloByName[state.Name]
			var totalViolationScore float64
			// Recompute violation score as the normalised average overshoot.
			// We only have aggregate data so we approximate using the mean.
			if slo.IsBiggerBetter {
				if meanSli < slo.Threshold {
					totalViolationScore = 1 - (meanSli / slo.Threshold)
				}
			} else {
				if meanSli > slo.Threshold {
					totalViolationScore = 1 - (slo.Threshold / meanSli)
				}
			}
			violationScore = totalViolationScore
		}

		var minPtr, maxPtr *float64
		if state.MinSet {
			v := state.Min
			minPtr = &v
			maxPtr = &state.Max
		}

		sloResults[i] = experimentv1alpha1.SLOResult{
			Name:           state.Name,
			ViolationScore: violationScore,
			MeanSli:        meanSli,
			MinSli:         minPtr,
			MaxSli:         maxPtr,
			Evaluations:    state.Count,
			Violations:     state.Violations,
		}

		if slo, ok := sloByName[state.Name]; ok {
			aggregateScore += violationScore * slo.Weight
		}
	}

	return experimentv1alpha1.RepetitionResult{
		Repetition:     repetition,
		SloResults:     sloResults,
		AggregateScore: aggregateScore,
	}
}

func (reconciler *ExperimentReconciler) applyManifestSet(ctx context.Context, namespace string, manifestRef *experimentv1alpha1.ManifestRef, experimentID, role string, owned *[]string) error {
	if manifestRef == nil {
		return nil
	}
	ownedName := fmt.Sprintf("ecoscape-%s-%s", experimentID, role)
	if err := reconciler.deployManifests(ctx, namespace, manifestRef.ConfigMapRef.Name, ownedName); err != nil {
		return fmt.Errorf("deploy %s manifests: %w", role, err)
	}
	*owned = append(*owned, ownedName)
	return nil
}

func (reconciler *ExperimentReconciler) cleanup(ctx context.Context, namespace string, ownedConfigMaps []string, experiment *experimentv1alpha1.Experiment) {
	for _, name := range ownedConfigMaps {
		reconciler.undeployManifests(ctx, namespace, name)
	}
}

// appendRepetitionToConfigMap reads (or creates) the results ConfigMap and
// appends the new RepetitionResult to the JSON array.
func (reconciler *ExperimentReconciler) appendRepetitionToConfigMap(
	ctx context.Context,
	namespace string,
	experiment *experimentv1alpha1.Experiment,
	result experimentv1alpha1.RepetitionResult,
) error {
	configMapName := fmt.Sprintf("ecoscape-%s-results", experiment.Status.ExperimentID)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: namespace},
	}

	var existing []experimentv1alpha1.RepetitionResult
	isNew := false

	err := reconciler.Get(ctx, client.ObjectKeyFromObject(configMap), configMap)
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("get results configmap: %w", err)
		}
		isNew = true
	} else {
		if raw, ok := configMap.Data["results.json"]; ok {
			var payload struct {
				RepetitionData []experimentv1alpha1.RepetitionResult `json:"repetitionData"`
			}
			if jsonErr := json.Unmarshal([]byte(raw), &payload); jsonErr == nil {
				existing = payload.RepetitionData
			}
		}
	}

	existing = append(existing, result)

	data := map[string]interface{}{
		"experimentId":          experiment.Status.ExperimentID,
		"experimentName":        experiment.GetName(),
		"configuredRepetitions": experiment.Spec.Duration.Repetitions,
		"completedRepetitions":  len(existing),
		"repetitionData":        existing,
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	if configMap.Data == nil {
		configMap.Data = map[string]string{}
	}
	configMap.Data["results.json"] = string(raw)

	if isNew {
		return reconciler.Create(ctx, configMap)
	}
	return reconciler.Update(ctx, configMap)
}

// appendMeasurementsToConfigMap writes the individual SLI values collected
// during one measurement window to the shared measurements ConfigMap for the
// experiment run. Each measurement is stored as a single JSON line:
//
//	{"repetition":1,"phase":"PreChaosMeasurement","slo":"response-time","index":0,"value":42.3}
//
// The ConfigMap is created on first write and appended to on subsequent calls.
func (reconciler *ExperimentReconciler) appendMeasurementsToConfigMap(
	ctx context.Context,
	namespace string,
	experiment *experimentv1alpha1.Experiment,
	phase experimentv1alpha1.ExperimentSubPhase,
) error {
	repetition := experiment.Status.RepetitionsCompleted + 1
	configMapName := fmt.Sprintf("ecoscape-%s-measurements", experiment.Status.ExperimentID)
	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: namespace},
	}

	isNew := false
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(configMap), configMap); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("get measurements configmap: %w", err)
		}
		isNew = true
	}

	var sb strings.Builder
	if !isNew {
		sb.WriteString(configMap.Data["measurements.jsonl"])
	}

	for _, state := range experiment.Status.MeasurementState {
		for i, value := range state.Values {
			line, err := json.Marshal(map[string]interface{}{
				"repetition": repetition,
				"phase":      string(phase),
				"slo":        state.Name,
				"index":      i,
				"value":      value,
			})
			if err != nil {
				return fmt.Errorf("marshal measurement: %w", err)
			}
			sb.Write(line)
			sb.WriteByte('\n')
		}
	}

	if configMap.Data == nil {
		configMap.Data = map[string]string{}
	}
	configMap.Data["measurements.jsonl"] = sb.String()

	if isNew {
		return reconciler.Create(ctx, configMap)
	}
	return reconciler.Update(ctx, configMap)
}

// prometheusClient wraps the Prometheus HTTP API.
type prometheusClient struct {
	serverURL  string
	httpClient *http.Client
}

func newPrometheusClient(url string, timeout int32) *prometheusClient {
	return &prometheusClient{
		serverURL: strings.TrimRight(url, "/"),
		httpClient: &http.Client{
			Timeout: time.Duration(timeout) * time.Second,
		},
	}
}

func (prometheus *prometheusClient) query(ctx context.Context, queryExpression string) (float64, bool, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", prometheus.serverURL+"/api/v1/query", nil)
	if err != nil {
		return 0, false, err
	}
	queryParameters := request.URL.Query()
	queryParameters.Set("query", queryExpression)
	request.URL.RawQuery = queryParameters.Encode()

	response, err := prometheus.httpClient.Do(request)
	if err != nil {
		return 0, false, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, false, err
	}

	var prometheusResponse struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &prometheusResponse); err != nil {
		return 0, false, fmt.Errorf("parse prometheus response: %w", err)
	}
	if prometheusResponse.Status != "success" {
		return 0, false, fmt.Errorf("prometheus query failed: %s", string(body))
	}
	if len(prometheusResponse.Data.Result) == 0 {
		return -1, false, nil
	}
	if len(prometheusResponse.Data.Result[0].Value) < 2 {
		return 0, false, fmt.Errorf("unexpected prometheus result format")
	}
	valueString, ok := prometheusResponse.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("prometheus value is not a string")
	}
	value, err := strconv.ParseFloat(valueString, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse prometheus value %q: %w", valueString, err)
	}
	return value, true, nil
}

func float64Ptr(value float64) *float64 {
	return &value
}

// waitForTopology and topology helpers are kept for activateTopology /
// deactivateTopology used in handleDeploying and handleCleanup.

func (reconciler *ExperimentReconciler) activateTopology(ctx context.Context, namespace, name string) error {
	topology := &experimentv1alpha1.Topology{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, topology); err != nil {
		return fmt.Errorf("get topology %s/%s: %w", namespace, name, err)
	}
	if topology.Spec.Active {
		return nil
	}
	patch := client.MergeFrom(topology.DeepCopy())
	topology.Spec.Active = true
	return reconciler.Patch(ctx, topology, patch)
}

func (reconciler *ExperimentReconciler) deactivateTopology(ctx context.Context, namespace, name string) error {
	topology := &experimentv1alpha1.Topology{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, topology); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get topology %s/%s: %w", namespace, name, err)
	}
	if !topology.Spec.Active {
		return nil
	}
	patch := client.MergeFrom(topology.DeepCopy())
	topology.Spec.Active = false
	return reconciler.Patch(ctx, topology, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (reconciler *ExperimentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Ignore updates that only touch the status subresource. The controller
	// drives its own reconcile cadence via RequeueAfter; status patches must
	// not generate additional reconcile events, otherwise multiple queue
	// entries accumulate for the same elapsed-second and produce duplicate
	// measurement samples.
	statusOnlyChanged := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
		},
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&experimentv1alpha1.Experiment{}, builder.WithPredicates(statusOnlyChanged)).
		Named("experiment").
		Complete(reconciler)
}
