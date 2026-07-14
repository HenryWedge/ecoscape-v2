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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

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

type sliCollector struct {
	name       string
	config     experimentv1alpha1.SLOConfig
	values     []float64
	violations int32
}

func newSLICollector(sloConfig experimentv1alpha1.SLOConfig) *sliCollector {
	return &sliCollector{name: sloConfig.Name, config: sloConfig}
}

func (collector *sliCollector) record(value float64, threshold float64, isBiggerBetter bool) {
	collector.values = append(collector.values, value)
	if isBiggerBetter {
		if value < threshold {
			collector.violations++
		}
	} else {
		if value > threshold {
			collector.violations++
		}
	}
}

func (collector *sliCollector) violationScore() float64 {
	if len(collector.values) == 0 {
		return 0
	}
	var total float64
	for _, value := range collector.values {
		if collector.config.IsBiggerBetter {
			if value < collector.config.Threshold {
				total += 1 - (value / collector.config.Threshold)
			}
		} else {
			if value > collector.config.Threshold {
				total += 1 - (collector.config.Threshold / value)
			}
		}
	}
	return total / float64(len(collector.values))
}

func (collector *sliCollector) mean() float64 {
	if len(collector.values) == 0 {
		return 0
	}
	var total float64
	for _, value := range collector.values {
		total += value
	}
	return total / float64(len(collector.values))
}

func (collector *sliCollector) minimum() float64 {
	if len(collector.values) == 0 {
		return 0
	}
	minimum := collector.values[0]
	for _, value := range collector.values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func (collector *sliCollector) maximum() float64 {
	if len(collector.values) == 0 {
		return 0
	}
	maximum := collector.values[0]
	for _, value := range collector.values[1:] {
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}

func (collector *sliCollector) toResult() experimentv1alpha1.SLOResult {
	return experimentv1alpha1.SLOResult{
		Name:           collector.name,
		ViolationScore: collector.violationScore(),
		MeanSli:        collector.mean(),
		MinSli:         float64Ptr(collector.minimum()),
		MaxSli:         float64Ptr(collector.maximum()),
		Evaluations:    int32(len(collector.values)),
		Violations:     collector.violations,
	}
}

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

// ExperimentReconciler reconciles a Experiment object.
type ExperimentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=experiment.cau-se.de,resources=experiments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=experiment.cau-se.de,resources=experiments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=experiment.cau-se.de,resources=experiments/finalizers,verbs=update
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
	logger.Info("reconciling experiment", "namespacedName", request.NamespacedName)

	experiment := &experimentv1alpha1.Experiment{}
	if err := reconciler.Get(ctx, request.NamespacedName, experiment); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if experiment.Status.Phase == experimentv1alpha1.PhaseSucceeded ||
		experiment.Status.Phase == experimentv1alpha1.PhaseFailed {
		return ctrl.Result{}, nil
	}

	if err := reconciler.runExperiment(ctx, experiment); err != nil {
		logger.Error(err, "experiment failed")
		reconciler.setPhase(ctx, experiment, experimentv1alpha1.PhaseFailed, err.Error())
		return ctrl.Result{}, err
	}

	reconciler.setPhase(ctx, experiment, experimentv1alpha1.PhaseSucceeded, "")
	logger.Info("experiment succeeded", "aggregateScore", experiment.Status.AggregateScore)
	return ctrl.Result{}, nil
}

func (reconciler *ExperimentReconciler) runExperiment(ctx context.Context, experiment *experimentv1alpha1.Experiment) error {
	logger := log.FromContext(ctx)
	namespace := experiment.GetNamespace()

	experimentID := experiment.Status.ExperimentID
	if experimentID == "" {
		experimentID = randomID(5)
	}

	now := metav1.Now()
	experiment.Status.ExperimentID = experimentID
	experiment.Status.Phase = experimentv1alpha1.PhaseRunning
	experiment.Status.StartTime = &now
	experiment.Status.CurrentRepetition = 1

	if err := reconciler.Status().Update(ctx, experiment); err != nil {
		return fmt.Errorf("status update: %w", err)
	}

	repetitions := derefOr(&experiment.Spec.Duration.Repetitions, 1)
	loadDelay := derefOr(experiment.Spec.Duration.LoadDelay, 0)
	chaosDelay := derefOr(experiment.Spec.Duration.ChaosDelay, 0)
	measurementDuration := derefOr(experiment.Spec.Duration.MeasurementDuration, 60)
	pauseBetweenRepetitions := derefOr(&experiment.Spec.Duration.PauseBetweenRepetitions, 60)

	var allRepetitionResults []experimentv1alpha1.RepetitionResult
	var latestSloResults []experimentv1alpha1.SLOResult

	for repetition := int32(1); repetition <= repetitions; repetition++ {
		logger.Info("starting repetition", "repetition", repetition, "total", repetitions)
		experiment.Status.CurrentRepetition = repetition

		var ownedConfigMaps []string
		collectors := makeCollectors(experiment.Spec.SLOs)
		prometheusClient := newPrometheusClient(experiment.Spec.Prometheus.URL,
			derefOr(experiment.Spec.Prometheus.QueryTimeoutSeconds, 10))

		if shouldDeploy := reconciler.isModeDeploySystem(experiment); shouldDeploy {
			reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Sut, experimentID, "sut", &ownedConfigMaps)
			reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Infra, experimentID, "infra", &ownedConfigMaps)
			reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Monitor, experimentID, "monitor", &ownedConfigMaps)
		}
		if reconciler.isModeStartLoad(experiment) {
			reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Load, experimentID, "load", &ownedConfigMaps)
		}
		experiment.Status.ManifestConfigMaps = ownedConfigMaps
		_ = reconciler.Status().Update(ctx, experiment)

		logger.Info("waiting load delay", "seconds", loadDelay)
		if err := sleepWithContext(ctx, time.Duration(loadDelay)*time.Second); err != nil {
			reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
			return err
		}

		logger.Info("pre-chaos evaluation", "seconds", chaosDelay)
		for second := int32(0); second < chaosDelay; second++ {
			if err := sleepWithContext(ctx, time.Second); err != nil {
				reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
				return err
			}
			evaluateAll(ctx, prometheusClient, collectors)
		}

		if reconciler.isModeApplyChaos(experiment) {
			reconciler.applyManifestSet(ctx, namespace, experiment.Spec.Manifests.Chaos, experimentID, "chaos", &ownedConfigMaps)
			experiment.Status.ManifestConfigMaps = ownedConfigMaps
			_ = reconciler.Status().Update(ctx, experiment)
		}

		logger.Info("chaos evaluation", "seconds", measurementDuration)
		for second := int32(0); second < measurementDuration; second++ {
			if err := sleepWithContext(ctx, time.Second); err != nil {
				reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)
				return err
			}
			evaluateAll(ctx, prometheusClient, collectors)
		}

		repetitionResult := makeRepetitionResult(repetition, collectors)
		allRepetitionResults = append(allRepetitionResults, repetitionResult)
		latestSloResults = repetitionResult.SloResults

		reconciler.cleanup(ctx, namespace, ownedConfigMaps, experiment)

		experiment.Status.RepetitionsCompleted = repetition
		experiment.Status.SloResults = latestSloResults
		experiment.Status.RepetitionResults = allRepetitionResults
		experiment.Status.AggregateScore = float64Ptr(aggregateScore(experiment.Spec.SLOs, latestSloResults))
		_ = reconciler.Status().Update(ctx, experiment)

		if repetition < repetitions {
			logger.Info("pausing before next repetition", "seconds", pauseBetweenRepetitions)
			if err := sleepWithContext(ctx, time.Duration(pauseBetweenRepetitions)*time.Second); err != nil {
				return err
			}
		}
	}

	if err := reconciler.writeResultsConfigMap(ctx, namespace, experimentID, experiment, allRepetitionResults); err != nil {
		return fmt.Errorf("write results configmap: %w", err)
	}

	completed := metav1.Now()
	experiment.Status.CompletionTime = &completed
	return nil
}

func (reconciler *ExperimentReconciler) applyManifestSet(ctx context.Context, namespace string, manifestRef *experimentv1alpha1.ManifestRef, experimentID, role string, owned *[]string) {
	if manifestRef == nil {
		return
	}
	ownedName := fmt.Sprintf("ecoscape-%s-%s", experimentID, role)
	*owned = append(*owned, ownedName)
	if err := reconciler.deployManifests(ctx, namespace, manifestRef.ConfigMapRef.Name, ownedName); err != nil {
		log.FromContext(ctx).Error(err, "failed to deploy manifests", "role", role)
	}
}

func (reconciler *ExperimentReconciler) cleanup(ctx context.Context, namespace string, ownedConfigMaps []string, experiment *experimentv1alpha1.Experiment) {
	for _, name := range ownedConfigMaps {
		reconciler.undeployManifests(ctx, namespace, name)
	}
}

func (reconciler *ExperimentReconciler) setPhase(ctx context.Context, experiment *experimentv1alpha1.Experiment, phase experimentv1alpha1.ExperimentPhase, message string) {
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
	_ = reconciler.Status().Update(ctx, experiment)
}

func (reconciler *ExperimentReconciler) writeResultsConfigMap(ctx context.Context, namespace, experimentID string, experiment *experimentv1alpha1.Experiment, repetitionResults []experimentv1alpha1.RepetitionResult) error {
	data := map[string]interface{}{
		"experimentId":          experimentID,
		"experimentName":        experiment.GetName(),
		"executionMode":         experiment.Spec.ExecutionMode,
		"configuredRepetitions": experiment.Spec.Duration.Repetitions,
		"aggregateScore":        experiment.Status.AggregateScore,
		"completedRepetitions":  len(repetitionResults),
		"repetitionData":        repetitionResults,
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	configMapName := fmt.Sprintf("ecoscape-%s-results", experimentID)
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: namespace}}
	if err := reconciler.Get(ctx, client.ObjectKeyFromObject(configMap), configMap); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		configMap.Data = map[string]string{"results.json": string(raw)}
		return reconciler.Create(ctx, configMap)
	}
	configMap.Data = map[string]string{"results.json": string(raw)}
	return reconciler.Update(ctx, configMap)
}

func (reconciler *ExperimentReconciler) isModeDeploySystem(experiment *experimentv1alpha1.Experiment) bool {
	switch experiment.Spec.ExecutionMode {
	case experimentv1alpha1.ExecutionModeFull, experimentv1alpha1.ExecutionModeSystemDeploy:
		return true
	default:
		return false
	}
}

func (reconciler *ExperimentReconciler) isModeStartLoad(experiment *experimentv1alpha1.Experiment) bool {
	switch experiment.Spec.ExecutionMode {
	case experimentv1alpha1.ExecutionModeFull, experimentv1alpha1.ExecutionModeExperiment, experimentv1alpha1.ExecutionModeLoadTest:
		return true
	default:
		return false
	}
}

func (reconciler *ExperimentReconciler) isModeApplyChaos(experiment *experimentv1alpha1.Experiment) bool {
	switch experiment.Spec.ExecutionMode {
	case experimentv1alpha1.ExecutionModeFull, experimentv1alpha1.ExecutionModeExperiment:
		return true
	default:
		return false
	}
}

func makeCollectors(slos []experimentv1alpha1.SLOConfig) []*sliCollector {
	collectors := make([]*sliCollector, len(slos))
	for index, sloConfig := range slos {
		collectors[index] = newSLICollector(sloConfig)
	}
	return collectors
}

func evaluateAll(ctx context.Context, prometheusClient *prometheusClient, collectors []*sliCollector) {
	for _, collector := range collectors {
		value, found, err := prometheusClient.query(ctx, collector.config.Query)
		if err != nil || !found {
			continue
		}
		collector.record(value, collector.config.Threshold, collector.config.IsBiggerBetter)
	}
}

func makeRepetitionResult(repetition int32, collectors []*sliCollector) experimentv1alpha1.RepetitionResult {
	repetitionResult := experimentv1alpha1.RepetitionResult{
		Repetition: repetition,
		SloResults: make([]experimentv1alpha1.SLOResult, len(collectors)),
	}
	var weighted float64
	for index, collector := range collectors {
		sloResult := collector.toResult()
		repetitionResult.SloResults[index] = sloResult
		weighted += sloResult.ViolationScore * collector.config.Weight
	}
	repetitionResult.AggregateScore = weighted
	return repetitionResult
}

func aggregateScore(slos []experimentv1alpha1.SLOConfig, results []experimentv1alpha1.SLOResult) float64 {
	lookup := make(map[string]experimentv1alpha1.SLOConfig, len(slos))
	for _, sloConfig := range slos {
		lookup[sloConfig.Name] = sloConfig
	}
	var score float64
	for _, result := range results {
		if sloConfig, found := lookup[result.Name]; found {
			score += result.ViolationScore * sloConfig.Weight
		}
	}
	return score
}

func float64Ptr(value float64) *float64 {
	return &value
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	select {
	case <-time.After(duration):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetupWithManager sets up the controller with the Manager.
func (reconciler *ExperimentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&experimentv1alpha1.Experiment{}).
		Named("experiment").
		Complete(reconciler)
}
