package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
)

func (reconciler *ExperimentReconciler) readConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	configMap := &corev1.ConfigMap{}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, configMap); err != nil {
		return nil, fmt.Errorf("read ConfigMap %s/%s: %w", namespace, name, err)
	}
	return configMap, nil
}

func (reconciler *ExperimentReconciler) ensureOwnedConfigMap(ctx context.Context, namespace, ownedName string, source *corev1.ConfigMap) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ownedName, Namespace: namespace}}
	err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ownedName}, configMap)
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("get ConfigMap %s/%s: %w", namespace, ownedName, err)
		}
		configMap = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ownedName, Namespace: namespace}}
	}
	configMap.Data = source.Data
	if configMap.CreationTimestamp.IsZero() {
		return reconciler.Create(ctx, configMap)
	}
	return reconciler.Update(ctx, configMap)
}

func (reconciler *ExperimentReconciler) deleteConfigMap(ctx context.Context, namespace, name string) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	if err := reconciler.Delete(ctx, configMap); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete ConfigMap %s/%s: %w", namespace, name, err)
	}
	return nil
}

func parseManifest(content string) (*unstructured.Unstructured, error) {
	object := &unstructured.Unstructured{}
	decoder := yaml.NewYAMLOrJSONDecoder(strings.NewReader(content), 4096)
	if err := decoder.Decode(object); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if object.GetName() == "" {
		return nil, fmt.Errorf("manifest has no metadata.name")
	}
	return object, nil
}

func (reconciler *ExperimentReconciler) applyManifests(ctx context.Context, namespace string, configMap *corev1.ConfigMap) error {
	for key, content := range configMap.Data {
		extension := filepath.Ext(key)
		if extension != ".yaml" && extension != ".yml" {
			continue
		}
		object, err := parseManifest(content)
		if err != nil {
			return fmt.Errorf("parse %s: %w", key, err)
		}
		if object.GetNamespace() == "" {
			object.SetNamespace(namespace)
		}

		err = reconciler.Create(ctx, object)
		if err != nil {
			if errors.IsAlreadyExists(err) {
				existing := &unstructured.Unstructured{}
				existing.SetAPIVersion(object.GetAPIVersion())
				existing.SetKind(object.GetKind())
				namespacedName := types.NamespacedName{Namespace: object.GetNamespace(), Name: object.GetName()}
				if getErr := reconciler.Get(ctx, namespacedName, existing); getErr != nil {
					return fmt.Errorf("get existing %s/%s: %w", object.GetNamespace(), object.GetName(), getErr)
				}
				object.SetResourceVersion(existing.GetResourceVersion())
				if updErr := reconciler.Update(ctx, object); updErr != nil {
					return fmt.Errorf("update %s/%s: %w", object.GetNamespace(), object.GetName(), updErr)
				}
			} else {
				return fmt.Errorf("create %s/%s: %w", object.GetNamespace(), object.GetName(), err)
			}
		}
	}
	return nil
}

func (reconciler *ExperimentReconciler) deleteManifests(ctx context.Context, configMap *corev1.ConfigMap) []error {
	var deleteErrors []error
	for key, content := range configMap.Data {
		extension := filepath.Ext(key)
		if extension != ".yaml" && extension != ".yml" {
			continue
		}
		object, err := parseManifest(content)
		if err != nil {
			continue
		}
		if err := reconciler.Delete(ctx, object); err != nil && !errors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			deleteErrors = append(deleteErrors, fmt.Errorf("delete %s/%s: %w", object.GetNamespace(), object.GetName(), err))
		}
	}
	return deleteErrors
}

func (reconciler *ExperimentReconciler) deployManifests(ctx context.Context, namespace, sourceName, ownedName string) error {
	source, err := reconciler.readConfigMap(ctx, namespace, sourceName)
	if err != nil {
		return err
	}
	if err := reconciler.ensureOwnedConfigMap(ctx, namespace, ownedName, source); err != nil {
		return err
	}
	return reconciler.applyManifests(ctx, namespace, source)
}

func (reconciler *ExperimentReconciler) undeployManifests(ctx context.Context, namespace, ownedName string) {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ownedName, Namespace: namespace}}
	if err := reconciler.Get(ctx, types.NamespacedName{Namespace: namespace, Name: ownedName}, configMap); err != nil {
		return
	}
	_ = reconciler.deleteManifests(ctx, configMap)
	_ = reconciler.deleteConfigMap(ctx, namespace, ownedName)
}
