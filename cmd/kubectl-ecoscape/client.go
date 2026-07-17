package main

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

type k8sClients struct {
	runtimeClient client.Client
	typedClient   *kubernetes.Clientset
	namespace     string
}

func newClients(config *rest.Config, namespace string) (*k8sClients, error) {
	scheme, err := experimentv1alpha1.SchemeBuilder.Build()
	if err != nil {
		return nil, err
	}

	runtimeClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, err
	}

	typedClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &k8sClients{
		runtimeClient: runtimeClient,
		typedClient:   typedClient,
		namespace:     namespace,
	}, nil
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	)
	return clientConfig.ClientConfig()
}

func getConfigAndClients(kubeconfig, namespace string) (*rest.Config, *k8sClients, error) {
	config, err := buildConfig(kubeconfig)
	if err != nil {
		return nil, nil, fmt.Errorf("build kubeconfig: %w", err)
	}
	clients, err := newClients(config, namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("create k8s clients: %w", err)
	}
	return config, clients, nil
}
