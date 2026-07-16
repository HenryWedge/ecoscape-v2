package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"
)

type initOptions struct {
	name       string
	sourceDir  string
	namespace  string
	kubeconfig string
	outputFile string
	dryRun     bool
}

func newInitCommand() *cobra.Command {
	opts := &initOptions{}
	cmd := &cobra.Command{
		Use:   "init <experiment-name>",
		Short: "Bootstrap ConfigMaps and Experiment CR from a manifest directory",
		Long: `Scans a directory for Kubernetes manifest subdirectories and creates
ConfigMaps and an Experiment custom resource.

Expected directory structure:
  --from-dir/
    sut/       System Under Test manifests
    load/      Load generator manifests
    infra/     Infrastructure constraint manifests
    chaos/     Chaos experiment manifests
    monitor/   Monitoring manifests (ServiceMonitor, PodMonitor)

Example:
  kubectl ecoscape init my-benchmark --from-dir ./build/ --namespace ecoscape
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.name = args[0]
			return runInit(cmd, opts)
		},
	}

	cmd.Flags().StringVar(&opts.sourceDir, "from-dir", ".", "Directory containing manifest subdirectories")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "default", "Target namespace")
	cmd.Flags().StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	cmd.Flags().StringVarP(&opts.outputFile, "output", "o", "", "Write Experiment CR YAML to file instead of creating it")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print actions without applying them")

	return cmd
}

type manifestRole struct {
	dirName string
	roleKey string
}

var manifestRoles = []manifestRole{
	{dirName: "sut", roleKey: "sut"},
	{dirName: "load", roleKey: "load"},
	{dirName: "infra", roleKey: "infra"},
	{dirName: "chaos", roleKey: "chaos"},
	{dirName: "monitor", roleKey: "monitor"},
}

type foundRole struct {
	role  manifestRole
	files []string
}

func runInit(cmd *cobra.Command, opts *initOptions) error {
	fmt.Printf("Scanning %s for manifest directories...\n", opts.sourceDir)

	var found []foundRole
	for _, role := range manifestRoles {
		dirPath := filepath.Join(opts.sourceDir, role.dirName)
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read %s: %w", dirPath, err)
		}
		var files []string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			ext := filepath.Ext(entry.Name())
			if ext == ".yaml" || ext == ".yml" {
				files = append(files, filepath.Join(dirPath, entry.Name()))
			}
		}
		if len(files) > 0 {
			found = append(found, foundRole{role: role, files: files})
			fmt.Printf("  ✓ %s/  (%d manifest(s))\n", role.dirName, len(files))
		}
	}

	if len(found) == 0 {
		return fmt.Errorf("no manifest directories found in %s (expected: sut/, load/, infra/, chaos/, monitor/)", opts.sourceDir)
	}

	configMapNames := make(map[string]string)
	for _, fr := range found {
		cmName := opts.name + "-" + fr.role.roleKey
		configMapNames[fr.role.roleKey] = cmName

		if opts.dryRun || opts.outputFile != "" {
			fmt.Printf("  Would create ConfigMap %s from %s/\n", cmName, fr.role.dirName)
			continue
		}

		config, err := buildConfig(opts.kubeconfig)
		if err != nil {
			return err
		}
		typedClient, err := kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}

		cmData := make(map[string]string)
		for _, path := range fr.files {
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
			cmData[filepath.Base(path)] = string(content)
		}

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: opts.namespace,
			},
			Data: cmData,
		}
		if _, err := typedClient.CoreV1().ConfigMaps(opts.namespace).
			Create(cmd.Context(), cm, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create ConfigMap %s: %w", cmName, err)
		}
		fmt.Printf("  Created ConfigMap %s/%s\n", opts.namespace, cmName)
	}

	experimentYAML, err := generateExperimentYAML(opts, configMapNames)
	if err != nil {
		return err
	}

	if opts.outputFile != "" {
		return os.WriteFile(opts.outputFile, experimentYAML, 0644)
	}

	if opts.dryRun {
		fmt.Println("\nExperiment CR (dry-run):")
		fmt.Println(string(experimentYAML))
		return nil
	}

	_, clients, err := getConfigAndClients(opts.kubeconfig, opts.namespace)
	if err != nil {
		return err
	}

	experiment := &unstructured.Unstructured{}
	experiment.SetAPIVersion("ecoscape.cau-se.de/v1alpha1")
	experiment.SetKind("Experiment")
	experiment.SetName(opts.name)
	experiment.SetNamespace(opts.namespace)
	if err := yaml.Unmarshal(experimentYAML, experiment.Object); err != nil {
		return fmt.Errorf("parse experiment yaml: %w", err)
	}

	if err := clients.runtimeClient.Create(cmd.Context(), experiment); err != nil {
		return fmt.Errorf("create Experiment CR: %w", err)
	}
	fmt.Printf("Created Experiment %s/%s\n", opts.namespace, opts.name)
	return nil
}

func generateExperimentYAML(opts *initOptions, configMapNames map[string]string) ([]byte, error) {
	manifestEntries := ""
	for _, role := range manifestRoles {
		cmName, ok := configMapNames[role.roleKey]
		if !ok {
			continue
		}
		manifestEntries += fmt.Sprintf("    %s:\n      configMapRef:\n        name: %s\n", role.roleKey, cmName)
	}

	yamlContent := fmt.Sprintf(`apiVersion: ecoscape.cau-se.de/v1alpha1
kind: Experiment
metadata:
  name: %s
  namespace: %s
spec:
  executionMode: FullExperimentRun
  duration:
    loadDelay: 20
    chaosDelay: 20
    measurementDuration: 60
    repetitions: 1
    pauseBetweenRepetitions: 60
  manifests:
%s
  prometheus:
    url: http://prometheus-k8s.monitoring:9090
  slos:
    - name: example-slo
      query: "vector(1)"
      threshold: 1.0
      thresholdDirection: LessThanOrEqual
      isBiggerBetter: true
      weight: 1.0
`, opts.name, opts.namespace, manifestEntries)

	return []byte(yamlContent), nil
}
