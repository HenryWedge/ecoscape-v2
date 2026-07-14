package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

type runOptions struct {
	filename   string
	namespace  string
	kubeconfig string
	wait       bool
	timeout    time.Duration
}

func newRunCommand() *cobra.Command {
	opts := &runOptions{}
	cmd := &cobra.Command{
		Use:   "run -f experiment.yaml",
		Short: "Create and optionally wait for an experiment to complete",
		Long: `Creates an Experiment custom resource and optionally waits for it
to finish, showing progress along the way.

Examples:
  kubectl ecoscape run -f experiment.yaml
  kubectl ecoscape run -f experiment.yaml --wait --timeout 10m
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if opts.filename == "" && len(args) > 0 {
				opts.filename = args[0]
			}
			if opts.filename == "" {
				return fmt.Errorf("specify experiment file with -f <file>")
			}
			return runRun(cmd, opts)
		},
	}

	cmd.Flags().StringVarP(&opts.filename, "filename", "f", "", "Experiment YAML file")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "default", "Target namespace")
	cmd.Flags().StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	cmd.Flags().BoolVarP(&opts.wait, "wait", "w", false, "Wait for experiment to complete")
	cmd.Flags().DurationVar(&opts.timeout, "timeout", 30*time.Minute, "Maximum time to wait")

	return cmd
}

func runRun(cmd *cobra.Command, opts *runOptions) error {
	raw, err := os.ReadFile(opts.filename)
	if err != nil {
		return fmt.Errorf("read %s: %w", opts.filename, err)
	}

	experiment := &experimentv1alpha1.Experiment{}
	if err := yaml.Unmarshal(raw, experiment); err != nil {
		return fmt.Errorf("parse %s: %w", opts.filename, err)
	}

	if opts.namespace != "default" {
		experiment.SetNamespace(opts.namespace)
	}
	ns := experiment.GetNamespace()
	name := experiment.GetName()

	_, clients, err := getConfigAndClients(opts.kubeconfig, opts.namespace)
	if err != nil {
		return err
	}

	existing := &experimentv1alpha1.Experiment{}
	if err := clients.runtimeClient.Get(cmd.Context(),
		types.NamespacedName{Namespace: ns, Name: name}, existing); err == nil {
		return fmt.Errorf("experiment %s/%s already exists (phase: %s)", ns, name, existing.Status.Phase)
	}

	if err := clients.runtimeClient.Create(cmd.Context(), experiment); err != nil {
		return fmt.Errorf("create experiment: %w", err)
	}
	fmt.Printf("Created experiment %s/%s (id: %s)\n", ns, name, experiment.Status.ExperimentID)

	if !opts.wait {
		return nil
	}

	fmt.Println("Waiting for experiment to complete...")
	ctx, cancel := context.WithTimeout(cmd.Context(), opts.timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastPhase experimentv1alpha1.ExperimentPhase
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout after %v", opts.timeout)
		case <-ticker.C:
			current := &experimentv1alpha1.Experiment{}
			if err := clients.runtimeClient.Get(ctx,
				types.NamespacedName{Namespace: ns, Name: name}, current); err != nil {
				return fmt.Errorf("get experiment: %w", err)
			}

			if current.Status.Phase != lastPhase {
				elapsed := time.Since(startTime).Round(time.Second)
				fmt.Printf("  %s → %s (after %v)\n", lastPhase, current.Status.Phase, elapsed)
				lastPhase = current.Status.Phase
			}

			if current.Status.Phase == experimentv1alpha1.PhaseSucceeded {
				score := "N/A"
				if current.Status.AggregateScore != nil {
					score = fmt.Sprintf("%.4f", *current.Status.AggregateScore)
				}
				fmt.Printf("\n✅ Experiment %s/%s succeeded!\n", ns, name)
				fmt.Printf("   Aggregate Score: %s\n", score)
				fmt.Printf("   Duration: %v\n", time.Since(startTime).Round(time.Second))
				fmt.Printf("   Results ConfigMap: ecoscape-%s-results\n", current.Status.ExperimentID)
				return nil
			}

			if current.Status.Phase == experimentv1alpha1.PhaseFailed {
				message := ""
				if len(current.Status.Conditions) > 0 {
					message = current.Status.Conditions[len(current.Status.Conditions)-1].Message
				}
				fmt.Printf("\n❌ Experiment %s/%s failed!\n", ns, name)
				if message != "" {
					fmt.Printf("   Reason: %s\n", message)
				}
				return fmt.Errorf("experiment failed: %s", message)
			}
		}
	}
}
