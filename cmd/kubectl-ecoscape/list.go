package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/client"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

type listOptions struct {
	kubeconfig string
	namespace  string
	allSpaces  bool
}

func newListCommand() *cobra.Command {
	opts := &listOptions{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all experiments and their current status",
		Long: `Lists all Experiment custom resources with their phase, score,
and age.

Examples:
  kubectl ecoscape list
  kubectl ecoscape list --all-namespaces
  kubectl ecoscape list -n ecoscape
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runList(cmd, opts)
		},
	}

	cmd.Flags().StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "default", "Namespace")
	cmd.Flags().BoolVarP(&opts.allSpaces, "all-namespaces", "A", false, "List experiments across all namespaces")

	return cmd
}

func runList(cmd *cobra.Command, opts *listOptions) error {
	config, err := buildConfig(opts.kubeconfig)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	clients, err := newClients(config, opts.namespace)
	if err != nil {
		return fmt.Errorf("create k8s clients: %w", err)
	}

	list := &experimentv1alpha1.ExperimentList{}
	if err := clients.runtimeClient.List(cmd.Context(), list,
		client.InNamespace(opts.namespace)); err != nil {
		return fmt.Errorf("list experiments: %w", err)
	}

	if len(list.Items) == 0 {
		fmt.Println("No experiments found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	if opts.allSpaces || opts.namespace == "" {
		fmt.Fprintln(w, "NAMESPACE\tNAME\tID\tPHASE\tSCORE\tAGE")
	} else {
		fmt.Fprintln(w, "NAME\tID\tPHASE\tSCORE\tAGE")
	}

	for _, exp := range list.Items {
		score := "-"
		if exp.Status.AggregateScore != nil {
			score = fmt.Sprintf("%.4f", *exp.Status.AggregateScore)
		}

		age := time.Since(exp.CreationTimestamp.Time).Round(time.Second)
		id := exp.Status.ExperimentID
		if id == "" {
			id = "-"
		}

		if opts.allSpaces || opts.namespace == "" {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%v\n",
				exp.GetNamespace(), exp.GetName(), id,
				exp.Status.Phase, score, age)
		} else {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%v\n",
				exp.GetName(), id,
				exp.Status.Phase, score, age)
		}
	}
	return w.Flush()
}
