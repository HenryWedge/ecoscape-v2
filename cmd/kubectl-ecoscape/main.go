package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	root := &cobra.Command{
		Use:   "kubectl-ecoscape",
		Short: "Ecoscape - reliability experiment runner plugin for kubectl",
		Long: `A kubectl plugin for managing Ecoscape experiments.

  Create, run and analyze reliability experiments on Kubernetes.
  Documentation: https://github.com/cau-se/ecoscape`,
		SilenceUsage: true,
	}

	root.AddCommand(
		newInitCommand(),
		newBundleCommand(),
		newRunCommand(),
		newResultsCommand(),
		newListCommand(),
		newCompletionCommand(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
