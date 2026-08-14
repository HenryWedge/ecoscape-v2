package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	experimentv1alpha1 "github.com/cau-se/ecoscape/api/v1alpha1"
)

type resultsOptions struct {
	experimentID string
	kubeconfig   string
	namespace    string
	format       string
	outputDir    string
}

func newResultsCommand() *cobra.Command {
	opts := &resultsOptions{}
	cmd := &cobra.Command{
		Use:   "results <experiment3-id>",
		Short: "Show experiment3 results from the results ConfigMap",
		Long: `Reads the results ConfigMap for a completed experiment3 and displays
them as a table or exports them as CSV files.

Examples:
  kubectl ecoscape results xqkzm
  kubectl ecoscape results xqkzm --csv --output-dir ./results
  kubectl ecoscape results xqkzm --json
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.experimentID = args[0]
			return runResults(cmd, opts)
		},
	}

	cmd.Flags().StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "default", "Namespace of the experiment3")
	cmd.Flags().StringVarP(&opts.format, "format", "f", "table", "Output format: table, csv, json")
	cmd.Flags().StringVarP(&opts.outputDir, "output-dir", "o", ".", "Directory for CSV export")

	return cmd
}

type resultsData struct {
	ExperimentID   string                                `json:"experimentId"`
	ExperimentName string                                `json:"experimentName"`
	ExecutionMode  string                                `json:"executionMode"`
	AggregateScore *float64                              `json:"aggregateScore"`
	Repetitions    []experimentv1alpha1.RepetitionResult `json:"repetitionData"`
}

func runResults(cmd *cobra.Command, opts *resultsOptions) error {
	config, err := buildConfig(opts.kubeconfig)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	clients, err := newClients(config, opts.namespace)
	if err != nil {
		return fmt.Errorf("create k8s clients: %w", err)
	}

	configMapName := fmt.Sprintf("ecoscape-%s-results", opts.experimentID)
	configMap := &corev1.ConfigMap{}
	if err := clients.runtimeClient.Get(cmd.Context(),
		types.NamespacedName{Namespace: opts.namespace, Name: configMapName}, configMap); err != nil {
		return fmt.Errorf("read ConfigMap %s/%s: %w", opts.namespace, configMapName, err)
	}

	rawJSON, ok := configMap.Data["results.json"]
	if !ok {
		return fmt.Errorf("ConfigMap %s/%s has no 'results.json' key", opts.namespace, configMapName)
	}

	var data resultsData
	if err := json.Unmarshal([]byte(rawJSON), &data); err != nil {
		return fmt.Errorf("parse results: %w", err)
	}

	switch opts.format {
	case "table":
		return printResultsTable(&data)
	case "json":
		fmt.Println(rawJSON)
		return nil
	case "csv":
		return exportResultsCSV(&data, opts.outputDir)
	default:
		return fmt.Errorf("unknown format: %s (use: table, csv, json)", opts.format)
	}
}

func printResultsTable(data *resultsData) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	score := "N/A"
	if data.AggregateScore != nil {
		score = fmt.Sprintf("%.4f", *data.AggregateScore)
	}

	fmt.Fprintf(w, "Experiment:\t%s (id: %s)\n", data.ExperimentName, data.ExperimentID)
	fmt.Fprintf(w, "Mode:\t%s\n", data.ExecutionMode)
	fmt.Fprintf(w, "Aggregate Score:\t%s\n", score)
	fmt.Fprintf(w, "Repetitions:\t%d\n\n", len(data.Repetitions))

	for _, rep := range data.Repetitions {
		fmt.Fprintf(w, "--- Repetition %d ---\n", rep.Repetition)
		fmt.Fprintf(w, "  Aggregate Score:\t%.4f\n\n", rep.AggregateScore)
		fmt.Fprintln(w, "  SLO\tScore\tMean\tMin\tMax\tEvaluations\tViolations")

		for _, slo := range rep.SloResults {
			minStr, maxStr := "-", "-"
			if slo.MinSli != nil {
				minStr = fmt.Sprintf("%.2f", *slo.MinSli)
			}
			if slo.MaxSli != nil {
				maxStr = fmt.Sprintf("%.2f", *slo.MaxSli)
			}
			fmt.Fprintf(w, "  %s\t%.4f\t%.2f\t%s\t%s\t%d\t%d\n",
				slo.Name, slo.ViolationScore, slo.MeanSli,
				minStr, maxStr, slo.Evaluations, slo.Violations)
		}
		fmt.Fprintln(w)
	}
	return w.Flush()
}

func exportResultsCSV(data *resultsData, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	for _, rep := range data.Repetitions {
		repDir := filepath.Join(outputDir, fmt.Sprintf("repetition-%d", rep.Repetition))
		if err := os.MkdirAll(repDir, 0755); err != nil {
			return err
		}

		for _, slo := range rep.SloResults {
			filePath := filepath.Join(repDir, fmt.Sprintf("%s.csv", slo.Name))
			file, err := os.Create(filePath)
			if err != nil {
				return fmt.Errorf("create %s: %w", filePath, err)
			}

			writer := csv.NewWriter(file)
			writer.Write([]string{"name", slo.Name})
			writer.Write([]string{"violationScore", fmt.Sprintf("%.6f", slo.ViolationScore)})
			writer.Write([]string{"meanSli", fmt.Sprintf("%.6f", slo.MeanSli)})
			if slo.MinSli != nil {
				writer.Write([]string{"minSli", fmt.Sprintf("%.6f", *slo.MinSli)})
			}
			if slo.MaxSli != nil {
				writer.Write([]string{"maxSli", fmt.Sprintf("%.6f", *slo.MaxSli)})
			}
			writer.Write([]string{"evaluations", fmt.Sprintf("%d", slo.Evaluations)})
			writer.Write([]string{"violations", fmt.Sprintf("%d", slo.Violations)})
			writer.Flush()
			file.Close()
			fmt.Printf("  Created %s\n", filePath)
		}
	}

	summaryPath := filepath.Join(outputDir, "summary.csv")
	summaryFile, err := os.Create(summaryPath)
	if err != nil {
		return err
	}
	defer summaryFile.Close()

	writer := csv.NewWriter(summaryFile)
	writer.Write([]string{"experimentId", data.ExperimentID})
	writer.Write([]string{"experimentName", data.ExperimentName})
	writer.Write([]string{"executionMode", data.ExecutionMode})
	if data.AggregateScore != nil {
		writer.Write([]string{"aggregateScore", fmt.Sprintf("%.6f", *data.AggregateScore)})
	}
	writer.Write([]string{"repetitions", fmt.Sprintf("%d", len(data.Repetitions))})
	writer.Flush()

	fmt.Printf("  Created %s\n", summaryPath)
	return nil
}
