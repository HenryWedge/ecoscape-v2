package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type bundleOptions struct {
	name       string
	namespace  string
	kubeconfig string
}

func newBundleCommand() *cobra.Command {
	opts := &bundleOptions{}
	cmd := &cobra.Command{
		Use:   "bundle <directory>",
		Short: "Create a ConfigMap from a directory of manifest files",
		Long: `Reads all .yaml/.yml files from the given directory and creates
a Kubernetes ConfigMap containing them as data entries.

If --name is not provided, a name is generated from the directory name
plus a short random ID (e.g. sut-a3f2c1).

The generated ConfigMap name is printed to stdout so it can be captured
in scripts or referenced in an Experiment CR.

Examples:
  kubectl ecoscape bundle ./sut/ -n ecoscape
  kubectl ecoscape bundle ./load/ --name my-load -n ecoscape
  CM=$(kubectl ecoscape bundle ./sut/ -n ecoscape)
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runBundle(cmd.Context(), args[0], opts)
		},
	}

	cmd.Flags().StringVar(&opts.name, "name", "", "ConfigMap name (default: <dirname>-<id>)")
	cmd.Flags().StringVarP(&opts.namespace, "namespace", "n", "default", "Target namespace")
	cmd.Flags().StringVar(&opts.kubeconfig, "kubeconfig", "", "Path to kubeconfig")

	return cmd
}

func runBundle(ctx context.Context, dir string, opts *bundleOptions) error {
	// Resolve directory
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("resolve path %s: %w", dir, err)
	}
	info, err := os.Stat(absDir)
	if err != nil {
		return fmt.Errorf("stat %s: %w", absDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", absDir)
	}

	// Collect .yaml/.yml files (non-recursive)
	entries, err := os.ReadDir(absDir)
	if err != nil {
		return fmt.Errorf("read directory %s: %w", absDir, err)
	}
	data := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(absDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		data[entry.Name()] = string(content)
	}
	if len(data) == 0 {
		return fmt.Errorf("no .yaml/.yml files found in %s", absDir)
	}

	// Determine ConfigMap name
	name := opts.name
	if name == "" {
		dirName := filepath.Base(absDir)
		id, err := shortID()
		if err != nil {
			return fmt.Errorf("generate id: %w", err)
		}
		name = dirName + "-" + id
	}

	// Build ConfigMap
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: opts.namespace,
		},
		Data: data,
	}

	// Apply to cluster
	config, err := buildConfig(opts.kubeconfig)
	if err != nil {
		return err
	}
	clients, err := newClients(config, opts.namespace)
	if err != nil {
		return err
	}

	if _, err := clients.typedClient.CoreV1().ConfigMaps(opts.namespace).
		Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create ConfigMap: %w", err)
	}

	fmt.Printf("Created ConfigMap %s/%s\n", opts.namespace, name)
	return nil
}

// shortID returns a 6-character lowercase hex string from crypto/rand.
func shortID() (string, error) {
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
