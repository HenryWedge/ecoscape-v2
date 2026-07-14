package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newCompletionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion script",
		Long: fmt.Sprintf(`Generate shell completion script for kubectl-ecoscape.

To load completions:

  Bash:
    $ source <(%[1]s completion bash)
    # or persist it:
    $ %[1]s completion bash > /etc/bash_completion.d/%[1]s

  Zsh:
    $ source <(%[1]s completion zsh)
    # or persist it:
    $ %[1]s completion zsh > "${fpath[1]}/_%[1]s"

  Fish:
    $ %[1]s completion fish > ~/.config/fish/completions/%[1]s.fish

  PowerShell:
    $ %[1]s completion powershell > %[1]s.ps1
`, "kubectl-ecoscape"),
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"bash", "zsh", "fish", "powershell"},
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletion(os.Stdout)
			default:
				return fmt.Errorf("unsupported shell: %s", args[0])
			}
		},
	}

	return cmd
}
