// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/state"
)

// StateCommand exposes k4a's local-state subsystems for inspection and
// reset. See docs/design/state-management.md for the model.
type StateCommand struct{}

// Help returns the help text.
func (c *StateCommand) Help() string {
	return `Usage: k4a state <subcommand> [options]

  Inspect and manage k4a's local state directory (~/.k4a). State above
  the user-owned config tier (session state, caches, indexes) is always
  safe to clear — k4a rebuilds whatever it needs from the cluster on
  next use.

Subcommands:
  status              Show size, age, and contents of each state subsystem
  clear [subsystem]   Remove state for one subsystem, or all non-config state
                      Subsystems: session, cache, index, all (default)
  dir                 Print the state directory path

Examples:
  k4a state status
  k4a state clear cache
  k4a state clear all
  k4a state dir`
}

// Synopsis returns the one-line description.
func (c *StateCommand) Synopsis() string {
	return "Inspect and manage local state (~/.k4a)"
}

// Run dispatches on the first arg to the subcommand handler.
func (c *StateCommand) Run(args []string) int {
	sub := "status"
	if len(args) > 0 {
		sub = strings.ToLower(args[0])
	}

	root := state.NewRoot(config.DefaultConfigDir())

	switch sub {
	case "status":
		return runStateStatus(root)
	case "clear":
		target := "all"
		if len(args) > 1 {
			target = strings.ToLower(args[1])
		}
		return runStateClear(root, target)
	case "dir":
		fmt.Println(root.Path())
		return 0
	case "help", "-h", "--help":
		fmt.Println(c.Help())
		return 0
	default:
		log.Error("unknown state subcommand", "subcommand", sub)
		fmt.Println(c.Help())
		return 1
	}
}

// runStateStatus prints a table of subsystems with size, age, and the
// state directory path.
//
//nolint:errcheck // writing to stdout via tabwriter; errors aren't actionable
func runStateStatus(root *state.Root) int {
	fmt.Printf("State directory: %s\n\n", root.Path())

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SUBSYSTEM\tTIER\tSIZE\tUPDATED")

	rows := []struct {
		name string
		tier string
		sub  state.Subsystem
	}{
		{"session", "1 (session)", state.SubsystemSession},
		{"cache", "2 (cache)", state.SubsystemCache},
		{"index", "3 (index)", state.SubsystemIndex},
	}

	for _, r := range rows {
		size, mod, ok := state.Stat(root, r.sub)
		if !ok {
			fmt.Fprintf(w, "%s\t%s\t-\t-\n", r.name, r.tier)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", r.name, r.tier, formatBytes(size), formatAge(mod))
	}
	w.Flush()

	printIndexInventory(root)
	return 0
}

// printIndexInventory shows one row per indexed (cluster, topic) so users
// can see exactly which topics have local indexes and how much each one
// covers. Suppressed when no indexes exist — output stays terse for the
// common case.
//
//nolint:errcheck // tabwriter to stdout; errors not actionable
func printIndexInventory(root *state.Root) {
	inv, err := index.List(root)
	if err != nil || len(inv) == 0 {
		return
	}

	fmt.Println("\nIndexed topics:")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tTOPIC\tPARTITIONS\tSIZE\tOLDEST\tNEWEST")
	for _, t := range inv {
		fmt.Fprintf(
			w, "%s\t%s\t%d\t%s\t%s\t%s\n",
			t.Cluster,
			t.Topic,
			t.Partitions,
			formatBytes(t.Bytes),
			formatTimeOrDash(t.OldestAt),
			formatTimeOrDash(t.NewestAt),
		)
	}
	w.Flush()
}

func formatTimeOrDash(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return formatAge(t)
}

// runStateClear removes one or all non-config subsystems.
func runStateClear(root *state.Root, target string) int {
	switch target {
	case "all", "":
		if err := state.ClearAll(root); err != nil {
			log.Error("clear all failed", "err", err)
			return 1
		}
		fmt.Println("Cleared all non-config state.")
		return 0
	case "session":
		return clearOne(root, state.SubsystemSession, "session")
	case "cache":
		return clearOne(root, state.SubsystemCache, "cache")
	case "index":
		return clearOne(root, state.SubsystemIndex, "index")
	default:
		log.Error("unknown subsystem", "subsystem", target,
			"valid", "session, cache, index, all")
		return 1
	}
}

func clearOne(root *state.Root, sub state.Subsystem, label string) int {
	size, _, exists := state.Stat(root, sub)
	if err := state.Clear(root, sub); err != nil {
		log.Error("clear failed", "subsystem", label, "err", err)
		return 1
	}
	if exists {
		fmt.Printf("Cleared %s (%s).\n", label, formatBytes(size))
	} else {
		fmt.Printf("%s: nothing to clear.\n", label)
	}
	return 0
}

// formatBytes renders a byte count in human-friendly units.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(n)/float64(div), "KMGTPE"[exp])
}

// formatAge renders a "5m ago"-style relative time.
func formatAge(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
