// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/state"
)

// IndexCommand inspects and clears the local Tier 3 index
// (docs/design/local-index.md, Tier 4 — Retention & disk management).
type IndexCommand struct {
	// Root and Out default to ~/.k4a and stdout; tests point them elsewhere.
	Root *state.Root
	Out  io.Writer
}

const (
	subcmdStatus = "status"
	subcmdHelp   = "help"
	flagHelp     = "--help"
)

type indexClearFlags struct {
	Cluster string `long:"cluster" description:"Only clear indexes for this cluster ID"`
	All     bool   `long:"all"     description:"Clear every local topic index"`
}

// Help returns the help text.
func (c *IndexCommand) Help() string {
	return `Usage: k4a index <subcommand> [options]

  Inspect and clear the local search index (~/.k4a/index). The index is
  an accelerator: clearing it only makes the next search of that topic
  scan the broker instead.

Subcommands:
  status                       List indexed topics with size and age
  clear <topic>... [--cluster ID]
                               Remove the index for each topic (in every
                               cluster unless --cluster is given)
  clear --all                  Remove every local topic index

  A topic another k4a process is writing is left alone and reported; close
  that process and run clear again.

Examples:
  k4a index status
  k4a index clear orders.events.v3
  k4a index clear --all`
}

// Synopsis returns the one-line description.
func (c *IndexCommand) Synopsis() string {
	return "Inspect and clear the local search index"
}

// Run dispatches on the first arg.
func (c *IndexCommand) Run(args []string) int {
	root := c.Root
	if root == nil {
		root = state.NewRoot(config.DefaultConfigDir())
	}
	out := c.Out
	if out == nil {
		out = os.Stdout
	}

	sub := subcmdStatus
	if len(args) > 0 {
		sub = strings.ToLower(args[0])
	}
	switch sub {
	case subcmdStatus:
		return runIndexStatus(out, root)
	case "clear":
		return runIndexClear(out, root, args[1:])
	case subcmdHelp, "-h", flagHelp:
		fmt.Fprintln(out, c.Help()) //nolint:errcheck // stdout; not actionable
		return 0
	default:
		log.Error("unknown index subcommand", "subcommand", sub)
		fmt.Fprintln(out, c.Help()) //nolint:errcheck // stdout; not actionable
		return 1
	}
}

//nolint:errcheck // tabwriter to stdout; errors not actionable
func runIndexStatus(out io.Writer, root *state.Root) int {
	inv, err := index.List(root)
	if err != nil {
		log.Error("listing indexes failed", "err", err)
		return 1
	}
	fmt.Fprintf(out, "Index directory: %s\n", root.File(state.SubsystemIndex))
	if len(inv) == 0 {
		fmt.Fprintln(out, "No local topic indexes.")
		return 0
	}
	fmt.Fprintln(out)
	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tTOPIC\tPARTITIONS\tSIZE\tOLDEST\tNEWEST")
	var total int64
	for _, t := range inv {
		total += t.Bytes
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n",
			t.Cluster, t.Topic, t.Partitions, formatBytes(t.Bytes),
			formatTimeOrDash(t.OldestAt), formatTimeOrDash(t.NewestAt))
	}
	w.Flush()
	fmt.Fprintf(out, "\n%d topic(s), %s total.\n", len(inv), formatBytes(total))
	return 0
}

//nolint:errcheck // stdout; not actionable
func runIndexClear(out io.Writer, root *state.Root, args []string) int {
	var flags indexClearFlags
	parser := goflags.NewParser(&flags, goflags.HelpFlag|goflags.PassDoubleDash)
	topics, err := parser.ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid arguments", "err", err)
		return 1
	}
	switch {
	case flags.All && len(topics) > 0:
		log.Error("give topics or --all, not both")
		return 1
	case flags.All && flags.Cluster != "":
		log.Error("--all clears every cluster; drop --cluster")
		return 1
	case !flags.All && len(topics) == 0:
		log.Error("usage: k4a index clear <topic>... [--cluster ID] | --all")
		return 1
	}

	var total index.ClearResult
	if flags.All {
		if total, err = index.ClearAll(root); err != nil {
			log.Error("clear failed", "err", err)
			return 1
		}
	}
	for _, topic := range topics {
		res, err := index.ClearTopic(root, flags.Cluster, topic)
		total.Bytes += res.Bytes
		total.Cleared += res.Cleared
		total.Locked = append(total.Locked, res.Locked...)
		if err != nil {
			log.Error("clear failed", "topic", topic, "err", err)
			return 1
		}
	}
	return reportIndexClear(out, total)
}

//nolint:errcheck // stdout; not actionable
func reportIndexClear(out io.Writer, res index.ClearResult) int {
	switch {
	case res.Cleared > 0:
		fmt.Fprintf(out, "Cleared %d index dir(s) (%s).\n", res.Cleared, formatBytes(res.Bytes))
	case len(res.Locked) == 0:
		fmt.Fprintln(out, "index: nothing to clear.")
	}
	if len(res.Locked) == 0 {
		return 0
	}
	for _, l := range res.Locked {
		log.Error("left in place: another k4a process is writing this index", "index", l)
	}
	return 1
}
