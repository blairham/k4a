// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
)

// Prompt asks a destructive command's confirmation. In, Out and Interactive
// default to stdin, stdout and a terminal check on stdin; tests replace them.
type Prompt struct {
	In          io.Reader
	Out         io.Writer
	Interactive func() bool
}

// ask is confirm with the defaults filled in.
func (p Prompt) ask(question string) error {
	in, interactive := p.In, p.Interactive
	if in == nil {
		in = os.Stdin
	}
	if interactive == nil {
		interactive = stdinIsTerminal
	}
	return confirm(in, orStdout(p.Out), interactive(), question)
}

// guardMutation is the gate every mutating command passes before touching
// the cluster: ui.readonly first, then the confirmation unless --yes.
func guardMutation(cf *ConnectionFlags, p Prompt, op string, yes bool, question string) error {
	if err := cf.checkWritable(op); err != nil {
		return err
	}
	if yes {
		return nil
	}
	if err := p.ask(question); err != nil {
		return fmt.Errorf("%s not done: %w", op, err)
	}
	return nil
}

// --- reset-offsets ---------------------------------------------------------

// The --to modes, named as the TUI's Reset Offsets form names them.
const (
	resetEarliest = "earliest"
	resetLatest   = "latest"
	resetOffset   = "offset"
	resetShift    = "shift"
)

// ResetOffsetsCmdFlags defines flags for the reset-offsets command.
type ResetOffsetsCmdFlags struct {
	Group string `short:"g" long:"group" required:"true" description:"Consumer group"`
	Topic string `short:"t" long:"topic" required:"true" description:"Topic whose committed offsets are reset"`
	Mode  string `          long:"to"    required:"true" description:"earliest, latest, offset (--value) or shift (--value, +/-)" choice:"earliest" choice:"latest" choice:"offset" choice:"shift"`
	Value string `          long:"value"                 description:"Target offset for --to offset; signed shift for --to shift"`
	ConnectionFlags
	Yes bool `short:"y" long:"yes"                   description:"Reset without asking for confirmation"`
}

// ResetOffsetsCmd implements the "reset-offsets" CLI command.
type ResetOffsetsCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *ResetOffsetsCmd) Help() string {
	return `Usage: k4a reset-offsets -g GROUP -t TOPIC --to MODE [--value N] [options]

  Reset a consumer group's committed offsets on every partition of a topic,
  with the modes of the TUI's Reset Offsets form:

    --to earliest          the log start offset
    --to latest            the high watermark (skip everything)
    --to offset --value N  offset N on every partition
    --to shift --value N   the committed offset plus N (negative rewinds,
                           written --value=-N); floors at 0, refused where a
                           partition has nothing committed

  The group must be Empty or Dead (no live consumers). Asks for confirmation
  unless --yes; without a terminal on stdin, --yes is required. Refused when
  ui.readonly is set.

Options:
  -g, --group=GROUP        Consumer group (required)
  -t, --topic=TOPIC        Topic (required)
      --to=MODE            earliest, latest, offset or shift (required)
      --value=N            Offset or shift amount
  -y, --yes                Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a reset-offsets -g orders-consumer -t orders --to earliest
  k4a reset-offsets -g orders-consumer -t orders --to shift --value=-100 --yes`
}

// Synopsis returns the one-line description.
func (c *ResetOffsetsCmd) Synopsis() string {
	return "Reset a consumer group's offsets on a topic"
}

// Run executes the reset-offsets command.
func (c *ResetOffsetsCmd) Run(args []string) int {
	var flags ResetOffsetsCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	strategy, err := parseResetStrategy(flags.Mode, flags.Value)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	question := fmt.Sprintf(
		"Reset group %q offsets on topic %q to %s?",
		flags.Group,
		flags.Topic,
		describeReset(flags.Mode, flags.Value),
	)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "reset-offsets", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		if err := client.ResetGroupOffsets(ctx, flags.Group, flags.Topic, strategy); err != nil {
			return fmt.Errorf("reset offsets: %w", err)
		}
		_, err := fmt.Fprintf(
			orStdout(c.Out),
			"Reset group %q on %q to %s.\n",
			flags.Group,
			flags.Topic,
			describeReset(flags.Mode, flags.Value),
		)
		return err
	})
}

// parseResetStrategy maps --to/--value onto the strategy the TUI's form
// builds, so both surfaces accept the same modes.
func parseResetStrategy(mode, value string) (kafka.OffsetResetStrategy, error) {
	switch mode {
	case resetEarliest, resetLatest:
		if value != "" {
			return kafka.OffsetResetStrategy{}, fmt.Errorf("--value does not apply to --to %s", mode)
		}
		if mode == resetEarliest {
			return kafka.OffsetResetStrategy{Mode: kafka.ResetToEarliest}, nil
		}
		return kafka.OffsetResetStrategy{Mode: kafka.ResetToLatest}, nil
	case resetOffset, resetShift:
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return kafka.OffsetResetStrategy{}, fmt.Errorf("--to %s needs an integer --value, got %q", mode, value)
		}
		if mode == resetOffset {
			if n < 0 {
				return kafka.OffsetResetStrategy{}, fmt.Errorf("--to offset needs a non-negative --value, got %d", n)
			}
			return kafka.OffsetResetStrategy{Mode: kafka.ResetToOffset, Value: n}, nil
		}
		return kafka.OffsetResetStrategy{Mode: kafka.ResetShiftBy, Value: n}, nil
	default:
		return kafka.OffsetResetStrategy{}, fmt.Errorf("unknown --to %q", mode)
	}
}

func describeReset(mode, value string) string {
	switch mode {
	case resetOffset:
		return "offset " + value
	case resetShift:
		return "committed offset shifted by " + value
	default:
		return mode
	}
}

// --- delete-offsets --------------------------------------------------------

// DeleteOffsetsCmdFlags defines flags for the delete-offsets command.
type DeleteOffsetsCmdFlags struct {
	ConnectionFlags
	Group      string `short:"g" long:"group"     required:"true" description:"Consumer group"`
	Topic      string `short:"t" long:"topic"     required:"true" description:"Topic"`
	Partitions []int  `short:"P" long:"partition" required:"true" description:"Partition whose committed offset is deleted (repeatable)"`
	Yes        bool   `short:"y" long:"yes"                       description:"Delete without asking for confirmation"`
}

// DeleteOffsetsCmd implements the "delete-offsets" CLI command.
type DeleteOffsetsCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *DeleteOffsetsCmd) Help() string {
	return `Usage: k4a delete-offsets -g GROUP -t TOPIC -P PARTITION... [options]

  Delete a consumer group's committed offset for one or more partitions of a
  topic (OffsetDelete), as Group Detail's :reset does for the selected row.
  The group then starts those partitions from its auto.offset.reset policy.
  The group must be Empty or Dead. Asks for confirmation unless --yes;
  refused when ui.readonly is set.

Options:
  -g, --group=GROUP        Consumer group (required)
  -t, --topic=TOPIC        Topic (required)
  -P, --partition=N        Partition (required, repeatable)
  -y, --yes                Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a delete-offsets -g orders-consumer -t orders -P 0 -P 3`
}

// Synopsis returns the one-line description.
func (c *DeleteOffsetsCmd) Synopsis() string {
	return "Delete a consumer group's committed offsets"
}

// Run executes the delete-offsets command.
func (c *DeleteOffsetsCmd) Run(args []string) int {
	var flags DeleteOffsetsCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	question := fmt.Sprintf(
		"Delete group %q committed offsets on topic %q partitions %v?",
		flags.Group,
		flags.Topic,
		flags.Partitions,
	)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "delete-offsets", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		var errs []error
		for _, p := range flags.Partitions {
			if err := client.DeleteGroupOffsets(ctx, flags.Group, flags.Topic, p); err != nil {
				errs = append(errs, fmt.Errorf("partition %d: %w", p, err))
				continue
			}
			if _, err := fmt.Fprintf(orStdout(c.Out), "Deleted %q offset on %s/%d.\n", flags.Group, flags.Topic, p); err != nil {
				return err
			}
		}
		return errors.Join(errs...)
	})
}

// --- delete-group ----------------------------------------------------------

// DeleteGroupCmdFlags defines flags for the delete-group command.
type DeleteGroupCmdFlags struct {
	Group string `short:"g" long:"group" required:"true" description:"Consumer group to delete"`
	ConnectionFlags
	Yes bool `short:"y" long:"yes"                   description:"Delete without asking for confirmation"`
}

// DeleteGroupCmd implements the "delete-group" CLI command.
type DeleteGroupCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *DeleteGroupCmd) Help() string {
	return `Usage: k4a delete-group -g GROUP [options]

  Delete a consumer group and all its committed offsets. The broker refuses
  a group that still has members. Asks for confirmation unless --yes;
  without a terminal on stdin, --yes is required. Refused when ui.readonly
  is set.

Options:
  -g, --group=GROUP        Consumer group (required)
  -y, --yes                Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a delete-group -g old-consumer
  k4a delete-group --context staging -g old-consumer --yes`
}

// Synopsis returns the one-line description.
func (c *DeleteGroupCmd) Synopsis() string {
	return "Delete a consumer group"
}

// Run executes the delete-group command.
func (c *DeleteGroupCmd) Run(args []string) int {
	var flags DeleteGroupCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	question := fmt.Sprintf("Delete consumer group %q and its committed offsets? This cannot be undone.", flags.Group)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "delete-group", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		if err := client.DeleteGroup(ctx, flags.Group); err != nil {
			return err
		}
		_, err := fmt.Fprintf(orStdout(c.Out), "Deleted consumer group %q.\n", flags.Group)
		return err
	})
}
