// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/timebound"
)

// --- delete-acl ------------------------------------------------------------

// DeleteACLCmdFlags defines flags for the delete-acl command; they name one
// binding exactly as create-acl does, with the same defaults.
type DeleteACLCmdFlags struct {
	ResourceType string `long:"resource-type" required:"true" description:"Resource type: topic, group, cluster, transactionalid"`
	ResourceName string `long:"resource-name" required:"true" description:"Resource name"`
	Principal    string `long:"principal"     required:"true" description:"Principal (e.g. User:alice)"`
	Operation    string `long:"operation"     required:"true" description:"Operation: all, read, write, create, delete, alter, describe, ..."`
	PatternType  string `long:"pattern-type"                  description:"Pattern type: literal, prefixed"                                   default:"literal"`
	Host         string `long:"host"                          description:"Host (* for all hosts)"                                            default:"*"`
	Permission   string `long:"permission"                    description:"Permission type: allow, deny"                                      default:"allow"`
	ConnectionFlags
	Yes bool `long:"yes"                           description:"Delete without asking for confirmation"                                              short:"y"`
}

// DeleteACLCmd implements the "delete-acl" CLI command.
type DeleteACLCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *DeleteACLCmd) Help() string {
	return `Usage: k4a delete-acl [options]

  Delete one ACL binding, named by the same flags (and defaults) as
  create-acl. Exactly one existing binding must match; the command never
  deletes by wildcard. Asks for confirmation unless --yes; refused when
  ui.readonly is set.

Options:
      --resource-type=TYPE   topic, group, cluster, transactionalid (required)
      --resource-name=NAME   Resource name (required)
      --principal=PRINCIPAL  Principal, e.g. User:alice (required)
      --operation=OP         all, read, write, create, delete, alter, describe, ... (required)
      --pattern-type=TYPE    literal or prefixed (default: literal)
      --host=HOST            Host, * for all (default: *)
      --permission=PERM      allow or deny (default: allow)
  -y, --yes                  Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a delete-acl --resource-type topic --resource-name orders \
    --principal User:alice --operation read`
}

// Synopsis returns the one-line description.
func (c *DeleteACLCmd) Synopsis() string {
	return "Delete an ACL binding"
}

// Run executes the delete-acl command.
func (c *DeleteACLCmd) Run(args []string) int {
	var flags DeleteACLCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	want := kafka.ACLEntry{
		ResourceType: flags.ResourceType, ResourceName: flags.ResourceName, PatternType: flags.PatternType,
		Principal: flags.Principal, Host: flags.Host, Operation: flags.Operation, Permission: flags.Permission,
	}
	question := fmt.Sprintf("Delete ACL %s %s on %s %q (%s, host %s)?",
		want.Permission, want.Operation, want.ResourceType, want.ResourceName, want.Principal, want.Host)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "delete-acl", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		entries, fetchErr := client.FetchACLs(ctx)
		if fetchErr != nil {
			return fetchErr
		}
		match, ok := findACL(entries, want)
		if !ok {
			return errors.New("no ACL binding matches; nothing deleted (k4a list-acls shows the bindings)")
		}
		if err := client.DeleteACL(ctx, match); err != nil {
			return err
		}
		_, err := fmt.Fprintln(orStdout(c.Out), "ACL deleted.")
		return err
	})
}

// findACL returns the listed binding want names. The enum fields compare
// loosely (case, '-' and '_'), since list-acls prints "describe_configs"
// where a user types "describe-configs"; names, principal and host are
// exact. Deleting the listed entry, not the typed one, sends the broker
// exactly the binding that exists.
func findACL(entries []kafka.ACLEntry, want kafka.ACLEntry) (kafka.ACLEntry, bool) {
	for _, e := range entries {
		if e.ResourceName == want.ResourceName && e.Principal == want.Principal && e.Host == want.Host &&
			aclEnum(e.ResourceType) == aclEnum(want.ResourceType) &&
			aclEnum(e.PatternType) == aclEnum(want.PatternType) &&
			aclEnum(e.Operation) == aclEnum(want.Operation) &&
			aclEnum(e.Permission) == aclEnum(want.Permission) {
			return e, true
		}
	}
	return kafka.ACLEntry{}, false
}

func aclEnum(s string) string {
	return strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(s))
}

// --- add-partitions --------------------------------------------------------

// AddPartitionsCmdFlags defines flags for the add-partitions command.
type AddPartitionsCmdFlags struct {
	Topic string `short:"t" long:"topic"      required:"true" description:"Topic"`
	ConnectionFlags
	Partitions int  `short:"n" long:"partitions" required:"true" description:"New total partition count (must exceed the current one)"`
	Yes        bool `short:"y" long:"yes"                        description:"Add without asking for confirmation"`
}

// AddPartitionsCmd implements the "add-partitions" CLI command.
type AddPartitionsCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *AddPartitionsCmd) Help() string {
	return `Usage: k4a add-partitions -t TOPIC -n COUNT [options]

  Raise a topic's partition count to COUNT, as Topic Detail's p does. Kafka
  cannot lower it again, and keyed records will hash to different
  partitions from now on, breaking per-key ordering across the change.
  Asks for confirmation unless --yes; refused when ui.readonly is set.

Options:
  -t, --topic=TOPIC        Topic (required)
  -n, --partitions=COUNT   New total partition count (required)
  -y, --yes                Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a add-partitions -t orders -n 12`
}

// Synopsis returns the one-line description.
func (c *AddPartitionsCmd) Synopsis() string {
	return "Increase a topic's partition count"
}

// Run executes the add-partitions command.
func (c *AddPartitionsCmd) Run(args []string) int {
	var flags AddPartitionsCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	if flags.Partitions < 1 {
		log.Error("--partitions must be at least 1")
		return 1
	}
	question := fmt.Sprintf(
		"Increase topic %q to %d partitions? Keys will hash to different partitions, and the count can never be lowered.",
		flags.Topic, flags.Partitions,
	)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "add-partitions", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		ids, fetchErr := client.FetchPartitionIDs(ctx, flags.Topic)
		if fetchErr != nil {
			return fetchErr
		}
		if flags.Partitions <= len(ids) {
			return fmt.Errorf("topic %q already has %d partitions; --partitions must be greater", flags.Topic, len(ids))
		}
		if err := client.IncreasePartitions(ctx, flags.Topic, flags.Partitions); err != nil {
			return err
		}
		_, err := fmt.Fprintf(
			orStdout(c.Out),
			"Topic %q now has %d partitions (was %d).\n",
			flags.Topic,
			flags.Partitions,
			len(ids),
		)
		return err
	})
}

// --- purge-topic -----------------------------------------------------------

// PurgeTopicCmdFlags defines flags for the purge-topic command.
type PurgeTopicCmdFlags struct {
	Topic     string `short:"t" long:"topic"      required:"true" description:"Topic"`
	OlderThan string `          long:"older-than"                 description:"Delete records older than this (30m, 6h, 7d, 2w)"`
	ConnectionFlags
	All bool `          long:"all"                        description:"Delete every record up to the high watermark"`
	Yes bool `short:"y" long:"yes"                        description:"Purge without asking for confirmation"`
}

// PurgeTopicCmd implements the "purge-topic" CLI command.
type PurgeTopicCmd struct {
	Prompt
}

// Help returns the detailed help text.
func (c *PurgeTopicCmd) Help() string {
	return `Usage: k4a purge-topic -t TOPIC (--all | --older-than DURATION) [options]

  Delete records now (DeleteRecords), as Topic Detail's ctrl+d does: the log
  start offset of every partition moves past them and consumers can no
  longer read them. --all purges up to the high watermark; --older-than
  purges what was written before that long ago. The topic's retention is
  not changed (k4a alter-config --set retention.ms=... does that, which is
  the TUI's "!" form). Asks for confirmation unless --yes; refused when
  ui.readonly is set.

Options:
  -t, --topic=TOPIC        Topic (required)
      --all                Purge every record
      --older-than=DUR     Purge records older than DUR (30m, 6h, 7d, 2w)
  -y, --yes                Skip the confirmation prompt

` + clusterFlagsHelp + `

Examples:
  k4a purge-topic -t orders.dlq --all
  k4a purge-topic -t orders --older-than 7d --yes`
}

// Synopsis returns the one-line description.
func (c *PurgeTopicCmd) Synopsis() string {
	return "Delete a topic's records (all, or older than a duration)"
}

// Run executes the purge-topic command.
func (c *PurgeTopicCmd) Run(args []string) int {
	var flags PurgeTopicCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	retentionMs, what, err := purgeTarget(flags.All, flags.OlderThan)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	question := fmt.Sprintf("Delete %s from topic %q? This cannot be undone.", what, flags.Topic)
	if err := guardMutation(&flags.ConnectionFlags, c.Prompt, "purge-topic", flags.Yes, question); err != nil {
		log.Error(err.Error())
		return 1
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		if err := client.PurgeTopicMessages(ctx, flags.Topic, retentionMs); err != nil {
			return fmt.Errorf("purge %q: %w", flags.Topic, err)
		}
		_, err := fmt.Fprintf(orStdout(c.Out), "Deleted %s from %q.\n", what, flags.Topic)
		return err
	})
}

// purgeTarget turns --all / --older-than into PurgeTopicMessages' argument,
// where anything up to one second means everything. A cutoff that short is
// refused rather than silently becoming --all.
func purgeTarget(all bool, olderThan string) (retentionMs int64, what string, err error) {
	switch {
	case all && olderThan != "":
		return 0, "", errors.New("give --all or --older-than, not both")
	case all:
		return 0, "every record", nil
	case olderThan == "":
		return 0, "", errors.New("give --all or --older-than DURATION")
	}
	d, err := timebound.ParseDuration(olderThan)
	if err != nil {
		return 0, "", fmt.Errorf("--older-than: %w", err)
	}
	if d <= time.Second {
		return 0, "", fmt.Errorf("--older-than %s is too short; use --all to purge everything", olderThan)
	}
	return d.Milliseconds(), "records older than " + olderThan, nil
}
