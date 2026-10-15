// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"io"

	"github.com/blairham/k4a/internal/kafka"
)

// ListTopicsCmdFlags defines flags for the list-topics command.
type ListTopicsCmdFlags struct {
	FormatFlag
	ConnectionFlags
	Internal bool `long:"internal" description:"Include internal topics (__consumer_offsets, _schemas, ...)"`
	Counts   bool `long:"counts"   description:"Add each topic's message count (an extra ListOffsets round trip)"`
}

// ListTopicsCmd implements the "list-topics" CLI command.
type ListTopicsCmd struct {
	Out io.Writer // stdout when nil
}

type topicRow struct {
	Messages          *int64 `json:"messages,omitempty"`
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	OutOfSync         int    `json:"outOfSyncPartitions"`
	Internal          bool   `json:"internal"`
}

// Help returns the detailed help text.
func (c *ListTopicsCmd) Help() string {
	return `Usage: k4a list-topics [options]

  List the cluster's topics with partition count, replication factor and
  out-of-sync partitions: the TUI's Topics view, headless. Internal topics
  are hidden unless --internal is given.

Options:
      --internal           Include internal topics
      --counts             Add message counts (extra ListOffsets)
      --format=FORMAT      Output format: text (default) or json

` + clusterFlagsHelp + `

Examples:
  k4a list-topics --context staging
  k4a list-topics --counts --format json | jq '.[] | select(.messages == 0)'`
}

// Synopsis returns the one-line description.
func (c *ListTopicsCmd) Synopsis() string {
	return "List topics"
}

// Run executes the list-topics command.
func (c *ListTopicsCmd) Run(args []string) int {
	var flags ListTopicsCmdFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		rows, err := fetchTopicRows(ctx, client, flags.Internal, flags.Counts)
		if err != nil {
			return err
		}
		out := orStdout(c.Out)
		if flags.JSON() {
			return writeJSON(out, rows)
		}
		return writeTopicRows(out, rows, flags.Counts)
	})
}

func fetchTopicRows(ctx context.Context, client *kafka.Client, internal, counts bool) ([]topicRow, error) {
	topics, err := client.FetchTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}
	rows := make([]topicRow, 0, len(topics))
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		if t.Internal && !internal {
			continue
		}
		rows = append(rows, topicRow{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			OutOfSync:         t.OutOfSync,
			Internal:          t.Internal,
		})
		names = append(names, t.Name)
	}
	if !counts || len(names) == 0 {
		return rows, nil
	}
	got, err := client.FetchTopicMessageCounts(ctx, names)
	if err != nil {
		return nil, fmt.Errorf("counting messages: %w", err)
	}
	for i := range rows {
		n := got[rows[i].Name]
		rows[i].Messages = &n
	}
	return rows, nil
}

func writeTopicRows(w io.Writer, rows []topicRow, counts bool) error {
	p := newPrinter(w)
	if len(rows) == 0 {
		p.printf("No topics found.\n")
		return p.done()
	}
	header := "NAME\tPARTITIONS\tRF\tOUT-OF-SYNC"
	if counts {
		header += "\tMESSAGES"
	}
	p.printf("%s\n", header)
	for _, r := range rows {
		p.printf("%s\t%d\t%d\t%d", r.Name, r.Partitions, r.ReplicationFactor, r.OutOfSync)
		if counts && r.Messages != nil {
			p.printf("\t%d", *r.Messages)
		}
		p.printf("\n")
	}
	return p.done()
}
