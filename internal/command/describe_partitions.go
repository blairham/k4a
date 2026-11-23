// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"io"
	"slices"
	"strconv"

	"github.com/blairham/k4a/internal/kafka"
)

// noBroker is the --leader / --broker default: no broker filter. Kafka's
// broker IDs are non-negative, so -1 cannot collide with one.
const noBroker = -1

// DescribePartitionsCmdFlags defines flags for the describe-partitions command.
type DescribePartitionsCmdFlags struct {
	FormatFlag
	ConnectionFlags
	Leader          int  `long:"leader"           default:"-1" description:"Only partitions led by broker N"`
	Broker          int  `long:"broker"           default:"-1" description:"Only partitions with a replica on broker N"`
	Internal        bool `long:"internal"                      description:"Include internal topics when no TOPIC is named"`
	UnderReplicated bool `long:"under-replicated"              description:"Only partitions whose ISR is smaller than their replica set"`
}

// DescribePartitionsCmd implements the "describe-partitions" CLI command.
type DescribePartitionsCmd struct {
	Out io.Writer // stdout when nil
}

type partitionRow struct {
	Topic           string `json:"topic"`
	Replicas        []int  `json:"replicas"`
	ISR             []int  `json:"isr"`
	Partition       int    `json:"partition"`
	Leader          int    `json:"leader"`
	FirstOffset     int64  `json:"firstOffset"`
	LastOffset      int64  `json:"lastOffset"`
	Messages        int64  `json:"messages"`
	UnderReplicated bool   `json:"underReplicated"`
}

// Help returns the detailed help text.
func (c *DescribePartitionsCmd) Help() string {
	return `Usage: k4a describe-partitions [options] [TOPIC...]

  Print each partition's leader, replicas, in-sync replicas and offsets: the
  TUI's Topic Detail partition table, headless. With no TOPIC, every topic is
  described from one Metadata request, so this is also a whole-cluster leader
  map; internal topics are then hidden unless --internal is given. Read-only:
  only Metadata and ListOffsets requests are sent, and no group is joined.

  A LEADER of -1 (text: "none") means the partition has no leader.

Options:
      --leader=N           Only partitions led by broker N
      --broker=N           Only partitions with a replica on broker N
      --under-replicated   Only partitions whose ISR is smaller than the replica set
      --internal           Include internal topics when no TOPIC is named
      --format=FORMAT      Output format: text (default) or json

` + clusterFlagsHelp + `

Examples:
  k4a describe-partitions orders
  k4a describe-partitions --leader 1                 # everything broker 1 leads
  k4a describe-partitions --under-replicated --format json`
}

// Synopsis returns the one-line description.
func (c *DescribePartitionsCmd) Synopsis() string {
	return "Show each partition's leader, replicas and ISR"
}

// Run executes the describe-partitions command.
func (c *DescribePartitionsCmd) Run(args []string) int {
	var flags DescribePartitionsCmdFlags
	topics, code, done := parseCmdArgs(&flags, args)
	if done {
		return code
	}
	return withClient(&flags.ConnectionFlags, func(ctx context.Context, client *kafka.Client) error {
		tps, err := client.FetchPartitions(ctx, topics...)
		if err != nil {
			return err
		}
		rows := partitionRows(tps, &flags, len(topics) > 0)
		out := orStdout(c.Out)
		if flags.JSON() {
			return writeJSON(out, rows)
		}
		return writePartitionRows(out, rows)
	})
}

// partitionRows flattens tps into rows, keeping those the filters select. A
// named topic is shown even when internal: naming it is the opt-in.
func partitionRows(tps []kafka.TopicPartitions, f *DescribePartitionsCmdFlags, named bool) []partitionRow {
	rows := []partitionRow{}
	for _, tp := range tps {
		if tp.Internal && !named && !f.Internal {
			continue
		}
		for i := range tp.Partitions {
			p := &tp.Partitions[i]
			row := partitionRow{
				Topic:           tp.Name,
				Partition:       p.ID,
				Leader:          p.Leader,
				Replicas:        p.Replicas,
				ISR:             p.ISR,
				FirstOffset:     p.FirstOffset,
				LastOffset:      p.LastOffset,
				Messages:        p.Messages,
				UnderReplicated: len(p.ISR) < len(p.Replicas),
			}
			if f.keep(&row) {
				rows = append(rows, row)
			}
		}
	}
	return rows
}

func (f *DescribePartitionsCmdFlags) keep(r *partitionRow) bool {
	if f.Leader != noBroker && r.Leader != f.Leader {
		return false
	}
	if f.Broker != noBroker && !slices.Contains(r.Replicas, f.Broker) {
		return false
	}
	return !f.UnderReplicated || r.UnderReplicated
}

func writePartitionRows(w io.Writer, rows []partitionRow) error {
	p := newPrinter(w)
	if len(rows) == 0 {
		p.printf("No partitions found.\n")
		return p.done()
	}
	p.printf("TOPIC\tPARTITION\tLEADER\tREPLICAS\tISR\tFIRST\tLAST\tMESSAGES\n")
	for i := range rows {
		r := &rows[i]
		p.printf("%s\t%d\t%s\t%v\t%v\t%d\t%d\t%d\n",
			r.Topic, r.Partition, leaderText(r.Leader), r.Replicas, r.ISR, r.FirstOffset, r.LastOffset, r.Messages)
	}
	return p.done()
}

func leaderText(id int) string {
	if id < 0 {
		return "none"
	}
	return strconv.Itoa(id)
}
