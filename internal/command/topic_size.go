// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

// topicSizeTimeout bounds the whole command. Describing log dirs on every broker
// plus an optional ListOffsets sweep across many topics is slower than a plain
// metadata fetch, so this is more generous than the 15s the lighter commands use.
const topicSizeTimeout = 60 * time.Second

// TopicSizeFlags defines flags for the topic-size command.
type TopicSizeFlags struct {
	SortBy string `long:"sort"     choice:"size" choice:"name" default:"size" description:"Sort order: size (desc) or name"`
	ConnectionFlags
	Counts   bool `long:"counts"                                              description:"Also fetch message counts (extra ListOffsets calls)" short:"m"`
	Internal bool `long:"internal"                                            description:"Include internal topics when listing all topics"     short:"i"`
	JSON     bool `long:"json"                                                description:"Emit JSON instead of a table"                        short:"j"`
}

// TopicSizeCmd implements the "topic-size" CLI command.
type TopicSizeCmd struct{}

// Help returns the detailed help text.
func (c *TopicSizeCmd) Help() string {
	return `Usage: k4a topic-size [options] [TOPIC...]

  Report per-topic on-disk size from broker log directories (DescribeLogDirs).

  With no TOPIC arguments, every topic is reported. Otherwise only the named
  topics are described.

  Two size columns are shown:
    SIZE   logical size — one replica's worth of data (leader partitions only).
    ONDISK full on-disk footprint summed across every replica on every broker
           (for an RF=3 topic this is ~3× SIZE).

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
      --sort=ORDER        Sort by size (default, descending) or name
  -m, --counts            Also fetch message counts (extra ListOffsets calls)
  -i, --internal          Include internal topics when listing all topics
  -j, --json              Emit JSON instead of a table
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, mtls, iam
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification

Examples:
  # Every topic, largest on disk first
  k4a topic-size --context production

  # A single topic, with message count
  k4a topic-size --context production --counts quotes-east-book

  # Machine-readable output for scripting
  k4a topic-size --context production --json quotes-east-book`
}

// Synopsis returns the one-line description.
func (c *TopicSizeCmd) Synopsis() string {
	return "Report per-topic on-disk size from broker log directories"
}

// Run executes the topic-size command.
func (c *TopicSizeCmd) Run(args []string) int {
	var flags TopicSizeFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	topics, err := parser.ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	expandEnvFlags(&flags)

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, topicSizeTimeout)
	defer timeoutCancel()

	sizes, err := client.FetchTopicSizes(ctx, topics)
	if err != nil {
		log.Error("fetching topic sizes failed", "err", err)
		return 1
	}

	// Drop internal topics unless explicitly requested or explicitly named.
	if len(topics) == 0 && !flags.Internal {
		filtered := sizes[:0]
		for _, s := range sizes {
			if !s.Internal {
				filtered = append(filtered, s)
			}
		}
		sizes = filtered
	}

	if flags.Counts {
		names := make([]string, len(sizes))
		for i, s := range sizes {
			names[i] = s.Name
		}
		if counts, cErr := client.FetchTopicMessageCounts(ctx, names); cErr != nil {
			log.Warn("message counts unavailable", "err", cErr)
		} else {
			for i := range sizes {
				if n, ok := counts[sizes[i].Name]; ok {
					sizes[i].Messages = n
				}
			}
		}
	}

	sortTopicSizes(sizes, flags.SortBy)

	if flags.JSON {
		return emitTopicSizesJSON(sizes)
	}
	emitTopicSizesTable(sizes, flags.Counts)
	return 0
}

// sortTopicSizes orders sizes in place: by ONDISK descending (default), or by
// name ascending. The name tiebreaker keeps the size ordering deterministic.
func sortTopicSizes(sizes []kafka.TopicSize, sortBy string) {
	sort.Slice(sizes, func(i, j int) bool {
		if sortBy == "name" {
			return sizes[i].Name < sizes[j].Name
		}
		if sizes[i].TotalBytes != sizes[j].TotalBytes {
			return sizes[i].TotalBytes > sizes[j].TotalBytes
		}
		return sizes[i].Name < sizes[j].Name
	})
}

func emitTopicSizesJSON(sizes []kafka.TopicSize) int {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(sizes); err != nil {
		log.Error("encoding JSON failed", "err", err)
		return 1
	}
	return 0
}

//nolint:errcheck // tabwriter to stdout; write errors aren't actionable
func emitTopicSizesTable(sizes []kafka.TopicSize, withCounts bool) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	header := "TOPIC\tPARTITIONS\tSIZE\tONDISK"
	if withCounts {
		header += "\tMESSAGES"
	}
	fmt.Fprintln(tw, header)

	var totalLeader, totalDisk, totalMsgs int64
	for _, s := range sizes {
		row := fmt.Sprintf("%s\t%d\t%s\t%s", s.Name, s.Partitions, formatBytes(s.LeaderBytes), formatBytes(s.TotalBytes))
		if withCounts {
			row += "\t" + topicMessageCount(s.Messages)
		}
		fmt.Fprintln(tw, row)
		totalLeader += s.LeaderBytes
		totalDisk += s.TotalBytes
		if s.Messages > 0 {
			totalMsgs += s.Messages
		}
	}

	if len(sizes) > 1 {
		total := fmt.Sprintf("TOTAL (%d topics)\t\t%s\t%s", len(sizes), formatBytes(totalLeader), formatBytes(totalDisk))
		if withCounts {
			total += "\t" + topicMessageCount(totalMsgs)
		}
		fmt.Fprintln(tw, total)
	}

	tw.Flush()
}

// topicMessageCount renders a message count, showing "-" for the not-fetched
// sentinel and otherwise deferring to the package's humanized formatCount.
func topicMessageCount(n int64) string {
	if n < 0 {
		return "-"
	}
	return formatCount(n)
}
