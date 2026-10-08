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

// describeTopicTimeout bounds the whole command. Each topic costs one
// DescribeConfigs round trip, so allow more than the 15s the single-shot
// commands use without going as far as topic-size's log-dir sweep.
const describeTopicTimeout = 30 * time.Second

// dynamicTopicSource is the ConfigSource that marks a topic-level override —
// a value explicitly set on the topic rather than inherited from the broker.
const dynamicTopicSource = "DYNAMIC_TOPIC"

// DescribeTopicFlags defines flags for the describe-topic command.
type DescribeTopicFlags struct {
	ConfigKey string `long:"config-key" description:"Only report this config key (shown even when inherited)"`
	ConnectionFlags
	All  bool `long:"all"        description:"Include inherited defaults, not just topic-level overrides"`
	JSON bool `long:"json"       description:"Emit JSON instead of a table"                               short:"j"`
}

// DescribeTopicCmd implements the "describe-topic" CLI command.
type DescribeTopicCmd struct{}

// topicConfigRow is one rendered line: a config entry tagged with its topic.
type topicConfigRow struct {
	Topic     string `json:"topic"`
	Name      string `json:"name"`
	Value     string `json:"value"`
	Source    string `json:"source"`
	ReadOnly  bool   `json:"readOnly"`
	Sensitive bool   `json:"sensitive"`
}

// Help returns the detailed help text.
func (c *DescribeTopicCmd) Help() string {
	return `Usage: k4a describe-topic [options] TOPIC...

  Report the configuration of one or more topics (DescribeConfigs).

  By default only topic-level OVERRIDES are shown — keys explicitly set on the
  topic (source DYNAMIC_TOPIC) rather than inherited from the broker. This
  matches "kafka-configs.sh --describe" and answers the question that usually
  prompts the lookup: has anyone pinned something on this topic? Use --all to
  see every key including inherited defaults.

  --config-key reports a single key even when it is inherited, which makes it
  easy to compare one setting across a set of topics.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME       Named context from config file
      --config=FILE        Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS    Bootstrap servers, comma-separated
      --config-key=NAME    Only report this config key (shown even if inherited)
      --all                Include inherited defaults, not just overrides
  -j, --json               Emit JSON instead of a table
  -a, --auth=METHOD        Auth method: plaintext, tls, scram, mtls, iam
      --username=USER      SASL/SCRAM username
      --password=PASS      SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE          Client certificate PEM (mTLS)
      --key=FILE           Client private key PEM (mTLS)
      --ca=FILE            CA certificate PEM
  -r, --region=REGION      AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE    AWS profile for IAM auth
  -k, --insecure           Skip TLS certificate verification

Examples:
  # What has been pinned on this topic?
  k4a describe-topic --context production example.ingest.markets.v1

  # Compare one setting across several topics
  k4a describe-topic --context production --config-key min.insync.replicas \
    example.ingest.markets.v1 example.ingest.prices.v1

  # Every key, including inherited broker defaults
  k4a describe-topic --context production --all quotes-east-book

  # Machine-readable output for scripting
  k4a describe-topic --context production --json quotes-east-book`
}

// Synopsis returns the one-line description.
func (c *DescribeTopicCmd) Synopsis() string {
	return "Show a topic's configuration (overrides by default)"
}

// Run executes the describe-topic command.
func (c *DescribeTopicCmd) Run(args []string) int {
	var flags DescribeTopicFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	topics, err := parser.ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	if len(topics) == 0 {
		log.Error("at least one TOPIC argument is required")
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
	ctx, timeoutCancel := context.WithTimeout(ctx, describeTopicTimeout)
	defer timeoutCancel()

	var rows []topicConfigRow
	failed := false
	for _, topic := range topics {
		entries, fErr := client.FetchTopicConfigs(ctx, topic)
		if fErr != nil {
			// Keep going: describing five topics should not be all-or-nothing
			// just because one name was mistyped or has been deleted.
			log.Error("describing topic failed", "topic", topic, "err", fErr)
			failed = true
			continue
		}
		rows = append(rows, filterTopicConfigs(topic, entries, flags.ConfigKey, flags.All)...)
	}

	sortTopicConfigRows(rows)

	if flags.JSON {
		if emitTopicConfigsJSON(rows) != 0 {
			return 1
		}
	} else {
		emitTopicConfigsTable(rows)
	}

	if failed {
		return 1
	}
	return 0
}

// filterTopicConfigs narrows a topic's entries to what was asked for. An
// explicit --config-key always wins, so a key can be inspected even when it is
// inherited; otherwise --all decides between every key and overrides only.
func filterTopicConfigs(topic string, entries []kafka.ConfigEntry, configKey string, all bool) []topicConfigRow {
	rows := make([]topicConfigRow, 0, len(entries))
	for _, e := range entries {
		switch {
		case configKey != "":
			if e.Name != configKey {
				continue
			}
		case !all && e.Source != dynamicTopicSource:
			continue
		}
		rows = append(rows, topicConfigRow{
			Topic:     topic,
			Name:      e.Name,
			Value:     e.Value,
			Source:    e.Source,
			ReadOnly:  e.ReadOnly,
			Sensitive: e.Sensitive,
		})
	}
	return rows
}

// sortTopicConfigRows orders rows by topic then config name, so output is
// stable and a multi-topic comparison reads down the page in a fixed order.
func sortTopicConfigRows(rows []topicConfigRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Topic != rows[j].Topic {
			return rows[i].Topic < rows[j].Topic
		}
		return rows[i].Name < rows[j].Name
	})
}

func emitTopicConfigsJSON(rows []topicConfigRow) int {
	if rows == nil {
		rows = []topicConfigRow{}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rows); err != nil {
		log.Error("encoding JSON failed", "err", err)
		return 1
	}
	return 0
}

//nolint:errcheck // tabwriter to stdout; write errors aren't actionable
func emitTopicConfigsTable(rows []topicConfigRow) {
	if len(rows) == 0 {
		fmt.Println("No topic-level config overrides. Use --all to include inherited defaults.")
		return
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tCONFIG\tVALUE\tSOURCE")
	for _, r := range rows {
		value := r.Value
		if r.Sensitive {
			value = "(sensitive)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Topic, r.Name, value, r.Source)
	}
	tw.Flush()
}
