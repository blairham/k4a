// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
)

// ProduceFlags defines flags for the produce command.
type ProduceFlags struct {
	Topic    string `short:"t" long:"topic"    env:"KAFKA_TOPIC" description:"Target topic (overridden by schema)"`
	Schema   string `short:"s" long:"schema"                     description:"YAML schema file for message generation"`
	Interval string `short:"i" long:"interval"                   description:"Delay between messages"                  default:"1s"`
	ConnectionFlags
	Count   int  `short:"c" long:"count"                      description:"Number of messages to produce"           default:"10"`
	Verbose bool `short:"v" long:"verbose"                    description:"Verbose output"`
}

// ProduceCommand implements the "produce" CLI command.
type ProduceCommand struct{}

// Help returns the detailed help text.
func (c *ProduceCommand) Help() string {
	return `Usage: k4a produce [options]

  Produce test messages to a Kafka topic. Uses random payloads by default,
  or realistic messages when a --schema YAML file is provided.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -t, --topic=TOPIC       Target topic (overridden by schema if provided)
  -s, --schema=FILE       YAML schema file for realistic message generation
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, mtls, iam
  -c, --count=N           Number of messages (default: 10)
  -i, --interval=DUR      Delay between messages (default: 1s)
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification
  -v, --verbose           Verbose output

Examples:
  # Use current-context from config
  k4a produce -t test-topic -c 5

  # Use a specific context
  k4a produce --context staging -t test-topic

  # Direct connection
  k4a produce -b broker:9196 -t test-topic --username user --password pass
  k4a produce -b broker:9098 -t test-topic --auth iam -c 5 -v
  k4a produce -b broker:9196 -s schemas/example.orders.v1.yaml -c 20`
}

// Synopsis returns the one-line description.
func (c *ProduceCommand) Synopsis() string {
	return "Produce test messages to a Kafka topic"
}

// Run executes the produce command.
func (c *ProduceCommand) Run(args []string) int {
	var flags ProduceFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	expandEnvFlags(&flags)

	interval, err := time.ParseDuration(flags.Interval)
	if err != nil {
		log.Error("invalid --interval", "err", err)
		return 1
	}

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}

	topic, messages, msgErr := buildMessages(flags)
	if msgErr != nil {
		log.Error("message generation failed", "err", msgErr)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Printf("Producing %d messages to %s...\n", flags.Count, topic)

	var failed int
	client.ProduceAll(ctx, messages, interval, func(r kafka.ProduceResult) {
		if r.Err != nil {
			fmt.Fprintf(os.Stderr, "  [%d/%d] FAIL: %v\n", r.Sequence, flags.Count, r.Err)
			failed++
		} else if flags.Verbose {
			fmt.Printf("  [%d/%d] OK key=%s latency=%s\n", r.Sequence, flags.Count, r.Key, r.Duration)
		}
	})

	fmt.Printf("\nDone: %d sent, %d failed\n", flags.Count-failed, failed)
	if failed > 0 {
		return 1
	}
	return 0
}

// buildMessages generates Kafka messages from either a schema file or random data.
func buildMessages(flags ProduceFlags) (string, []*kgo.Record, error) {
	topic := flags.Topic

	if flags.Schema != "" {
		schema, err := kafka.LoadTopicSchema(flags.Schema)
		if err != nil {
			return "", nil, err
		}
		if topic == "" {
			topic = schema.Topic
		}
		messages, err := kafka.GenerateSchemaMessages(schema, flags.Count)
		if err != nil {
			return "", nil, err
		}
		if topic != schema.Topic {
			for i := range messages {
				messages[i].Topic = topic
			}
		}
		return topic, messages, nil
	}

	if topic == "" {
		return "", nil, fmt.Errorf("--topic or --schema is required")
	}
	messages, err := kafka.GenerateMessages(topic, flags.Count)
	if err != nil {
		return "", nil, err
	}
	return topic, messages, nil
}
