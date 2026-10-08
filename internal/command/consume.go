// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

// ConsumeFlags defines flags for the consume command.
type ConsumeFlags struct {
	Topic string `short:"t" long:"topic"    env:"KAFKA_TOPIC" required:"true" description:"Topic to consume from"`
	Group string `short:"g" long:"group"    env:"KAFKA_GROUP"                 description:"Consumer group ID"                                          default:"k4a"`
	ConnectionFlags
	Verbose bool `short:"v" long:"verbose"                                    description:"Show keys and partition/offset metadata"`
	NoGroup bool `          long:"no-group"                                   description:"Consume without a consumer group (partition 0, from start)"`
}

// ConsumeCommand implements the "consume" CLI command.
type ConsumeCommand struct{}

// Help returns the detailed help text.
func (c *ConsumeCommand) Help() string {
	return `Usage: k4a consume [options]

  Consume messages from a Kafka topic. Press Ctrl+C to stop.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -t, --topic=TOPIC       Topic to consume from (required)
  -g, --group=GROUP       Consumer group ID (default: k4a)
      --no-group          Consume without a consumer group (partition 0, from start)
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, mtls, iam
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification
  -v, --verbose           Show keys and partition/offset metadata

Examples:
  # Use current-context from config
  k4a consume -t my-topic

  # Use a specific context
  k4a consume --context staging -t my-topic

  # Direct connection
  k4a consume -b broker:9098 -t my-topic --auth iam
  k4a consume -b broker:9196 -t my-topic --username user --password pass -v`
}

// Synopsis returns the one-line description.
func (c *ConsumeCommand) Synopsis() string {
	return "Consume messages from a Kafka topic"
}

// Run executes the consume command.
func (c *ConsumeCommand) Run(args []string) int {
	var flags ConsumeFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	group := flags.Group
	if flags.NoGroup {
		group = ""
		fmt.Printf("Consuming from %s (no group, partition 0)...\n", flags.Topic)
	} else {
		fmt.Printf("Consuming from %s (group: %s)...\n", flags.Topic, flags.Group)
	}
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	count := 0
	err = client.ConsumeBlocking(ctx, flags.Topic, group, func(msg kafka.ConsumedMessage) {
		count++
		if flags.Verbose {
			fmt.Printf("[%d] partition=%d offset=%d key=%s\n%s\n\n",
				count, msg.Partition, msg.Offset, msg.Key, msg.Value)
		} else {
			fmt.Printf("[%d] %s\n", count, msg.Value)
		}
	})
	if err != nil {
		log.Error("consume failed", "err", err)
		return 1
	}

	fmt.Printf("\nConsumed %d messages.\n", count)
	return 0
}
