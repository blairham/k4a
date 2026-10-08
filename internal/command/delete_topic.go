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
)

const deleteTopicTimeout = 15 * time.Second

// DeleteTopicCmdFlags defines flags for the delete-topic command.
type DeleteTopicCmdFlags struct {
	Topic string `short:"t" long:"topic" env:"KAFKA_TOPIC" required:"true" description:"Topic name to delete"`
	ConnectionFlags
}

// DeleteTopicCmd implements the "delete-topic" CLI command.
type DeleteTopicCmd struct{}

// Help returns the detailed help text.
func (c *DeleteTopicCmd) Help() string {
	return `Usage: k4a delete-topic [options]

  Delete a Kafka topic.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -t, --topic=TOPIC       Topic name to delete (required)
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
  # Use current-context from config
  k4a delete-topic -t my-topic

  # Use a specific context
  k4a delete-topic --context staging -t my-topic

  # Direct connection
  k4a delete-topic -b broker:9098 -t my-topic --auth iam
  k4a delete-topic -b broker:9196 -t my-topic --username user --password pass`
}

// Synopsis returns the one-line description.
func (c *DeleteTopicCmd) Synopsis() string {
	return "Delete a Kafka topic"
}

// Run executes the delete-topic command.
func (c *DeleteTopicCmd) Run(args []string) int {
	var flags DeleteTopicCmdFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, deleteTopicTimeout)
	defer timeoutCancel()

	fmt.Printf("Deleting topic %q...\n", flags.Topic)

	deleted, err := client.DeleteTopic(ctx, flags.Topic)
	if err != nil {
		log.Error("delete topic failed", "err", err)
		return 1
	}

	if deleted {
		fmt.Println("Topic deleted successfully.")
	} else {
		fmt.Println("Topic does not exist.")
	}
	return 0
}
