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

	"github.com/blairham/k4a/internal/kafka"
)

const createTopicTimeout = 15 * time.Second

// CreateTopicCmdFlags defines flags for the create-topic command.
type CreateTopicCmdFlags struct {
	Topic string `short:"t" long:"topic"              env:"KAFKA_TOPIC" required:"true" description:"Topic name to create"`
	ConnectionFlags
	Partitions        int `short:"n" long:"partitions"                                           description:"Number of partitions" default:"3"`
	ReplicationFactor int `          long:"replication-factor"                                   description:"Replication factor"   default:"3"`
}

// CreateTopicCmd implements the "create-topic" CLI command.
type CreateTopicCmd struct{}

// Help returns the detailed help text.
func (c *CreateTopicCmd) Help() string {
	return `Usage: k4a create-topic [options]

  Create a Kafka topic.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME        Named context from config file
      --config=FILE         Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS       Bootstrap servers, comma-separated
  -t, --topic=TOPIC           Topic name to create (required)
  -n, --partitions=N          Number of partitions (default: 3)
      --replication-factor=N  Replication factor (default: 3)
  -a, --auth=METHOD           Auth method: plaintext, tls, scram, mtls, iam
      --username=USER         SASL/SCRAM username
      --password=PASS         SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE             Client certificate PEM (mTLS)
      --key=FILE              Client private key PEM (mTLS)
      --ca=FILE               CA certificate PEM
  -r, --region=REGION         AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE       AWS profile for IAM auth
  -k, --insecure              Skip TLS certificate verification

Examples:
  # Use current-context from config
  k4a create-topic -t my-topic

  # Use a specific context
  k4a create-topic --context staging -t my-topic -n 6

  # Direct connection
  k4a create-topic -b broker:9098 -t my-topic --auth iam
  k4a create-topic -b broker:9196 -t my-topic -n 6 --replication-factor 3`
}

// Synopsis returns the one-line description.
func (c *CreateTopicCmd) Synopsis() string {
	return "Create a Kafka topic"
}

// Run executes the create-topic command.
func (c *CreateTopicCmd) Run(args []string) int {
	var flags CreateTopicCmdFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, createTopicTimeout)
	defer timeoutCancel()

	fmt.Printf("Creating topic %q (%d partitions, replication factor %d)...\n",
		flags.Topic, flags.Partitions, flags.ReplicationFactor)

	created, err := client.CreateTopic(ctx, kafka.CreateTopicConfig{
		Topic:             flags.Topic,
		NumPartitions:     flags.Partitions,
		ReplicationFactor: flags.ReplicationFactor,
	})
	if err != nil {
		log.Error("create topic failed", "err", err)
		return 1
	}

	if created {
		fmt.Println("Topic created successfully.")
	} else {
		fmt.Println("Topic already exists.")
	}
	return 0
}
