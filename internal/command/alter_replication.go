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

const alterReplicationTimeout = 60 * time.Second

// AlterReplicationCmdFlags defines flags for the alter-replication command.
type AlterReplicationCmdFlags struct {
	Topic string `short:"t" long:"topic"              env:"KAFKA_TOPIC" required:"true" description:"Topic name"`
	ConnectionFlags
	ReplicationFactor int `          long:"replication-factor"                   required:"true" description:"New replication factor"`
}

// AlterReplicationCmd implements the "alter-replication" CLI command.
type AlterReplicationCmd struct{}

// Help returns the detailed help text.
func (c *AlterReplicationCmd) Help() string {
	return `Usage: k4a alter-replication [options]

  Change the replication factor of a Kafka topic.

  This uses the AlterPartitionReassignments API to add or remove replicas
  for every partition of the topic. New replicas are distributed evenly
  across available brokers.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME        Named context from config file
      --config=FILE         Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS          Bootstrap servers, comma-separated
  -t, --topic=TOPIC              Topic name (required)
      --replication-factor=N     New replication factor (required)
  -a, --auth=METHOD              Auth method: plaintext, tls, scram, mtls, iam
      --username=USER            SASL/SCRAM username
      --password=PASS            SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE                Client certificate PEM (mTLS)
      --key=FILE                 Client private key PEM (mTLS)
      --ca=FILE                  CA certificate PEM
  -r, --region=REGION            AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE          AWS profile for IAM auth
  -k, --insecure                 Skip TLS certificate verification

Examples:
  # Increase replication factor to 3
  k4a alter-replication -t my-topic --replication-factor 3

  # Using a specific context
  k4a alter-replication --context production -t my-topic --replication-factor 3`
}

// Synopsis returns the one-line description.
func (c *AlterReplicationCmd) Synopsis() string {
	return "Change the replication factor of a topic"
}

// Run executes the alter-replication command.
func (c *AlterReplicationCmd) Run(args []string) int {
	var flags AlterReplicationCmdFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, alterReplicationTimeout)
	defer timeoutCancel()

	fmt.Printf("Changing replication factor for %q to %d...\n",
		flags.Topic, flags.ReplicationFactor)

	if err := client.ChangeReplicationFactor(ctx, flags.Topic, flags.ReplicationFactor); err != nil {
		log.Error("alter replication failed", "err", err)
		return 1
	}

	fmt.Println("Partition reassignment submitted successfully.")
	fmt.Println("Use the TUI or kafka tools to monitor reassignment progress.")
	return 0
}
