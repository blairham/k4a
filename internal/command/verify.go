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

const verifyTimeout = 15 * time.Second

// VerifyFlags defines flags for the verify command.
type VerifyFlags struct {
	ConnectionFlags
}

// VerifyCommand implements the "verify" CLI command.
type VerifyCommand struct{}

// Help returns the detailed help text.
func (c *VerifyCommand) Help() string {
	return `Usage: k4a verify [options]

  Connect to a Kafka cluster and fetch metadata to verify connectivity.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
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
  k4a verify

  # Use a specific context
  k4a verify --context staging

  # Direct connection
  k4a verify -b broker:9098 --auth iam
  k4a verify -b broker:9196 --username user --password pass`
}

// Synopsis returns the one-line description.
func (c *VerifyCommand) Synopsis() string {
	return "Verify connectivity to a Kafka cluster"
}

// Run executes the verify command.
func (c *VerifyCommand) Run(args []string) int {
	var flags VerifyFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, verifyTimeout)
	defer timeoutCancel()

	fmt.Println("Connecting...")

	latency, err := client.Ping(ctx)
	if err != nil {
		log.Error("connection failed", "err", err)
		return 1
	}

	cluster, err := client.FetchCluster(ctx)
	if err != nil {
		log.Error("metadata fetch failed", "err", err)
		return 1
	}

	fmt.Printf("\nConnection successful! (latency: %s)\n\n", latency.Round(time.Millisecond))
	fmt.Printf("Cluster ID:  %s\n", cluster.ClusterID)
	fmt.Printf("Controller:  %d\n", cluster.ControllerID)
	fmt.Printf("Brokers:     %d\n", len(cluster.Brokers))
	for _, b := range cluster.Brokers {
		fmt.Printf("  - %s:%d (ID: %d)\n", b.Host, b.Port, b.ID)
	}

	return 0
}
