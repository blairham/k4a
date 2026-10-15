// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"io"
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
	Yes bool `short:"y" long:"yes"                                     description:"Delete without asking for confirmation"`
}

// DeleteTopicCmd implements the "delete-topic" CLI command.
type DeleteTopicCmd struct {
	// In, Out and Interactive default to stdin, stdout and a terminal check
	// on stdin; tests replace them to drive the confirmation prompt.
	In          io.Reader
	Out         io.Writer
	Interactive func() bool
}

// Help returns the detailed help text.
func (c *DeleteTopicCmd) Help() string {
	return `Usage: k4a delete-topic [options]

  Delete a Kafka topic. Asks for confirmation first unless --yes is given;
  without a terminal on stdin, --yes is required. Refused when ui.readonly
  is set.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -t, --topic=TOPIC       Topic name to delete (required)
  -y, --yes               Skip the confirmation prompt (required when stdin is not a terminal)
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, scram-sha-256, plain, oauthbearer, mtls, iam
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password # pragma: allowlist secret
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification
      --no-tls            Run a SASL method without TLS (credentials in cleartext)
      --oauth-token=TOKEN OAUTHBEARER token (or --oauth-token-file, or --oauth-token-url
                          with --oauth-client-id/--oauth-client-secret/--oauth-scopes)

Examples:
  # Use current-context from config
  k4a delete-topic -t my-topic

  # Use a specific context
  k4a delete-topic --context staging -t my-topic

  # Direct connection
  k4a delete-topic -b broker:9098 -t my-topic --auth iam
  k4a delete-topic -b broker:9196 -t my-topic --username user --password pass

  # Non-interactive (scripts, CI)
  k4a delete-topic --context staging -t my-topic --yes`
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

	if err := flags.checkWritable("delete-topic"); err != nil {
		log.Error(err.Error())
		return 1
	}

	if !flags.Yes {
		if err := c.confirm(flags.Topic); err != nil {
			log.Error("topic not deleted", "topic", flags.Topic, "err", err)
			return 1
		}
	}

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

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

func (c *DeleteTopicCmd) confirm(topic string) error {
	in, out, interactive := c.In, c.Out, c.Interactive
	if in == nil {
		in = os.Stdin
	}
	if out == nil {
		out = os.Stdout
	}
	if interactive == nil {
		interactive = stdinIsTerminal
	}
	return confirm(in, out, interactive(), fmt.Sprintf("Delete topic %q? This cannot be undone.", topic))
}
