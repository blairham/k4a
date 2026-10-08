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

const listACLsTimeout = 15 * time.Second

// ListACLsCmdFlags defines flags for the list-acls command.
type ListACLsCmdFlags struct {
	ConnectionFlags
}

// ListACLsCmd implements the "list-acls" CLI command.
type ListACLsCmd struct{}

// Help returns the detailed help text.
func (c *ListACLsCmd) Help() string {
	return `Usage: k4a list-acls [options]

  List all Kafka ACL bindings in the cluster.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, mtls, iam
      --username=USER     SASL/SCRAM username
      --password=PASS     SASL/SCRAM password
      --cert=FILE         Client certificate PEM (mTLS)
      --key=FILE          Client private key PEM (mTLS)
      --ca=FILE           CA certificate PEM
  -r, --region=REGION     AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE   AWS profile for IAM auth
  -k, --insecure          Skip TLS certificate verification

Examples:
  # Use current-context from config
  k4a list-acls

  # Use a specific context
  k4a list-acls --context staging

  # Direct connection
  k4a list-acls -b broker:9098 --auth iam`
}

// Synopsis returns the one-line description.
func (c *ListACLsCmd) Synopsis() string {
	return "List all Kafka ACL bindings"
}

// Run executes the list-acls command.
func (c *ListACLsCmd) Run(args []string) int {
	var flags ListACLsCmdFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, listACLsTimeout)
	defer timeoutCancel()

	entries, err := client.FetchACLs(ctx)
	if err != nil {
		log.Error("list ACLs failed", "err", err)
		return 1
	}

	if len(entries) == 0 {
		fmt.Println("No ACLs found.")
		return 0
	}

	fmt.Printf("%-30s  %-14s  %-30s  %-10s  %-6s  %-10s  %s\n",
		"PRINCIPAL", "RESOURCE TYPE", "RESOURCE NAME", "PATTERN", "HOST", "OPERATION", "PERMISSION")
	for _, e := range entries {
		fmt.Printf("%-30s  %-14s  %-30s  %-10s  %-6s  %-10s  %s\n",
			e.Principal, e.ResourceType, e.ResourceName, e.PatternType,
			e.Host, e.Operation, e.Permission)
	}

	return 0
}
