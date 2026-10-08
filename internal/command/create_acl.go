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

const createACLTimeout = 15 * time.Second

// CreateACLCmdFlags defines flags for the create-acl command.
type CreateACLCmdFlags struct {
	ResourceType string `long:"resource-type" required:"true" description:"Resource type: topic, group, cluster, transactionalid"`
	ResourceName string `long:"resource-name" required:"true" description:"Resource name"`
	Principal    string `long:"principal"     required:"true" description:"Principal (e.g. User:alice)"`
	Operation    string `long:"operation"     required:"true" description:"Operation: all, read, write, create, delete, alter, describe"`
	PatternType  string `long:"pattern-type"                  description:"Pattern type: literal, prefixed"                              default:"literal"`
	Host         string `long:"host"                          description:"Host (* for all hosts)"                                       default:"*"`
	Permission   string `long:"permission"                    description:"Permission type: allow, deny"                                 default:"allow"`
	ConnectionFlags
}

// CreateACLCmd implements the "create-acl" CLI command.
type CreateACLCmd struct{}

// Help returns the detailed help text.
func (c *CreateACLCmd) Help() string {
	return `Usage: k4a create-acl [options]

  Create a Kafka ACL entry.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME        Named context from config file
      --config=FILE         Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS       Bootstrap servers, comma-separated
      --resource-type=TYPE    Resource type: topic, group, cluster, transactionalid (required)
      --resource-name=NAME    Resource name (required)
      --principal=PRINCIPAL   Principal, e.g. User:alice (required)
      --operation=OP          Operation: all, read, write, create, delete, alter, describe (required)
      --pattern-type=TYPE     Pattern type: literal, prefixed (default: literal)
      --host=HOST             Host, * for all (default: *)
      --permission=PERM       Permission: allow, deny (default: allow)
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
  k4a create-acl --resource-type topic --resource-name my-topic \
    --principal User:alice --operation read

  # Use a specific context
  k4a create-acl --context staging \
    --resource-type group --resource-name my-group \
    --principal User:bob --operation all

  # Direct connection
  k4a create-acl -b broker:9098 --auth iam \
    --resource-type topic --resource-name my-topic \
    --principal User:alice --operation read`
}

// Synopsis returns the one-line description.
func (c *CreateACLCmd) Synopsis() string {
	return "Create a Kafka ACL entry"
}

// Run executes the create-acl command.
func (c *CreateACLCmd) Run(args []string) int {
	var flags CreateACLCmdFlags
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

	ctx, timeoutCancel := context.WithTimeout(ctx, createACLTimeout)
	defer timeoutCancel()

	fmt.Printf("Creating ACL: %s %s on %s %q (principal: %s, host: %s)...\n",
		flags.Permission, flags.Operation, flags.ResourceType, flags.ResourceName,
		flags.Principal, flags.Host)

	err = client.CreateACL(ctx, kafka.CreateACLConfig{
		ResourceType: flags.ResourceType,
		ResourceName: flags.ResourceName,
		PatternType:  flags.PatternType,
		Principal:    flags.Principal,
		Host:         flags.Host,
		Operation:    flags.Operation,
		Permission:   flags.Permission,
	})
	if err != nil {
		log.Error("create ACL failed", "err", err)
		return 1
	}

	fmt.Println("ACL created successfully.")
	return 0
}
