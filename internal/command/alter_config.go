// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

// alterConfigTimeout bounds the whole command. Each topic costs one
// AlterConfigs round trip, matching describe-topic's budget.
const alterConfigTimeout = 30 * time.Second

// AlterConfigFlags defines flags for the alter-config command.
type AlterConfigFlags struct {
	Set []string `long:"set"    description:"Set a topic config as key=value (repeatable)" short:"s"`
	Del []string `long:"delete" description:"Remove a topic-level override (repeatable)"   short:"d"`
	ConnectionFlags
}

// AlterConfigCmd implements the "alter-config" CLI command.
type AlterConfigCmd struct{}

// configOp is one resolved mutation to apply to every named topic.
type configOp struct {
	Key    string
	Value  string
	Delete bool
}

// Help returns the detailed help text.
func (c *AlterConfigCmd) Help() string {
	return `Usage: k4a alter-config [options] TOPIC...

  Set or remove topic-level configuration overrides (AlterConfigs).

  --set pins a key on the topic. --delete removes the topic-level override so
  the key falls back to the broker default — which is usually what you want
  when undoing a hand-set value, because it restores "inherited" rather than
  freezing today's default as a new override.

  Both flags are repeatable and every operation is applied to every TOPIC.
  Deletes are applied before sets, so "--delete k --set k=v" ends with v.

  Changes take effect immediately for subsequent requests: this is a dynamic
  config change, with no broker restart, partition reassignment or leader
  election. Use "k4a describe-topic" to read the result back.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME       Named context from config file
      --config=FILE        Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS    Bootstrap servers, comma-separated
  -s, --set=KEY=VALUE      Set a topic config (repeatable)
  -d, --delete=KEY         Remove a topic-level override (repeatable)
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
  # Drop a hand-set override so the topic inherits the broker default again
  k4a alter-config --context production --delete min.insync.replicas \
    example.ingest.markets.v1 example.ingest.events.v1

  # Pin a retention on one topic
  k4a alter-config --context production --set retention.ms=86400000 quotes-east-book

  # Read the result back
  k4a describe-topic --context production example.ingest.markets.v1`
}

// Synopsis returns the one-line description.
func (c *AlterConfigCmd) Synopsis() string {
	return "Set or remove topic-level configuration overrides"
}

// Run executes the alter-config command.
func (c *AlterConfigCmd) Run(args []string) int {
	var flags AlterConfigFlags
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

	ops, err := parseConfigOps(flags.Set, flags.Del)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	if len(ops) == 0 {
		log.Error("nothing to do: pass at least one --set or --delete")
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
	ctx, timeoutCancel := context.WithTimeout(ctx, alterConfigTimeout)
	defer timeoutCancel()

	failed := false
	for _, topic := range topics {
		for _, op := range ops {
			// Keep going on failure: one rejected key must not silently strand
			// the remaining topics in a half-applied state with no report.
			if !applyConfigOp(ctx, client, topic, op) {
				failed = true
			}
		}
	}

	if failed {
		return 1
	}
	return 0
}

// applyConfigOp applies one operation to one topic and reports the outcome,
// returning false if the broker rejected it.
func applyConfigOp(ctx context.Context, client *kafka.Client, topic string, op configOp) bool {
	var err error
	if op.Delete {
		err = client.DeleteTopicConfigOverride(ctx, topic, op.Key)
	} else {
		err = client.AlterTopicConfig(ctx, topic, op.Key, op.Value)
	}
	if err != nil {
		log.Error("altering config failed", "topic", topic, "key", op.Key, "err", err)
		return false
	}
	if op.Delete {
		fmt.Printf("%s: removed override %s (now inherited)\n", topic, op.Key)
	} else {
		fmt.Printf("%s: set %s=%s\n", topic, op.Key, op.Value)
	}
	return true
}

// parseConfigOps turns the repeatable --delete and --set flags into an ordered
// operation list. Deletes come first so that "--delete k --set k=v" is a
// rewrite rather than a race between two operations on the same key.
func parseConfigOps(sets, dels []string) ([]configOp, error) {
	ops := make([]configOp, 0, len(sets)+len(dels))

	for _, key := range dels {
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("--delete requires a config key")
		}
		ops = append(ops, configOp{Key: strings.TrimSpace(key), Delete: true})
	}

	for _, kv := range sets {
		key, value, found := strings.Cut(kv, "=")
		key = strings.TrimSpace(key)
		if !found || key == "" {
			return nil, fmt.Errorf("--set %q is not in key=value form", kv)
		}
		ops = append(ops, configOp{Key: key, Value: value})
	}

	return ops, nil
}
