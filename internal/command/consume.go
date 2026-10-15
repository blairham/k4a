// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/timebound"
)

// ConsumeFlags defines flags for the consume command.
type ConsumeFlags struct {
	Topic string `short:"t" long:"topic"    env:"KAFKA_TOPIC" required:"true" description:"Topic to consume from"`
	Group string `short:"g" long:"group"    env:"KAFKA_GROUP"                 description:"Consumer group ID"                                              default:"k4a"`
	ConnectionFlags
	Verbose bool `short:"v" long:"verbose"                                    description:"Show keys and partition/offset metadata"`
	NoGroup bool `          long:"no-group"                                   description:"Consume without a consumer group (every partition, from start)"`
	// -1 is kafka.AllPartitions; int32 so go-flags rejects an out-of-range ID.
	Partition      int32  `long:"partition"      default:"-1"   description:"With --no-group or a seek, read only this partition"`
	Since          string `long:"since"                         description:"Start at the first record since a duration ago (1h, 7d) or an RFC3339 time; implies --no-group"`
	Offset         int64  `long:"offset"         default:"-1"   description:"Start every partition (or --partition) at this offset; implies --no-group"`
	FromBeginning  bool   `long:"from-beginning"                description:"Start at the log start; implies --no-group"`
	Decode         string `long:"decode"         default:"auto" description:"auto decodes schema-registry payloads when a registry is configured; raw never does"            choice:"auto" choice:"raw"`
	SchemaRegistry SchemaRegistryFlags
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
      --no-group          Consume without a consumer group (every partition, from start)
      --partition=N       With --no-group or a seek, read only partition N
      --from-beginning    Start at the log start
      --offset=N          Start each partition at offset N (clamped into its log)
      --since=WHEN        Start at the first record since WHEN: a duration ago
                          (30m, 24h, 7d, 2w) or an RFC3339 time

  A seek (--from-beginning, --offset, --since) reads without a consumer
  group, since a group starts at its committed offsets; giving one with
  --group is an error. Without a seek, a group starts at its commits and
  --no-group at the beginning, as before.
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
  -v, --verbose           Show keys and partition/offset metadata

Schema Registry (decodes Avro, Protobuf and JSON Schema keys and values):
      --decode=auto|raw              auto (default) decodes when a registry is
                                     configured; raw prints the bytes as before
      --schema-registry-url=URL      Registry URL (or the context's schema-registry.url)
      --schema-registry-username=U   Basic-auth username
      --schema-registry-password=P   Basic-auth password # pragma: allowlist secret
      --schema-registry-token=T      Bearer token (instead of username/password)
      --schema-registry-ca=FILE      CA certificate PEM for the registry

Examples:
  # Use current-context from config
  k4a consume -t my-topic

  # Use a specific context
  k4a consume --context staging -t my-topic

  # Direct connection
  k4a consume -b broker:9098 -t my-topic --auth iam
  k4a consume -b broker:9196 -t my-topic --username user --password pass -v

  # Seek: the last hour, or partition 3 from offset 1200
  k4a consume -t my-topic --since 1h
  k4a consume -t my-topic --partition 3 --offset 1200`
}

// Synopsis returns the one-line description.
func (c *ConsumeCommand) Synopsis() string {
	return "Consume messages from a Kafka topic"
}

// Run executes the consume command.
func (c *ConsumeCommand) Run(args []string) int {
	flags, groupSet, err := parseConsumeArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	seek, seeking, err := consumeSeek(flags, groupSet, time.Now())
	if err != nil {
		log.Error("invalid flags", "err", err)
		return 1
	}
	if seeking {
		flags.NoGroup = true
	}

	if err := validateConsumePartition(flags); err != nil {
		log.Error("invalid flags", "err", err)
		return 1
	}

	dec, err := newConsumeDecoder(flags)
	if err != nil {
		log.Error(err.Error())
		return 1
	}

	client, err := flags.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	group := flags.Group
	switch {
	case seeking:
		group = ""
		fmt.Printf("Consuming from %s (no group, %s, from %s)...\n", flags.Topic, partitionLabel(flags.Partition), seek)
	case flags.NoGroup:
		group = ""
		fmt.Printf("Consuming from %s (no group, %s)...\n", flags.Topic, partitionLabel(flags.Partition))
	default:
		fmt.Printf("Consuming from %s (group: %s)...\n", flags.Topic, flags.Group)
	}
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	count := 0
	onMessage := func(msg kafka.ConsumedMessage) {
		count++
		dec.decode(ctx, &msg)
		if flags.Verbose {
			fmt.Printf("[%d] partition=%d offset=%d key=%s%s\n%s\n\n",
				count, msg.Partition, msg.Offset, msg.Key, schemaSuffix(msg), msg.Value)
		} else {
			fmt.Printf("[%d] %s\n", count, msg.Value)
		}
	}
	if seeking {
		err = client.ConsumeBlockingFrom(ctx, flags.Topic, flags.Partition, seek, onMessage)
	} else {
		err = client.ConsumeBlocking(ctx, flags.Topic, group, flags.Partition, onMessage)
	}
	if err != nil {
		log.Error("consume failed", "err", err)
		return 1
	}

	fmt.Printf("\nConsumed %d messages.\n", count)
	return 0
}

// parseConsumeArgs parses args and reports whether --group was given on the
// command line, as opposed to left at its "k4a" default (or set by
// KAFKA_GROUP, which go-flags treats as a default too).
func parseConsumeArgs(args []string) (flags ConsumeFlags, groupSet bool, err error) {
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err = parser.ParseArgs(args); err != nil {
		return flags, false, err
	}
	expandEnvFlags(&flags)
	opt := parser.FindOptionByLongName("group")
	return flags, opt.IsSet() && !opt.IsSetDefault(), nil
}

// consumeSeek reads --from-beginning, --offset and --since. At most one may
// be given, and not with an explicit --group: a seek reads groupless, and
// silently dropping the group (or the seek) would read something else than
// was asked for. seeking reports whether any was given.
func consumeSeek(flags ConsumeFlags, groupSet bool, now time.Time) (seek kafka.Seek, seeking bool, err error) {
	n := 0
	if flags.FromBeginning {
		n++
	}
	if flags.Offset != -1 {
		n++
		if flags.Offset < 0 {
			return seek, false, fmt.Errorf("--offset must be 0 or more, got %d", flags.Offset)
		}
		seek = kafka.Seek{Mode: kafka.SeekOffset, Offset: flags.Offset}
	}
	if flags.Since != "" {
		n++
		t, perr := timebound.Parse(flags.Since, now)
		if perr != nil {
			return seek, false, fmt.Errorf("--since: %w", perr)
		}
		seek = kafka.Seek{Mode: kafka.SeekTime, Time: t}
	}
	switch {
	case n == 0:
		return kafka.Seek{}, false, nil
	case n > 1:
		return seek, false, errors.New("give one of --from-beginning, --offset and --since")
	case groupSet:
		return seek, false, errors.New("a seek reads without a consumer group (a group starts at its commits); drop --group")
	}
	return seek, true, nil
}

func partitionLabel(p int32) string {
	if p == kafka.AllPartitions {
		return "all partitions"
	}
	return fmt.Sprintf("partition %d", p)
}

// validateConsumePartition refuses a --partition that cannot be honored: a
// group assigns its own partitions, and IDs below -1 do not exist.
func validateConsumePartition(flags ConsumeFlags) error {
	if flags.Partition < kafka.AllPartitions {
		return fmt.Errorf("--partition must be a partition ID (0 or more), got %d", flags.Partition)
	}
	if flags.Partition != kafka.AllPartitions && !flags.NoGroup {
		return errors.New("--partition needs --no-group or a seek: a consumer group assigns its own partitions")
	}
	return nil
}
