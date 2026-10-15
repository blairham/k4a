// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
)

// ProduceFlags defines flags for the produce command.
type ProduceFlags struct {
	Topic        string   `short:"t" long:"topic"         env:"KAFKA_TOPIC" description:"Target topic (overrides the schema's topic)"`
	Schema       string   `short:"s" long:"schema"                          description:"YAML schema file for message generation"`
	Interval     string   `short:"i" long:"interval"                        description:"Delay between messages (default: 1s generated, 0 for --value/--stdin)"`
	Key          string   `          long:"record-key"                      description:"Record key (with --value); --key is the mTLS key file"`
	Value        string   `          long:"value"                           description:"Record value: send this instead of generated messages"`
	KeySeparator string   `          long:"key-separator"                   description:"With --stdin, split each line into key and value at the first SEP"`
	Headers      []string `short:"H" long:"header"                          description:"Record header name=value (repeatable)"`
	ConnectionFlags
	Count   int  `short:"c" long:"count"                           description:"Number of messages (default: 10 generated, 1 with --value)"`
	Stdin   bool `          long:"stdin"                           description:"Read records from stdin, one per line"`
	Verbose bool `short:"v" long:"verbose"                         description:"Verbose output"`
}

// ProduceCommand implements the "produce" CLI command.
type ProduceCommand struct {
	In io.Reader // stdin when nil; tests replace it
}

// Help returns the detailed help text.
func (c *ProduceCommand) Help() string {
	return `Usage: k4a produce [options]

  Produce messages to a Kafka topic: generated ones (random payloads, or
  realistic ones from a --schema YAML file), one record you write with
  --value, or your own records read from stdin with --stdin.

  --value sends the value (with --record-key and --header) --count times, default
  once. --stdin reads one record per line: the whole line is the value, or,
  with --key-separator SEP, the text before the first SEP is the key and
  the rest the value (a line without SEP is an error). Empty lines are
  skipped and a line may be up to 1 MiB. --header applies to every record,
  generated ones included. --value and --stdin send without a delay unless
  --interval is given.

  Connection can be specified via config file (~/.k4a/config.yaml) or flags.
  When both are present, flags override config values.

Config:
      --context=NAME      Named context from config file
      --config=FILE       Config file path (default: ~/.k4a/config.yaml)

Options:
  -b, --brokers=BROKERS   Bootstrap servers, comma-separated
  -t, --topic=TOPIC       Target topic (overrides the schema's topic if both are given)
  -s, --schema=FILE       YAML schema file for realistic message generation
  -a, --auth=METHOD       Auth method: plaintext, tls, scram, scram-sha-256, plain, oauthbearer, mtls, iam
  -c, --count=N           Number of messages (default: 10; 1 with --value)
  -i, --interval=DUR      Delay between messages (default: 1s; 0 with --value/--stdin)
      --record-key=KEY    Record key (with --value; --key is the mTLS key file)
      --value=VALUE       Send this value instead of generated messages
  -H, --header=NAME=VAL   Record header (repeatable)
      --stdin             Read records from stdin, one per line
      --key-separator=SEP With --stdin, split each line into key SEP value
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
  -v, --verbose           Verbose output

Examples:
  # Use current-context from config
  k4a produce -t test-topic -c 5

  # Use a specific context
  k4a produce --context staging -t test-topic

  # Direct connection
  k4a produce -b broker:9196 -t test-topic --username user --password pass
  k4a produce -b broker:9098 -t test-topic --auth iam -c 5 -v
  k4a produce -b broker:9196 -s schemas/example.orders.v1.yaml -c 20

  # Your own records
  k4a produce -t orders --record-key order-42 --value '{"id":42}' -H source=cli
  printf 'order-1\t{"id":1}\norder-2\t{"id":2}\n' | \
    k4a produce -t orders --stdin --key-separator "$(printf '\t')"`
}

// Synopsis returns the one-line description.
func (c *ProduceCommand) Synopsis() string {
	return "Produce messages to a Kafka topic (generated, --value or --stdin)"
}

// Run executes the produce command.
func (c *ProduceCommand) Run(args []string) int {
	var flags ProduceFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	expandEnvFlags(&flags)

	if err := flags.checkWritable("produce"); err != nil {
		log.Error(err.Error())
		return 1
	}

	topic, messages, msgErr := buildRecords(&flags, c.In)
	if msgErr != nil {
		log.Error("building messages failed", "err", msgErr)
		return 1
	}
	interval, err := produceInterval(flags)
	if err != nil {
		log.Error("invalid --interval", "err", err)
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

	fmt.Printf("Producing %d messages to %s...\n", len(messages), topic)

	tally := produceTally{total: len(messages)}
	client.ProduceAll(ctx, messages, interval, func(r kafka.ProduceResult) {
		tally.record(r)
		if r.Err != nil {
			fmt.Fprintf(os.Stderr, "  [%d/%d] FAIL: %v\n", r.Sequence, len(messages), r.Err)
		} else if flags.Verbose {
			fmt.Printf("  [%d/%d] OK key=%s latency=%s\n", r.Sequence, len(messages), r.Key, r.Duration)
		}
	})

	fmt.Printf("\n%s\n", tally.summary())
	if !tally.complete() {
		return 1
	}
	return 0
}

// produceTally counts what ProduceAll actually reported. Deriving "sent"
// as total minus failed counted every record a Ctrl+C skipped as sent.
type produceTally struct {
	total, sent, failed int
}

func (t *produceTally) record(r kafka.ProduceResult) {
	if r.Err != nil {
		t.failed++
	} else {
		t.sent++
	}
}

// complete reports whether every record was produced successfully.
func (t *produceTally) complete() bool {
	return t.sent == t.total
}

func (t *produceTally) summary() string {
	s := fmt.Sprintf("Done: %d sent, %d failed", t.sent, t.failed)
	if unsent := t.total - t.sent - t.failed; unsent > 0 {
		s += fmt.Sprintf(", %d not sent (interrupted)", unsent)
	}
	return s
}

// checkRecordSource refuses flag combinations that would silently drop one
// of the record sources (--value, --stdin, generation) or their options.
func checkRecordSource(flags *ProduceFlags) error {
	explicit := flags.Value != "" || flags.Stdin
	switch {
	case flags.Value != "" && flags.Stdin:
		return errors.New("give --value or --stdin, not both")
	case explicit && flags.Schema != "":
		return errors.New("--schema generates messages; it cannot be combined with --value or --stdin")
	case flags.Key != "" && flags.Value == "":
		return errors.New("--record-key needs --value (with --stdin, use --key-separator)")
	case flags.KeySeparator != "" && !flags.Stdin:
		return errors.New("--key-separator applies only to --stdin")
	case flags.Stdin && flags.Count != 0:
		return errors.New("--count does not apply to --stdin; every line is one record")
	case explicit && flags.Topic == "":
		return errors.New("--topic is required with --value or --stdin")
	}
	return nil
}

// buildRecords builds the records from --value, --stdin or generation.
func buildRecords(flags *ProduceFlags, stdin io.Reader) (string, []*kgo.Record, error) {
	if err := checkRecordSource(flags); err != nil {
		return "", nil, err
	}
	headers, err := kafka.ParseHeaders(flags.Headers)
	if err != nil {
		return "", nil, err
	}

	switch {
	case flags.Stdin:
		if stdin == nil {
			stdin = os.Stdin
		}
		records, readErr := kafka.ReadLineRecords(stdin, flags.Topic, flags.KeySeparator, headers)
		if readErr != nil {
			return "", nil, fmt.Errorf("reading stdin: %w", readErr)
		}
		if len(records) == 0 {
			return "", nil, errors.New("stdin held no records")
		}
		return flags.Topic, records, nil
	case flags.Value != "":
		count := flags.Count
		if count == 0 {
			count = 1
		}
		return flags.Topic, kafka.CustomRecords(flags.Topic, flags.Key, flags.Value, headers, count), nil
	}

	if flags.Count == 0 {
		flags.Count = defaultGeneratedCount
	}
	topic, records, err := buildMessages(*flags)
	if err != nil {
		return "", nil, err
	}
	for _, r := range records {
		r.Headers = append(r.Headers, headers...)
	}
	return topic, records, nil
}

// defaultGeneratedCount is --count for generated messages when not given.
const defaultGeneratedCount = 10

// produceInterval is --interval, defaulting to a 1s pace for generated
// messages (a demo you can watch) and none for records you supplied.
func produceInterval(flags ProduceFlags) (time.Duration, error) {
	switch {
	case flags.Interval != "":
		return time.ParseDuration(flags.Interval)
	case flags.Value != "" || flags.Stdin:
		return 0, nil
	default:
		return time.Second, nil
	}
}

// buildMessages generates Kafka messages from either a schema file or random data.
func buildMessages(flags ProduceFlags) (string, []*kgo.Record, error) {
	topic := flags.Topic

	if flags.Schema != "" {
		schema, err := kafka.LoadTopicSchema(flags.Schema)
		if err != nil {
			return "", nil, err
		}
		if topic == "" {
			topic = schema.Topic
		}
		messages, err := kafka.GenerateSchemaMessages(schema, flags.Count)
		if err != nil {
			return "", nil, err
		}
		if topic != schema.Topic {
			for i := range messages {
				messages[i].Topic = topic
			}
		}
		return topic, messages, nil
	}

	if topic == "" {
		return "", nil, fmt.Errorf("--topic or --schema is required")
	}
	messages, err := kafka.GenerateMessages(topic, flags.Count)
	if err != nil {
		return "", nil, err
	}
	return topic, messages, nil
}
