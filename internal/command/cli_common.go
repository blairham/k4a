// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

const formatJSON = "json"

// FormatFlag is the --format flag shared by the commands that print records,
// spelled as search's: text for people, json for scripts.
type FormatFlag struct {
	Format string `long:"format" choice:"text" choice:"json" default:"text" description:"Output format: text or json"`
}

// JSON reports whether --format json was asked for.
func (f FormatFlag) JSON() bool { return f.Format == formatJSON }

// clusterFlagsHelp is the connection half of every cluster command's help.
const clusterFlagsHelp = `Config:
      --context=NAME       Named context from config file
      --config=FILE        Config file path (default: ~/.k4a/config.yaml)

Connection:
  -b, --brokers=BROKERS    Bootstrap servers, comma-separated
  -a, --auth=METHOD        Auth method: plaintext, tls, scram, scram-sha-256, plain, oauthbearer, mtls, iam
      --username=USER      SASL username
      --password=PASS      SASL password # pragma: allowlist secret
      --cert=FILE          Client certificate PEM (mTLS)
      --key=FILE           Client private key PEM (mTLS)
      --ca=FILE            CA certificate PEM
  -r, --region=REGION      AWS region for IAM auth (default: us-east-1)
  -p, --profile=PROFILE    AWS profile for IAM auth
  -k, --insecure           Skip TLS certificate verification (refused with mtls)
      --no-tls             Run a SASL method without TLS (credentials in cleartext)
      --oauth-token=TOKEN  OAUTHBEARER token (or --oauth-token-file, or --oauth-token-url
                           with --oauth-client-id/--oauth-client-secret/--oauth-scopes,
                           or --oauth-issuer to log in; see k4a login --help)
      --oauth-extensions=K=V,...  OAUTHBEARER SASL extensions
      --oauth-authzid=ID   OAUTHBEARER authorization ID`

// parseCmdArgs parses args into flags and expands their $VAR references.
// When done is true the caller returns code: help was printed, or the flags
// were invalid.
func parseCmdArgs(flags any, args []string) (rest []string, code int, done bool) {
	rest, err := goflags.NewParser(flags, goflags.Default).ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return nil, 0, true
		}
		log.Error("invalid flags", "err", err)
		return nil, 1, true
	}
	expandEnvFlags(flags)
	return rest, 0, false
}

// clusterCmdTimeout bounds one headless cluster command end to end.
const clusterCmdTimeout = 30 * time.Second

// withClient connects with cf and runs fn under a context that ends on
// interrupt or after clusterCmdTimeout, returning the command's exit code.
func withClient(cf *ConnectionFlags, fn func(context.Context, *kafka.Client) error) int {
	client, err := cf.BuildClient()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, timeoutCancel := context.WithTimeout(ctx, clusterCmdTimeout)
	defer timeoutCancel()

	if err := fn(ctx, client); err != nil {
		log.Error(err.Error())
		return 1
	}
	return 0
}

// orStdout is w, or stdout when a command was built without one.
func orStdout(w io.Writer) io.Writer {
	if w == nil {
		return os.Stdout
	}
	return w
}

// writeJSON writes v as indented JSON, the shape describe-topic and
// topic-size already emit.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// printer writes text output and keeps the first error, so a table can be
// printed line by line and the failure (a closed pipe) reported once.
type printer struct {
	tw  *tabwriter.Writer
	err error
}

// newPrinter prints to w; columns separated by tabs are aligned on done.
func newPrinter(w io.Writer) *printer {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	return &printer{tw: tw}
}

func (p *printer) printf(format string, a ...any) {
	if p.err == nil {
		_, p.err = fmt.Fprintf(p.tw, format, a...)
	}
}

// done flushes the aligned output and returns the first error.
func (p *printer) done() error {
	if p.err == nil {
		p.err = p.tw.Flush()
	}
	return p.err
}
