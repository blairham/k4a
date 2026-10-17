// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blairham/k4a/internal/archive"
	"github.com/blairham/k4a/internal/searchcli"
)

const archiveTestConfig = `current-context: prod
contexts:
  prod:
    brokers: localhost:9092
    profile: cluster-profile
    region: us-west-2
    archive:
      workgroup: k4a
      confirm-bytes: 1KiB
      max-bytes: 1MiB
      topics:
        orders:
          database: kafka_archive
          table: orders
          columns: {value: value, timestamp: ts, partition: p, offset: o}
          time-partitions:
            - {column: dt, format: "{yyyy}-{MM}-{dd}-{HH}"}
          location: s3://arch/orders/dt={yyyy}-{MM}-{dd}-{HH}/
        nolocation:
          database: kafka_archive
          table: nolocation
          columns: {value: value, timestamp: ts}
          time-partitions:
            - {column: dt, format: "{yyyy}-{MM}-{dd}"}
  lake:
    brokers: localhost:9092
    profile: cluster-profile
    archive:
      profile: lake-profile
      region: eu-west-1
      topics:
        orders:
          database: db
          table: t
          columns: {value: v, timestamp: ts}
          time-partitions:
            - {column: dt, format: "{yyyy}-{MM}-{dd}"}
`

// archiveEnv writes the config under t.TempDir and points HOME and every
// AWS file at it, so nothing here can read the developer's ~/.aws or ~/.k4a.
func archiveEnv(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(cfg, []byte(archiveTestConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "aws-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "aws-credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	return cfg
}

type cmdFakeAthena struct {
	started *athena.StartQueryExecutionInput
	// valueOnly returns rows for a table with only value and timestamp.
	valueOnly bool
}

func (f *cmdFakeAthena) StartQueryExecution(
	_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options),
) (*athena.StartQueryExecutionOutput, error) {
	f.started = in
	return &athena.StartQueryExecutionOutput{QueryExecutionId: aws.String("q-7")}, nil
}

func (f *cmdFakeAthena) GetQueryExecution(
	context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options),
) (*athena.GetQueryExecutionOutput, error) {
	return &athena.GetQueryExecutionOutput{QueryExecution: &types.QueryExecution{
		Status:     &types.QueryExecutionStatus{State: types.QueryExecutionStateSucceeded},
		Statistics: &types.QueryExecutionStatistics{DataScannedInBytes: aws.Int64(512)},
	}}, nil
}

func (f *cmdFakeAthena) GetQueryResults(
	context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options),
) (*athena.GetQueryResultsOutput, error) {
	cell := func(s string) types.Datum { return types.Datum{VarCharValue: aws.String(s)} }
	if f.valueOnly {
		return &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{Rows: []types.Row{
			{Data: []types.Datum{cell("value"), cell("ts")}},
			{Data: []types.Datum{cell(`order-41`), cell("1792148400000")}},
		}}}, nil
	}
	return &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{Rows: []types.Row{
		{Data: []types.Datum{cell("value"), cell("ts"), cell("p"), cell("o")}},
		{Data: []types.Datum{cell(`{"id":"order-41"}`), cell("1792148400000"), cell("2"), cell("40")}},
	}}}, nil
}

func (f *cmdFakeAthena) StopQueryExecution(
	context.Context, *athena.StopQueryExecutionInput, ...func(*athena.Options),
) (*athena.StopQueryExecutionOutput, error) {
	return &athena.StopQueryExecutionOutput{}, nil
}

type cmdFakeS3 struct{ size int64 }

func (f cmdFakeS3) ListObjectsV2(
	context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{Contents: []s3types.Object{{Size: aws.Int64(f.size)}}}, nil
}

type archiveHarness struct {
	athena          *cmdFakeAthena
	gotProfile      string
	gotRegion       string
	promptAnswer    string
	stdout          bytes.Buffer
	stderr          bytes.Buffer
	promptQuestions bytes.Buffer
	sizePerPrefix   int64
	clientsBuilt    bool
	promptTerminal  bool
}

func (h *archiveHarness) deps() archiveDeps {
	return archiveDeps{
		newClients: func(_ context.Context, profile, region string) (archive.AthenaAPI, archive.S3API, error) {
			h.clientsBuilt = true
			h.gotProfile, h.gotRegion = profile, region
			return h.athena, cmdFakeS3{size: h.sizePerPrefix}, nil
		},
		sleep:  func(context.Context, time.Duration) error { return nil },
		stdout: &h.stdout,
		stderr: &h.stderr,
		prompt: Prompt{
			In:          strings.NewReader(h.promptAnswer),
			Out:         &h.promptQuestions,
			Interactive: func() bool { return h.promptTerminal },
		},
		now: func() time.Time { return time.Date(2026, 10, 16, 11, 30, 0, 0, time.UTC) },
	}
}

func runArchiveCLI(t *testing.T, h *archiveHarness, cfgPath string, extra ...string) int {
	t.Helper()
	var flags SearchFlags
	flags.Topic = "orders"
	flags.ConfigFile = cfgPath
	flags.Since = "2026-10-16T10:00:00Z"
	flags.Archive = true
	for i := 0; i < len(extra); i += 2 {
		switch extra[i] {
		case "topic":
			flags.Topic = extra[i+1]
		case "context":
			flags.Context = extra[i+1]
		case "format":
			flags.Format = extra[i+1]
		case "profile":
			flags.Profile = extra[i+1]
		case "brokers":
			flags.Brokers = extra[i+1]
		case "max-bytes":
			flags.MaxBytes = extra[i+1]
		case "yes":
			flags.Yes = true
		case "explain":
			flags.Explain = true
		case "path":
			flags.Path = extra[i+1]
		}
	}
	params, err := buildSearchParams("order-41", &flags)
	if err != nil {
		t.Fatal(err)
	}
	if h.athena == nil {
		h.athena = &cmdFakeAthena{}
	}
	return runArchiveSearch(context.Background(), &flags, "order-41", params, h.deps())
}

// Under confirm-bytes the query runs without asking, and prints the match
// and its coverage.
func TestArchiveSearchRunsUnderConfirm(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{sizePerPrefix: 100} // 2 prefixes -> 200 B < 1 KiB
	if code := runArchiveCLI(t, h, cfg); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr.String())
	}
	if h.promptQuestions.Len() != 0 {
		t.Errorf("asked under the threshold: %q", h.promptQuestions.String())
	}
	if !strings.Contains(h.stdout.String(), "[p2 o40 ") {
		t.Errorf("stdout = %q", h.stdout.String())
	}
	for _, want := range []string{"archive · 1 matches", "query q-7", archive.NoteSinkLag} {
		if !strings.Contains(h.stderr.String(), want) {
			t.Errorf("stderr lacks %q:\n%s", want, h.stderr.String())
		}
	}
	if got := aws.ToString(h.athena.started.WorkGroup); got != "k4a" {
		t.Errorf("workgroup = %q", got)
	}
	if h.gotProfile != "cluster-profile" || h.gotRegion != "us-west-2" {
		t.Errorf("profile/region = %q/%q, want the context's", h.gotProfile, h.gotRegion)
	}
}

// Over max-bytes is refused before anything is billed, even with --yes.
func TestArchiveSearchRefusesOverMax(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{sizePerPrefix: 1 << 20}
	if code := runArchiveCLI(t, h, cfg, "yes", ""); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if h.athena.started != nil {
		t.Fatal("a query over max-bytes was started")
	}
	if !strings.Contains(h.stderr.String(), "over max-bytes") {
		t.Errorf("stderr = %q", h.stderr.String())
	}
	// --max-bytes raises the cap for one run.
	h2 := &archiveHarness{sizePerPrefix: 1 << 20}
	if code := runArchiveCLI(t, h2, cfg, "yes", "", "max-bytes", "10MiB"); code != 0 {
		t.Fatalf("exit %d with a raised cap: %s", code, h2.stderr.String())
	}
}

// Over confirm-bytes asks; a no aborts, a yes runs, and a non-terminal
// without --yes is refused rather than prompted.
func TestArchiveSearchConfirm(t *testing.T) {
	cfg := archiveEnv(t)

	no := &archiveHarness{sizePerPrefix: 4096, promptTerminal: true, promptAnswer: "n\n"}
	if code := runArchiveCLI(t, no, cfg); code != 1 || no.athena.started != nil {
		t.Fatalf("declined: exit %d started %v", code, no.athena.started != nil)
	}
	if !strings.Contains(no.promptQuestions.String(), "will scan up to 8.0 KiB") {
		t.Errorf("question = %q", no.promptQuestions.String())
	}

	yes := &archiveHarness{sizePerPrefix: 4096, promptTerminal: true, promptAnswer: "y\n"}
	if code := runArchiveCLI(t, yes, cfg); code != 0 || yes.athena.started == nil {
		t.Fatalf("accepted: exit %d", code)
	}

	pipe := &archiveHarness{sizePerPrefix: 4096}
	if code := runArchiveCLI(t, pipe, cfg); code != 1 || pipe.athena.started != nil {
		t.Fatalf("non-terminal: exit %d", code)
	}
	if !strings.Contains(pipe.stderr.String(), "--yes") {
		t.Errorf("non-terminal refusal does not name --yes: %q", pipe.stderr.String())
	}
}

// Without a location there is no estimate, so it always asks.
func TestArchiveSearchNoEstimateAsks(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{}
	if code := runArchiveCLI(t, h, cfg, "topic", "nolocation"); code != 1 || h.athena.started != nil {
		t.Fatalf("exit %d", code)
	}
	h = &archiveHarness{athena: &cmdFakeAthena{valueOnly: true}}
	if code := runArchiveCLI(t, h, cfg, "topic", "nolocation", "yes", ""); code != 0 {
		t.Fatalf("--yes: exit %d: %s", code, h.stderr.String())
	}
}

// The archive's own profile wins over the cluster's -p, which wins over
// the context's.
func TestArchiveSearchProfilePrecedence(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{}
	runArchiveCLI(t, h, cfg, "context", "lake", "profile", "flag-profile", "yes", "")
	if h.gotProfile != "lake-profile" || h.gotRegion != "eu-west-1" {
		t.Errorf("lake: %q/%q, want lake-profile/eu-west-1", h.gotProfile, h.gotRegion)
	}
	h = &archiveHarness{sizePerPrefix: 1}
	runArchiveCLI(t, h, cfg, "profile", "flag-profile")
	if h.gotProfile != "flag-profile" {
		t.Errorf("prod with -p: %q, want flag-profile", h.gotProfile)
	}
}

func TestArchiveSearchExplainRunsNothing(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{}
	if code := runArchiveCLI(t, h, cfg, "explain", ""); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr.String())
	}
	if h.clientsBuilt {
		t.Error("--explain built AWS clients")
	}
	for _, want := range []string{`FROM "AwsDataCatalog"."kafka_archive"."orders"`, "s3://arch/orders/dt=2026-10-16-10/"} {
		if !strings.Contains(h.stdout.String(), want) {
			t.Errorf("explain lacks %q:\n%s", want, h.stdout.String())
		}
	}
}

func TestArchiveSearchRefusals(t *testing.T) {
	cfg := archiveEnv(t)
	for name, extra := range map[string][]string{
		"brokers":       {"brokers", "localhost:9092"},
		"path":          {"path", ".id == 1"},
		"unknown topic": {"topic", "payments"},
		"glob topic":    {"topic", "orders.*"},
	} {
		h := &archiveHarness{}
		if code := runArchiveCLI(t, h, cfg, extra...); code != 1 || h.clientsBuilt {
			t.Errorf("%s: exit %d, clients built %v", name, code, h.clientsBuilt)
		}
	}
}

func TestArchiveSearchSummary(t *testing.T) {
	cfg := archiveEnv(t)
	h := &archiveHarness{sizePerPrefix: 1}
	if code := runArchiveCLI(t, h, cfg, "format", "summary"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr.String())
	}
	var sum searchcli.Summary
	if err := json.Unmarshal(h.stdout.Bytes(), &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Stats.Source != "archive" || sum.Archive == nil || sum.Archive.QueryExecutionID != "q-7" ||
		len(sum.Matches) != 1 || sum.Archive.ScannedBytes != 512 || sum.Archive.EstimatedBytes == nil {
		t.Errorf("summary = %+v archive %+v", sum, sum.Archive)
	}
}

func TestGateArchive(t *testing.T) {
	t.Parallel()
	l := archive.Limits{ConfirmBytes: 10, MaxBytes: 100}
	n := func(v int64) *int64 { return &v }
	refuse := Prompt{Interactive: func() bool { return false }}
	if err := gateArchive(n(5), l, false, refuse, "t"); err != nil {
		t.Errorf("under confirm: %v", err)
	}
	if err := gateArchive(n(50), l, false, refuse, "t"); err == nil {
		t.Error("over confirm ran without asking")
	}
	if err := gateArchive(n(50), l, true, refuse, "t"); err != nil {
		t.Errorf("--yes over confirm: %v", err)
	}
	if err := gateArchive(n(500), l, true, refuse, "t"); err == nil {
		t.Error("--yes overrode max-bytes")
	}
	if err := gateArchive(nil, l, false, refuse, "t"); err == nil {
		t.Error("no estimate ran without asking")
	}
	if err := gateArchive(n(500), archive.Limits{ConfirmBytes: 10}, true, refuse, "t"); err != nil {
		t.Errorf("max-bytes 0 should not refuse: %v", err)
	}
}
