// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/blairham/k4a/internal/kafka"
)

func testTopic() Topic {
	return Topic{
		Database: "kafka_archive",
		Table:    "orders",
		Columns: Columns{
			Value: "value", Timestamp: "ts", Key: "key",
			Partition: "kafka_partition", Offset: "kafka_offset",
		},
		TimePartitions: []TimePartition{{Column: "dt", Format: "{yyyy}-{MM}-{dd}-{HH}"}},
		Location:       "s3://arch/topics/orders/dt={yyyy}-{MM}-{dd}-{HH}/",
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func TestParseSize(t *testing.T) {
	t.Parallel()
	cases := map[string]int64{
		"2048": 2048, "1KB": 1000, "1KiB": 1024, "1.5GiB": 3 << 29, "50GiB": 50 << 30,
		"10 MB": 10_000_000, "1TB": 1e12, "7B": 7,
	}
	for in, want := range cases {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "x", "-1GB", "1XB", "1e30TB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("ParseSize(%q) succeeded", bad)
		}
	}
}

func TestLimitsDefaultsAndOverrides(t *testing.T) {
	t.Parallel()
	l, err := Config{}.Limits()
	if err != nil || l.ConfirmBytes != DefaultConfirmBytes || l.MaxBytes != DefaultMaxBytes {
		t.Fatalf("defaults = %+v, %v", l, err)
	}
	l, err = Config{ConfirmBytes: "2GiB", MaxBytes: "4GiB"}.Limits()
	if err != nil || l.ConfirmBytes != 2<<30 || l.MaxBytes != 4<<30 {
		t.Fatalf("overrides = %+v, %v", l, err)
	}
	if _, err := (Config{MaxBytes: "lots"}).Limits(); err == nil {
		t.Fatal("bad max-bytes accepted")
	}
}

func TestTopicValidation(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*Topic){
		"no time partitions": func(tp *Topic) { tp.TimePartitions = nil },
		"injected table":     func(tp *Topic) { tp.Table = `orders"; DROP TABLE x; --` },
		"expression column":  func(tp *Topic) { tp.Columns.Value = "json_format(v)" },
		"unknown token":      func(tp *Topic) { tp.TimePartitions[0].Format = "{YYYY}-{MM}" },
		"no token":           func(tp *Topic) { tp.TimePartitions[0].Format = "static" },
		"quote in format":    func(tp *Topic) { tp.TimePartitions[0].Format = "{yyyy}'" },
		"compound integer": func(tp *Topic) {
			tp.TimePartitions[0].Type = PartitionInteger
		},
		"bad timestamp type": func(tp *Topic) { tp.Columns.TimestampType = "seconds" },
		"bad zone":           func(tp *Topic) { tp.TimeZone = "Mars/Olympus" },
		"non-s3 location":    func(tp *Topic) { tp.Location = "https://example.com/{yyyy}" },
		"bad location token": func(tp *Topic) { tp.Location = "s3://b/{yy}/" },
		"missing value":      func(tp *Topic) { tp.Columns.Value = "" },
	}
	for name, mutate := range cases {
		tp := testTopic()
		mutate(&tp)
		_, err := Config{Topics: map[string]Topic{"orders": tp}}.Topic("orders")
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Positive control: the unmutated topic is valid, so each failure above
	// is the mutation's.
	if _, err := (Config{Topics: map[string]Topic{"orders": testTopic()}}).Topic("orders"); err != nil {
		t.Fatalf("valid topic rejected: %v", err)
	}
	if _, err := (Config{}).Topic("orders"); !errors.Is(err, ErrNoArchive) {
		t.Errorf("missing topic err = %v, want ErrNoArchive", err)
	}
	if _, err := (Config{Catalog: "a.b", Topics: map[string]Topic{"orders": testTopic()}}).Topic("orders"); err == nil {
		t.Error("dotted catalog accepted")
	}
}

// Every local hour that intersects the range must appear, in any zone:
// checked against a minute-by-minute walk.
func TestPartitionTuplesCoverEveryLocalHour(t *testing.T) {
	t.Parallel()
	parts := []TimePartition{{Column: "dt", Format: "{yyyy}-{MM}-{dd}-{HH}"}}
	zones := []string{"UTC", "Asia/Kolkata", "America/New_York", "Australia/Lord_Howe", "Asia/Kathmandu"}
	ranges := [][2]string{
		{"2026-03-08T05:17:00Z", "2026-03-08T09:03:00Z"}, // US DST start
		{"2026-11-01T04:59:59Z", "2026-11-01T07:30:00Z"}, // US DST end
		{"2026-10-16T00:00:00Z", "2026-10-16T00:00:00Z"}, // empty range
		{"2026-10-03T14:45:00Z", "2026-10-03T17:20:00Z"}, // Lord Howe 30-min DST: a 30-minute local hour
		{"2026-04-04T13:50:00Z", "2026-04-04T16:10:00Z"}, // Lord Howe DST end
		{"2026-06-01T10:10:00Z", "2026-06-01T10:20:00Z"},
	}
	for _, zn := range zones {
		loc, err := time.LoadLocation(zn)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range ranges {
			since, until := mustTime(t, r[0]), mustTime(t, r[1])
			got := map[string]bool{}
			for _, tup := range partitionTuples(parts, loc, since, until) {
				got[tup[0]] = true
			}
			want := map[string]bool{}
			for m := since; !m.After(until); m = m.Add(time.Minute) {
				want[formatAt(parts[0].Format, m.In(loc))] = true
			}
			want[formatAt(parts[0].Format, until.In(loc))] = true
			for v := range want {
				if !got[v] {
					t.Errorf("%s %v: partition %s missing", zn, r, v)
				}
			}
			for v := range got {
				if !want[v] {
					t.Errorf("%s %v: partition %s selected but outside the range", zn, r, v)
				}
			}
		}
	}
}

func TestPartitionPredicateShapes(t *testing.T) {
	t.Parallel()
	since, until := mustTime(t, "2026-10-16T22:30:00Z"), mustTime(t, "2026-10-17T01:10:00Z")

	single := []TimePartition{{Column: "dt", Format: "{yyyy}-{MM}-{dd}-{HH}"}}
	got := partitionPredicate(single, partitionTuples(single, time.UTC, since, until))
	want := `"dt" IN ('2026-10-16-22', '2026-10-16-23', '2026-10-17-00', '2026-10-17-01')`
	if got != want {
		t.Errorf("single:\n got %s\nwant %s", got, want)
	}

	hive := []TimePartition{
		{Column: "year", Format: "{yyyy}"},
		{Column: "month", Format: "{MM}", Type: PartitionInteger},
		{Column: "day", Format: "{dd}"},
		{Column: "hour", Format: "{HH}"},
	}
	got = partitionPredicate(hive, partitionTuples(hive, time.UTC, since, until))
	want = `("year" = '2026' AND ("month" = 10 AND (("day" = '16' AND "hour" IN ('22', '23')) OR ` +
		`("day" = '17' AND "hour" IN ('00', '01')))))`
	if got != want {
		t.Errorf("hive:\n got %s\nwant %s", got, want)
	}
}

func TestCandidateLiteral(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pattern string
		want    string
	}{
		{"(?i)order-41000017", "order-41000017"},
		{"order-41000017", "order-41000017"},
		{`(?i)"orderId":\d+`, `"orderid":`},
		{"(?i)abc(defgh)+x", "defgh"},
		{"(?i)foo.*barbaz", "barbaz"},
		{"(?i)(?:trace){2,}", "trace"},
		// Kelvin sign and long s split the literal.
		{"(?i)kafka-sink-01", "-01"},
		{"(?i)ÉCOLE-normale", "cole-normale"},
		{"(?i)status-x", "tatu"},
		{"Écolenormale", "colenormale"},
	}
	for _, tc := range cases {
		got, err := CandidateLiteral(tc.pattern)
		if err != nil || got != tc.want {
			t.Errorf("CandidateLiteral(%q) = %q, %v; want %q", tc.pattern, got, err, tc.want)
		}
	}
	for _, p := range []string{"(?i)a|b", "(?i)(abc)?", "[a-z]+", "(?i)ks", "(?i)ab", "(?i)kafka-sink"} {
		if got, err := CandidateLiteral(p); !errors.Is(err, ErrNoLiteral) {
			t.Errorf("CandidateLiteral(%q) = %q, %v; want ErrNoLiteral", p, got, err)
		}
	}
}

// The candidate filter must never miss: every string the regex matches
// contains the literal after lower(). The fold variants are what Go's (?i)
// accepts and lower() does not normalize.
func TestCandidateLiteralNeverMisses(t *testing.T) {
	t.Parallel()
	lower := func(s string) string { return strings.ToLower(s) }
	cases := []struct {
		pattern string
		inputs  []string
	}{
		{"(?i)kafka-sink-topic", []string{"KAFKA-SINK-TOPIC", "x\u212Aafka-\u017Fink-topic", "Kafka-Sink-Topic!"}},
		{"(?i)status", []string{"STATU\u017F", "\u017Ftatus"}},
		{"(?i)order-41", []string{"ORDER-41", "Order-41x"}},
		{`(?i)"id":\d+`, []string{`{"ID":5}`, `"Id":77`}},
	}
	for _, tc := range cases {
		lit, err := CandidateLiteral(tc.pattern)
		if err != nil {
			t.Fatalf("%q: %v", tc.pattern, err)
		}
		re := regexp.MustCompile(tc.pattern)
		for _, in := range tc.inputs {
			if !re.MatchString(in) {
				t.Fatalf("%q does not match %q; the case tests nothing", tc.pattern, in)
			}
			if !strings.Contains(lower(in), lit) {
				t.Errorf("%q matches %q but lower() lacks literal %q", tc.pattern, in, lit)
			}
		}
	}
	// Positive control: the naive literal would have missed the Kelvin row,
	// so the check above can fail.
	if strings.Contains(lower("x\u212Aafka-\u017Fink-topic"), "kafka-sink-topic") {
		t.Fatal("lower() normalizes Kelvin/long s here; the never-misses check proves nothing")
	}
}

func testRequest(t *testing.T) Request {
	t.Helper()
	return Request{
		Topic:   "orders",
		Pattern: regexp.MustCompile("(?i)order-41"),
		Since:   mustTime(t, "2026-10-16T10:00:00Z"),
		Until:   mustTime(t, "2026-10-16T11:30:00Z"),
		Cap:     100,
	}
}

func TestBuildPlanSQL(t *testing.T) {
	t.Parallel()
	req := testRequest(t)
	req.Partitions = []int32{0, 3}
	p, err := BuildPlan(DefaultCatalog, testTopic(), req)
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT "value", "ts", "key", "kafka_partition", "kafka_offset"
FROM "AwsDataCatalog"."kafka_archive"."orders"
WHERE "dt" IN ('2026-10-16-10', '2026-10-16-11')
  AND "ts" BETWEEN 1792144800000 AND 1792150200000
  AND (strpos(lower("key"), 'order-41') > 0 OR strpos(lower("value"), 'order-41') > 0)
  AND "kafka_partition" IN (0, 3)
ORDER BY "ts" DESC
LIMIT 100`
	if p.SQL != want {
		t.Errorf("SQL:\n%s\nwant:\n%s", p.SQL, want)
	}
	if len(p.Prefixes) != 2 || p.Prefixes[0] != "s3://arch/topics/orders/dt=2026-10-16-10/" {
		t.Errorf("prefixes = %v", p.Prefixes)
	}
	if p.PartitionCount != 2 || p.FirstPartition != "2026-10-16-10" || p.LastPartition != "2026-10-16-11" {
		t.Errorf("partitions = %d %s %s", p.PartitionCount, p.FirstPartition, p.LastPartition)
	}
}

func TestBuildPlanQuotesLiteral(t *testing.T) {
	t.Parallel()
	req := testRequest(t)
	req.Pattern = regexp.MustCompile(`(?i)o'brien`)
	p, err := BuildPlan(DefaultCatalog, testTopic(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.SQL, `'o''brien'`) {
		t.Errorf("quote not doubled:\n%s", p.SQL)
	}
}

func TestBuildPlanTimestampType(t *testing.T) {
	t.Parallel()
	tp := testTopic()
	tp.Columns.TimestampType = TimestampTimestamp
	p, err := BuildPlan(DefaultCatalog, tp, testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		p.SQL,
		`"ts" BETWEEN TIMESTAMP '2026-10-16 10:00:00.000' AND TIMESTAMP '2026-10-16 11:30:00.000'`,
	) {
		t.Errorf("timestamp predicate missing:\n%s", p.SQL)
	}
}

func TestBuildPlanRefusals(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*Topic, *Request){
		"no since":          func(_ *Topic, r *Request) { r.Since = time.Time{} },
		"until before":      func(_ *Topic, r *Request) { r.Until = r.Since },
		"no literal":        func(_ *Topic, r *Request) { r.Pattern = regexp.MustCompile("(?i)a|b") },
		"no pattern":        func(_ *Topic, r *Request) { r.Pattern = nil },
		"partitions no col": func(tp *Topic, r *Request) { tp.Columns.Partition = ""; r.Partitions = []int32{1} },
		"scope missing": func(tp *Topic, r *Request) {
			tp.Columns.Headers = ""
			r.Scope = kafka.ScopeHeaders
		},
		"too many partitions": func(_ *Topic, r *Request) { r.Since = r.Until.AddDate(-3, 0, 0) },
	}
	for name, mutate := range cases {
		tp, req := testTopic(), testRequest(t)
		mutate(&tp, &req)
		if _, err := BuildPlan(DefaultCatalog, tp, req); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestBuildPlanReportsMissingFields(t *testing.T) {
	t.Parallel()
	tp := testTopic()
	tp.Columns.Key = ""
	p, err := BuildPlan(DefaultCatalog, tp, testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(p.Fields, ",") != FieldValue || strings.Join(p.MissingFields, ",") != FieldKey {
		t.Errorf("fields %v missing %v", p.Fields, p.MissingFields)
	}
	cov := NewCoverage(p, nil, Result{})
	if !strings.Contains(strings.Join(cov.Notes, "\n"), "does not store") {
		t.Errorf("coverage notes %v do not mention the missing key", cov.Notes)
	}
}

func row(cells ...string) []string { return cells }

func TestRunFiltersDedupesAndCaps(t *testing.T) {
	t.Parallel()
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	p.Limit = 4
	fa := &fakeAthena{
		states: []fakeState{
			{state: types.QueryExecutionStateQueued},
			{state: types.QueryExecutionStateRunning, scanned: 10},
			{state: types.QueryExecutionStateSucceeded, scanned: 2048},
		},
		pages: [][][]string{
			{
				row("value", "ts", "key", "kafka_partition", "kafka_offset"),
				row(`{"id":"ORDER-41"}`, "1792148400000", "k1", "0", "7"),
				row(`{"id":"order-4"}`, "1792148300000", "order-41x", "1", "9"),
			},
			{
				row(`{"id":"ORDER-41"}`, "1792148400000", "k1", "0", "7"),    // redelivery
				row(`{"id":"orderXX41"}`, "1792148200000", "\x00", "2", "1"), // candidate, not a match
			},
		},
	}
	r := Runner{
		Athena:         fa,
		Workgroup:      "k4a",
		OutputLocation: "s3://out/",
		Sleep:          func(context.Context, time.Duration) error { return nil },
	}
	res, err := r.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(fa.started.WorkGroup) != "k4a" ||
		aws.ToString(fa.started.ResultConfiguration.OutputLocation) != "s3://out/" ||
		aws.ToString(fa.started.QueryString) != p.SQL {
		t.Errorf("start input = %+v", fa.started)
	}
	if res.QueryID != "q-1" || res.ScannedBytes != 2048 || res.Candidates != 4 || res.DuplicatesDropped != 1 {
		t.Errorf("result = %+v", res)
	}
	if len(res.Matches) != 2 {
		t.Fatalf("matches = %+v, want the two real ones", res.Matches)
	}
	m := res.Matches[0]
	if m.Partition != 0 || m.Offset != 7 || m.Key != "k1" || m.Topic != "orders" ||
		!m.Time.Equal(time.UnixMilli(1792148400000)) {
		t.Errorf("first match = %+v", m)
	}
	if !res.Capped {
		t.Error("candidates reached the limit but the result is not capped")
	}
	if len(fa.stopped) != 0 {
		t.Errorf("a successful query was stopped: %v", fa.stopped)
	}
}

func TestRunWatchdogStopsOverCap(t *testing.T) {
	t.Parallel()
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	fa := &fakeAthena{states: []fakeState{
		{state: types.QueryExecutionStateRunning, scanned: 100},
		{state: types.QueryExecutionStateRunning, scanned: 5000},
	}}
	r := Runner{Athena: fa, MaxBytes: 1000, Sleep: func(context.Context, time.Duration) error { return nil }}
	_, err = r.Run(context.Background(), p)
	if !errors.Is(err, ErrBytesCap) {
		t.Fatalf("err = %v, want ErrBytesCap", err)
	}
	if len(fa.stopped) != 1 || fa.stopped[0] != "q-1" {
		t.Errorf("stopped = %v, want [q-1]", fa.stopped)
	}

	// Positive control: the same scan under a higher cap is not stopped.
	fa2 := &fakeAthena{states: []fakeState{
		{state: types.QueryExecutionStateRunning, scanned: 5000},
		{state: types.QueryExecutionStateSucceeded, scanned: 5000},
	}, pages: [][][]string{{row("value", "ts", "key", "kafka_partition", "kafka_offset")}}}
	r.Athena, r.MaxBytes = fa2, 10_000
	if _, err := r.Run(context.Background(), p); err != nil || len(fa2.stopped) != 0 {
		t.Errorf("under-cap run: err %v stopped %v", err, fa2.stopped)
	}
}

func TestRunCancelStopsQuery(t *testing.T) {
	t.Parallel()
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	fa := &fakeAthena{states: []fakeState{{state: types.QueryExecutionStateRunning}}}
	ctx, cancel := context.WithCancel(context.Background())
	r := Runner{Athena: fa, Sleep: func(context.Context, time.Duration) error { cancel(); return context.Canceled }}
	if _, err := r.Run(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if len(fa.stopped) != 1 {
		t.Errorf("an interrupted query was left running: stopped = %v", fa.stopped)
	}
}

func TestRunFailedQuery(t *testing.T) {
	t.Parallel()
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	fa := &fakeAthena{states: []fakeState{{state: types.QueryExecutionStateFailed, reason: "TABLE_NOT_FOUND"}}}
	r := Runner{Athena: fa}
	if _, err := r.Run(context.Background(), p); err == nil || !strings.Contains(err.Error(), "TABLE_NOT_FOUND") {
		t.Fatalf("err = %v, want the state change reason", err)
	}
	fa = &fakeAthena{startErr: errors.New("denied")}
	r.Athena = fa
	if _, err := r.Run(context.Background(), p); err == nil {
		t.Fatal("start error swallowed")
	}
}

func TestEstimate(t *testing.T) {
	t.Parallel()
	fs := &fakeS3{objects: map[string]int64{
		"arch/topics/orders/dt=2026-10-16-10/a.json": 100,
		"arch/topics/orders/dt=2026-10-16-10/b.json": 200,
		"arch/topics/orders/dt=2026-10-16-10/c.json": 300, // second page
		"arch/topics/orders/dt=2026-10-16-11/a.json": 1000,
		"arch/topics/orders/dt=2026-10-16-12/a.json": 99999, // outside the range
	}}
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	n, err := Estimate(context.Background(), fs, p.Prefixes)
	if err != nil || n != 1600 {
		t.Fatalf("Estimate = %d, %v; want 1600", n, err)
	}
	fs.err = errors.New("AccessDenied")
	if _, err := Estimate(context.Background(), fs, p.Prefixes); err == nil {
		t.Fatal("list error swallowed")
	}
	if _, err := Estimate(context.Background(), fs, []string{"https://x"}); err == nil {
		t.Fatal("non-s3 prefix accepted")
	}
}

func TestCoverageLines(t *testing.T) {
	t.Parallel()
	p, err := BuildPlan(DefaultCatalog, testTopic(), testRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	est := int64(3 << 30)
	cov := NewCoverage(p, &est, Result{QueryID: "q-9", ScannedBytes: 1 << 30, Candidates: 100, Capped: true})
	text := strings.Join(cov.Lines(42), "\n")
	for _, want := range []string{"42 matches", "scanned 1.0 GiB", "estimated 3.0 GiB", "q-9", "capped", NoteSinkLag, NoteRetention} {
		if !strings.Contains(text, want) {
			t.Errorf("coverage text lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, NoteNoDedupe) {
		t.Error("dedupe note on an archive with partition and offset columns")
	}
}
