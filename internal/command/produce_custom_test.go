// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// readAll consumes every record on readonlyTopic from the start.
func readAll(t *testing.T, brokers string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(readonlyTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func TestProduceValueKeyHeaders(t *testing.T) {
	t.Parallel()
	brokers, _ := newGuardCluster(t)
	cfg := writeConfig(t, t.TempDir(), "")
	code := (&ProduceCommand{}).Run([]string{
		"--config", cfg, "-b", brokers, "-t", readonlyTopic,
		"--record-key", "order-42", "--value", `{"id":42}`, "-H", "source=cli", "-H", "trace=a=b",
	})
	if code != 0 {
		t.Fatalf("Run = %d", code)
	}
	rs := readAll(t, brokers, 1)
	if len(rs) != 1 {
		t.Fatalf("got %d records, want exactly 1 (--value defaults to one)", len(rs))
	}
	r := rs[0]
	if string(r.Key) != "order-42" || string(r.Value) != `{"id":42}` ||
		header(r, "source") != "cli" || header(r, "trace") != "a=b" {
		t.Errorf("record key=%q value=%q headers=%v", r.Key, r.Value, r.Headers)
	}
}

func TestProduceStdin(t *testing.T) {
	t.Parallel()
	brokers, _ := newGuardCluster(t)
	cfg := writeConfig(t, t.TempDir(), "")
	in := strings.NewReader("k1|v1\nk2|v|2\n\n")
	code := (&ProduceCommand{In: in}).Run([]string{
		"--config", cfg, "-b", brokers, "-t", readonlyTopic, "--stdin", "--key-separator", "|", "-H", "h=1",
	})
	if code != 0 {
		t.Fatalf("Run = %d", code)
	}
	rs := readAll(t, brokers, 2)
	if len(rs) != 2 {
		t.Fatalf("got %d records, want 2", len(rs))
	}
	if string(rs[0].Key) != "k1" || string(rs[0].Value) != "v1" || string(rs[1].Key) != "k2" ||
		string(rs[1].Value) != "v|2" || header(rs[1], "h") != "1" {
		t.Errorf("records = %q/%q, %q/%q", rs[0].Key, rs[0].Value, rs[1].Key, rs[1].Value)
	}
}

func TestBuildRecordsRefusesAmbiguousInput(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]ProduceFlags{
		"value and stdin":         {Topic: "t", Value: "v", Stdin: true},
		"value and schema":        {Topic: "t", Value: "v", Schema: "s.yaml"},
		"key without value":       {Topic: "t", Key: "k"},
		"separator without stdin": {Topic: "t", Value: "v", KeySeparator: "|"},
		"count with stdin":        {Topic: "t", Stdin: true, Count: 3},
		"value without topic":     {Value: "v"},
		"bad header":              {Topic: "t", Value: "v", Headers: []string{"noequals"}},
	} {
		if _, _, err := buildRecords(&f, strings.NewReader("x\n")); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, _, err := buildRecords(&ProduceFlags{Topic: "t", Stdin: true}, strings.NewReader("\n\n")); err == nil {
		t.Error("an empty stdin was accepted")
	}

	// Generated messages get the headers too, and keep the default count.
	f := ProduceFlags{Topic: "t", Headers: []string{"h=1"}}
	_, rs, err := buildRecords(&f, nil)
	if err != nil || len(rs) != defaultGeneratedCount || header(rs[0], "h") != "1" {
		t.Errorf("generated = %d records, %v", len(rs), err)
	}
}

func TestProduceInterval(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		f    ProduceFlags
		want time.Duration
	}{
		{ProduceFlags{}, time.Second},
		{ProduceFlags{Value: "v"}, 0},
		{ProduceFlags{Stdin: true}, 0},
		{ProduceFlags{Value: "v", Interval: "250ms"}, 250 * time.Millisecond},
	} {
		if got, err := produceInterval(tc.f); err != nil || got != tc.want {
			t.Errorf("produceInterval(%+v) = %v, %v; want %v", tc.f, got, err, tc.want)
		}
	}
}
