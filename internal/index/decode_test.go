// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// fakeDecoder stands in for a schema registry: a framed value (zero magic
// byte, four ID bytes, then a word) decodes to {"fruit":"<word>"}.
type fakeDecoder struct {
	id        string
	calls     atomic.Int32
	unsettled bool
}

func (d *fakeDecoder) Identity() string { return d.id }

func (d *fakeDecoder) DecodeRecord(_ context.Context, m *kafka.ConsumedMessage) bool {
	d.calls.Add(1)
	if len(m.RawValue) < 5 {
		return true
	}
	if d.unsettled {
		m.ValueDecoding = kafka.Decoding{Reason: "schema registry unreachable"}
		return false
	}
	m.ValueDecoding = kafka.Decoding{Raw: m.Value, Schema: "fruit-value v1 (id 1, AVRO)"}
	m.Value = `{"fruit":"` + string(m.RawValue[5:]) + `"}`
	return true
}

func framedValue(word string) []byte { return append([]byte{0, 0, 0, 0, 1}, word...) }

// framedMsg is a record as recordToConsumed renders a framed value: hex for
// display, the bytes kept for a decoder.
func framedMsg(offset int64, word string, ts time.Time) kafka.ConsumedMessage {
	m := sampleMsg(0, offset, "k", "hex:0000000001", ts)
	m.RawValue = framedValue(word)
	return m
}

func openDecoding(t *testing.T, root *state.Root, dec kafka.RecordDecoder) *Indexer {
	t.Helper()
	ix, err := OpenDecoding(root, testCluster, "orders", dec)
	if err != nil {
		t.Fatalf("OpenDecoding: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix
}

func queryOffsets(t *testing.T, ix *Indexer, params QueryParams) []int64 {
	t.Helper()
	res, err := ix.Query(context.Background(), params)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	out := make([]int64, 0, len(res.Matches))
	for _, m := range res.Matches {
		out = append(out, m.Offset)
	}
	slices.Sort(out)
	return out
}

func TestIndexBatch_StoresDecodedText(t *testing.T) {
	t.Parallel()
	dec := &fakeDecoder{id: "sr:a"}
	ix := openDecoding(t, state.NewRoot(t.TempDir()), dec)
	now := time.Now()
	msgs := []kafka.ConsumedMessage{framedMsg(1, "orange", now), sampleMsg(0, 2, "k", `{"plain":"pear"}`, now)}
	if err := ix.IndexBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	if n := dec.calls.Load(); n != 1 {
		t.Errorf("decoder called %d times, want 1: an unframed record must not be decoded", n)
	}
	if got := ix.BookkeeperSnapshot().Decoder; got != "sr:a" {
		t.Errorf("bookkeeper decoder = %q, want sr:a", got)
	}
	if got := queryOffsets(t, ix, QueryParams{Pattern: regexp.MustCompile("orange")}); !slices.Equal(got, []int64{1}) {
		t.Errorf("decoded text: matched %v, want [1]", got)
	}
	if got := queryOffsets(t, ix, QueryParams{Pattern: regexp.MustCompile("hex")}); len(got) != 0 {
		t.Errorf("the raw rendering of a decoded record matched: %v", got)
	}
	if got := queryOffsets(t, ix, QueryParams{Pattern: regexp.MustCompile("pear")}); !slices.Equal(got, []int64{2}) {
		t.Errorf("unframed text: matched %v, want [2]", got)
	}
}

func TestIndexBatch_UnsettledDecodeLeavesGap(t *testing.T) {
	t.Parallel()
	ix := openDecoding(t, state.NewRoot(t.TempDir()), &fakeDecoder{id: "sr:a", unsettled: true})
	now := time.Now()
	msgs := []kafka.ConsumedMessage{
		framedMsg(4, "plum", now), sampleMsg(0, 5, "k", "v", now), framedMsg(7, "fig", now),
	}
	if err := ix.IndexBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	book := ix.BookkeeperSnapshot()
	want := []Gap{{Partition: 0, Start: 4, End: 8, Reason: GapDecodeUnavailable}}
	if !slices.Equal(book.Gaps, want) {
		t.Fatalf("gaps = %+v, want %+v", book.Gaps, want)
	}
	ok, reason := book.CanServe(QueryParams{
		Since: now, Until: now, Decoder: "sr:a",
	})
	if ok || !strings.Contains(reason, "gap") {
		t.Errorf("CanServe = %v (%q) over records the decoder could not settle; want a gap refusal", ok, reason)
	}
}

func TestOpenDecoding_OtherDecoderRebuilds(t *testing.T) {
	t.Parallel()
	root := state.NewRoot(t.TempDir())
	populate := func(dec kafka.RecordDecoder) {
		ix, err := OpenDecoding(root, testCluster, "orders", dec)
		if err != nil {
			t.Fatal(err)
		}
		defer ix.Close() //nolint:errcheck // test cleanup
		if err := ix.Index(context.Background(), framedMsg(1, "kiwi", time.Now())); err != nil {
			t.Fatal(err)
		}
		if err := ix.FlushBookkeeper(); err != nil {
			t.Fatal(err)
		}
	}
	docs := func(dec kafka.RecordDecoder) (uint64, string) {
		ix, err := OpenDecoding(root, testCluster, "orders", dec)
		if err != nil {
			t.Fatal(err)
		}
		defer ix.Close() //nolint:errcheck // test cleanup
		n, err := ix.DocCount()
		if err != nil {
			t.Fatal(err)
		}
		return n, ix.BookkeeperSnapshot().Decoder
	}

	populate(&fakeDecoder{id: "sr:a"})
	if n, id := docs(&fakeDecoder{id: "sr:a"}); n != 1 || id != "sr:a" {
		t.Fatalf("same decoder: %d docs under %q, want the index kept (1 under sr:a)", n, id)
	}
	if n, id := docs(&fakeDecoder{id: "sr:b"}); n != 0 || id != "sr:b" {
		t.Errorf("other registry: %d docs under %q, want a rebuilt empty index under sr:b", n, id)
	}
	populate(&fakeDecoder{id: "sr:b"})
	if n, id := docs(nil); n != 0 || id != "" {
		t.Errorf("no registry: %d docs under %q, want a rebuilt empty raw index", n, id)
	}
}

func TestCanServe_DecoderMustMatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UnixMilli()
	book := Bookkeeper{
		Decoder:                  "sr:a",
		NewestOffsetPerPartition: map[int32]int64{0: 10},
		OldestOffsetPerPartition: map[int32]int64{0: 0},
		NewestTimePerPartition:   map[int32]int64{0: now},
		OldestTimePerPartition:   map[int32]int64{0: now - 60_000},
	}
	params := QueryParams{Since: time.UnixMilli(now - 30_000), Until: time.UnixMilli(now - 1)}
	for search, want := range map[string]bool{"sr:a": true, "sr:b": false, "": false} {
		params.Decoder = search
		if ok, reason := book.CanServe(params); ok != want {
			t.Errorf("index sr:a, search %q: CanServe = %v (%q), want %v", search, ok, reason, want)
		}
	}
}

// TestDecodedIndexMatchesDecodedScan is the parity contract end to end: the
// same records, the same decoder, the same patterns, once through a broker
// scan and once through the index, must select the same offsets. Patterns
// that hit only decoded text, only the framed bytes, and only an unframed
// record each exercise a different side of it.
func TestDecodedIndexMatchesDecodedScan(t *testing.T) {
	t.Parallel()
	const topic = "decoded.parity.v1"
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)
	seeds := strings.Join(fake.ListenAddrs(), ",")
	produceParityRecords(t, seeds, topic)

	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	dec := &fakeDecoder{id: "sr:a"}
	ix := openDecoding(t, state.NewRoot(t.TempDir()), dec)
	indexAll(t, client, ix, topic)

	for _, pat := range []string{"orange", `"fruit":"or`, "fruit", "pear", "plain", `\x00`} {
		re := regexp.MustCompile(pat)
		scanned := scanOffsets(t, client, topic, kafka.SearchParams{Pattern: re, Decoder: dec})
		indexed := queryOffsets(t, ix, QueryParams{Pattern: re, Decoder: dec.Identity()})
		if !slices.Equal(scanned, indexed) {
			t.Errorf("%q: scan matched %v, index matched %v", pat, scanned, indexed)
		}
	}
	// Falsifiability: without the decoder the scan reads the bytes, so the
	// decoded-only pattern must stop matching. If it does not, the scan was
	// never decoding and the agreement above proves nothing.
	if raw := scanOffsets(t, client, topic, kafka.SearchParams{Pattern: regexp.MustCompile("fruit")}); len(raw) != 0 {
		t.Errorf("a raw scan matched decoded-only text at %v", raw)
	}
}

func produceParityRecords(t *testing.T, seeds, topic string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(seeds, ",")...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	values := [][]byte{
		framedValue("orange"), []byte(`{"plain":"pear"}`), framedValue("lemon"), []byte("orange juice"),
	}
	for _, v := range values {
		if err := cl.ProduceSync(context.Background(), &kgo.Record{Key: []byte("k"), Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func indexAll(t *testing.T, client *kafka.Client, ix *Indexer, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ends, err := client.EndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	starts := make(map[int32]int64, len(ends))
	for p := range ends {
		starts[p] = 0
	}
	var msgs []kafka.ConsumedMessage
	if err := client.ConsumeRange(ctx, topic, starts, ends, func(m kafka.ConsumedMessage) {
		msgs = append(msgs, m)
	}); err != nil {
		t.Fatal(err)
	}
	if err := ix.IndexBatch(ctx, msgs); err != nil {
		t.Fatal(err)
	}
}

func scanOffsets(t *testing.T, client *kafka.Client, topic string, params kafka.SearchParams) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	matchCh, progCh, errCh := client.DeepSearch(ctx, topic, params)
	var out []int64
	var last kafka.DeepSearchProgress
	for matchCh != nil || progCh != nil || errCh != nil {
		select {
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			out = append(out, m.Offset)
		case p, ok := <-progCh:
			if !ok {
				progCh = nil
				continue
			}
			last = p
		case e, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			t.Fatalf("scan: %v", e)
		}
	}
	if last.Incomplete() {
		t.Fatalf("scan incomplete: %+v", last)
	}
	slices.Sort(out)
	return out
}
