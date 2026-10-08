// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// multiChunkRecords is deliberately larger than deepSearchInitialChunk so the
// plan spans more than one chunk — the exact condition that used to corrupt
// the scan. A single-chunk range never exercised the bug.
const multiChunkRecords = int(deepSearchInitialChunk) + 2_500

// TestDeepSearchMultiChunkIsCompleteAndUnique is the regression proof for
// a bug where a scan whose range exceeds one chunk silently dropped the
// newest chunk and emitted the oldest one twice.
//
// The root cause was positional. Each sub-worker built its client with
// ConsumePartitions().At(r.first) and then called SetOffsets(span.start)
// before its first PollFetches. franz-go documents SetOffsets as valid only
// for partitions "previously returned from a PollFetches"; before the first
// poll the partition's offset load has not completed, so assignSetMatching
// finds no cursor to set and the call is a silent no-op (see
// kgo/consumer.go, assignPartitions). The constructor's oldest-offset
// position therefore won, and the newest chunk was scanned with the oldest
// records.
//
// The assertions below are the two user-visible symptoms, not the mechanism:
// every offset exactly once, and the newest record actually reached.
func TestDeepSearchMultiChunkIsCompleteAndUnique(t *testing.T) {
	t.Parallel()

	const topic = "deep.search.multichunk.v1"
	seeds := newScanFakeCluster(t, topic)
	produceScanRecords(t, seeds, topic, multiChunkRecords)

	client := newScanTestClient(t, seeds)

	// Sanity-check the premise: this range really does span >1 chunk.
	if spans := planReverseChunks(0, int64(multiChunkRecords)); len(spans) < 2 {
		t.Fatalf("test premise broken: want a multi-chunk plan, got %d span(s)", len(spans))
	}

	msgs := runScan(t, client, topic, SearchParams{
		Pattern: regexp.MustCompile("scan-record"),
		Cap:     10 * multiChunkRecords, // never bind, so a short read can't hide behind the cap
	})

	seen := make(map[int64]int, multiChunkRecords)
	for _, m := range msgs {
		seen[m.Offset]++
	}

	var dupes []int64
	for off, n := range seen {
		if n > 1 {
			dupes = append(dupes, off)
		}
	}
	if len(dupes) > 0 {
		t.Errorf("scan emitted %d duplicated offsets (total emitted %d, unique %d)",
			len(dupes), len(msgs), len(seen))
	}

	if len(seen) != multiChunkRecords {
		t.Errorf("scan covered %d unique offsets, want %d (missing %d)",
			len(seen), multiChunkRecords, multiChunkRecords-len(seen))
	}

	// The newest record is the one a truncated scan loses, and the one a
	// user is most likely to care about.
	newest := int64(multiChunkRecords - 1)
	if seen[newest] == 0 {
		var maxOff int64 = -1
		for off := range seen {
			maxOff = max(maxOff, off)
		}
		t.Errorf("scan never reached the newest record: max offset %d, high watermark %d",
			maxOff, newest)
	}
}

// TestDeepSearchSingleChunkIsComplete pins the control case from the issue's
// reproduction table — a range inside one chunk was always correct, and must
// stay that way.
func TestDeepSearchSingleChunkIsComplete(t *testing.T) {
	t.Parallel()

	const topic = "deep.search.singlechunk.v1"
	const n = 400

	seeds := newScanFakeCluster(t, topic)
	produceScanRecords(t, seeds, topic, n)
	client := newScanTestClient(t, seeds)

	msgs := runScan(t, client, topic, SearchParams{
		Pattern: regexp.MustCompile("scan-record"),
	})

	seen := make(map[int64]struct{}, n)
	for _, m := range msgs {
		seen[m.Offset] = struct{}{}
	}
	if len(msgs) != n || len(seen) != n {
		t.Errorf("single-chunk scan emitted %d messages / %d unique offsets, want %d of each",
			len(msgs), len(seen), n)
	}
}

// TestDeepSearchReportsTruncationOnPartialScan proves the silent-short-read
// guard: when a scan cannot reach the end of its range, the terminal progress
// must say so rather than reporting a clean completion. A cap that binds is
// the one truncation we can force deterministically.
func TestDeepSearchReportsTruncationOnPartialScan(t *testing.T) {
	t.Parallel()

	const topic = "deep.search.capped.v1"
	seeds := newScanFakeCluster(t, topic)
	produceScanRecords(t, seeds, topic, multiChunkRecords)
	client := newScanTestClient(t, seeds)

	_, prog := runScanWithProgress(t, client, topic, SearchParams{
		Pattern: regexp.MustCompile("scan-record"),
		Cap:     10,
	})

	if !prog.Capped {
		t.Errorf("terminal progress does not report the match cap as hit")
	}
	if !prog.Incomplete() {
		t.Errorf("a capped scan must report Incomplete(); got Capped=%v Truncated=%v",
			prog.Capped, prog.Truncated)
	}
}

// runScan drains a DeepSearch to completion and returns the matches.
func runScan(t *testing.T, c *Client, topic string, params SearchParams) []ConsumedMessage {
	t.Helper()
	msgs, _ := runScanWithProgress(t, c, topic, params)
	return msgs
}

// runScanWithProgress drains a DeepSearch, returning the matches and the
// terminal progress snapshot.
func runScanWithProgress(
	t *testing.T, c *Client, topic string, params SearchParams,
) ([]ConsumedMessage, DeepSearchProgress) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	matchCh, progCh, errCh := c.DeepSearch(ctx, topic, params)

	var (
		msgs     []ConsumedMessage
		lastProg DeepSearchProgress
	)
	for matchCh != nil || progCh != nil {
		select {
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			msgs = append(msgs, m)
		case p, ok := <-progCh:
			if !ok {
				progCh = nil
				continue
			}
			lastProg = p
		case <-ctx.Done():
			t.Fatal("deep search did not finish before the deadline")
		}
	}
	for err := range errCh {
		if err != nil {
			t.Fatalf("deep search: %v", err)
		}
	}
	return msgs, lastProg
}

// newScanFakeCluster spins up a single-broker, single-partition kfake seeded
// with topic. One partition keeps offsets directly comparable to counts.
func newScanFakeCluster(t *testing.T, topic string) []string {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	return fake.ListenAddrs()
}

// newScanTestClient dials the fake with plaintext auth.
func newScanTestClient(t *testing.T, seeds []string) *Client {
	t.Helper()
	client, err := NewClient(AuthConfig{Method: AuthPlaintext}, strings.Join(seeds, ","))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// produceScanRecords writes n matching records, each tagged with its own
// offset so a duplicate is identifiable by value as well as by offset.
func produceScanRecords(t *testing.T, seeds []string, topic string, n int) {
	t.Helper()

	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Produced async and flushed once; ProduceSync per record is far too slow
	// at this record count.
	for i := range n {
		cl.Produce(ctx, &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: fmt.Appendf(nil, `{"tag":"scan-record","seq":%d}`, i),
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}
