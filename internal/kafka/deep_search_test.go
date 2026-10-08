// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"regexp"
	"testing"
)

func TestPlanReverseChunks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		first, last int64
		wantFirst   chunkSpan // newest chunk (first in slice)
		wantLast    chunkSpan // oldest chunk (last in slice)
		wantTotal   int64     // sum of chunk sizes must equal last-first
		wantCount   int       // number of chunks (only checked if > 0)
	}{
		{
			name:      "tiny range smaller than initial chunk",
			first:     0,
			last:      100,
			wantFirst: chunkSpan{start: 0, end: 100},
			wantLast:  chunkSpan{start: 0, end: 100},
			wantTotal: 100,
			wantCount: 1,
		},
		{
			name:      "exactly initial chunk",
			first:     0,
			last:      deepSearchInitialChunk,
			wantFirst: chunkSpan{start: 0, end: deepSearchInitialChunk},
			wantLast:  chunkSpan{start: 0, end: deepSearchInitialChunk},
			wantTotal: deepSearchInitialChunk,
			wantCount: 1,
		},
		{
			name:  "doubles up: initial + 2x + 4x",
			first: 0,
			// 5000 + 10000 + 20000 = 35000
			last:      deepSearchInitialChunk * 7,
			wantFirst: chunkSpan{start: 30_000, end: 35_000},
			wantLast:  chunkSpan{start: 0, end: 5_000},
			wantTotal: deepSearchInitialChunk * 7,
		},
		{
			name:      "empty range returns nil",
			first:     500,
			last:      500,
			wantTotal: 0,
		},
		{
			name:      "inverted range returns nil",
			first:     1000,
			last:      500,
			wantTotal: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			spans := planReverseChunks(tt.first, tt.last)

			if tt.last <= tt.first {
				if spans != nil {
					t.Fatalf("planReverseChunks(%d,%d) = %v, want nil", tt.first, tt.last, spans)
				}
				return
			}

			// Total coverage must equal the input range.
			var total int64
			for _, s := range spans {
				total += s.end - s.start
			}
			if total != tt.wantTotal {
				t.Errorf("total covered = %d, want %d (spans: %v)", total, tt.wantTotal, spans)
			}

			// Newest chunk (first in slice) ends at `last`.
			if spans[0].end != tt.last {
				t.Errorf("spans[0].end = %d, want %d", spans[0].end, tt.last)
			}
			// Oldest chunk (last in slice) starts at `first`.
			if spans[len(spans)-1].start != tt.first {
				t.Errorf("spans[last].start = %d, want %d", spans[len(spans)-1].start, tt.first)
			}

			// Spans are contiguous newest-first: spans[i].start == spans[i+1].end.
			for i := 0; i < len(spans)-1; i++ {
				if spans[i].start != spans[i+1].end {
					t.Errorf("non-contiguous: spans[%d]=%v spans[%d]=%v",
						i, spans[i], i+1, spans[i+1])
				}
			}

			// First chunk size is initial-chunk-sized (or smaller if the
			// whole range fits in one chunk).
			firstSize := spans[0].end - spans[0].start
			if firstSize > deepSearchInitialChunk {
				t.Errorf("first chunk size = %d, want <= %d", firstSize, deepSearchInitialChunk)
			}

			// Chunk sizes monotonically grow (or stay equal at the cap)
			// from newest to oldest, until possibly being clipped at
			// `first` (the very last chunk).
			for i := 1; i < len(spans)-1; i++ {
				prevSize := spans[i-1].end - spans[i-1].start
				thisSize := spans[i].end - spans[i].start
				if thisSize < prevSize {
					t.Errorf("non-monotonic growth at i=%d: %d -> %d",
						i, prevSize, thisSize)
				}
				if thisSize > deepSearchMaxChunk {
					t.Errorf("chunk exceeds max: %d > %d", thisSize, deepSearchMaxChunk)
				}
			}

			if tt.wantCount > 0 && len(spans) != tt.wantCount {
				t.Errorf("len(spans) = %d, want %d", len(spans), tt.wantCount)
			}
		})
	}
}

func TestPlanReverseChunks_HugeRange(t *testing.T) {
	t.Parallel()

	// One billion offsets: confirms we cap chunk size and don't blow up.
	spans := planReverseChunks(0, 1_000_000_000)
	if len(spans) == 0 {
		t.Fatal("expected non-zero spans")
	}
	for _, s := range spans {
		size := s.end - s.start
		if size > deepSearchMaxChunk {
			t.Fatalf("chunk size %d exceeds max %d", size, deepSearchMaxChunk)
		}
	}
}

func TestSubWorkerCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeSize  int64
		partitions int
		want       int
	}{
		// Single-partition topics keep the original per-partition behavior:
		// the whole client budget is theirs to spend.
		{"empty range", 0, 1, 1},
		{"negative range", -100, 1, 1},
		{"tiny partition", 1_000, 1, 1},
		{"just under one worker", deepSearchOffsetsPerSubWorker - 1, 1, 1},
		{"exactly one worker threshold", deepSearchOffsetsPerSubWorker, 1, 1},
		{"two workers", deepSearchOffsetsPerSubWorker * 2, 1, 2},
		{"cap reached", deepSearchOffsetsPerSubWorker * 10, 1, deepSearchSubWorkerCap},
		{"huge partition", 1_000_000_000, 1, deepSearchSubWorkerCap},

		// Partitions are scanned concurrently, so the budget is shared. A
		// 6-partition topic must not open 6 x cap clients.
		{"six partitions share the budget", 1_000_000_000, 6, 1},
		{"two partitions split the budget", 1_000_000_000, 2, deepSearchScanClientBudget / 2},
		{"more partitions than budget still get one each", 1_000_000_000, 64, 1},

		// A small range is still the binding constraint when it is smaller
		// than the budget share.
		{"small range beats a generous share", deepSearchOffsetsPerSubWorker, 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := subWorkerCount(tt.rangeSize, tt.partitions); got != tt.want {
				t.Errorf("subWorkerCount(%d, %d) = %d, want %d",
					tt.rangeSize, tt.partitions, got, tt.want)
			}
		})
	}
}

// The whole point of the budget: total scan clients across a concurrent
// multi-partition scan stay bounded. Before this, an unbounded search of a
// 6-partition topic opened 48 and made no progress at all.
func TestSubWorkerCount_TotalFanOutIsBounded(t *testing.T) {
	t.Parallel()

	const hugeRange = int64(1_000_000_000)
	for _, partitions := range []int{1, 2, 3, 6, 12, 50, 200} {
		total := subWorkerCount(hugeRange, partitions) * partitions
		// Every partition gets at least one client, so with more partitions
		// than budget the floor is the partition count itself.
		limit := max(deepSearchScanClientBudget, partitions)
		if total > limit {
			t.Errorf("partitions=%d: total scan clients = %d, want <= %d",
				partitions, total, limit)
		}
	}
}

// Never starve a partition — zero sub-workers would silently drop its matches.
func TestSubWorkerCount_NeverZero(t *testing.T) {
	t.Parallel()

	for _, partitions := range []int{1, 6, 64, 1_000} {
		for _, size := range []int64{-1, 0, 1, deepSearchOffsetsPerSubWorker * 100} {
			if got := subWorkerCount(size, partitions); got < 1 {
				t.Errorf("subWorkerCount(%d, %d) = %d, want >= 1", size, partitions, got)
			}
		}
	}
}

func TestExtractPrefilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    string // empty string means nil
	}{
		{"plain literal", "foo", "foo"},
		{"anchored literal", "^foo", "foo"},
		{"literal then wildcard", "foo.*bar", "foo"},
		{"alternation no prefix", "foo|bar", ""},
		{"wildcard start", ".*foo", ""},
		{"single char prefix skipped", "a.*", ""},
		{"empty regex", "", ""},
		{"escaped meta", `foo\.bar`, "foo.bar"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := regexp.MustCompile(tt.pattern)
			got := extractPrefilter(re)
			if tt.want == "" {
				if got != nil {
					t.Errorf("extractPrefilter(%q) = %q, want nil", tt.pattern, got)
				}
				return
			}
			if string(got) != tt.want {
				t.Errorf("extractPrefilter(%q) = %q, want %q", tt.pattern, got, tt.want)
			}
		})
	}
}
