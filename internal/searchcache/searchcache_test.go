// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcache

import (
	"container/list"
	"context"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// fakeSearcher is a tiny stand-in for *kafka.Client that satisfies the
// cache's Searcher interface. Each invocation replays a canned response.
type fakeSearcher struct {
	clusterID string
	hwm       map[int32]int64
	matches   []kafka.ConsumedMessage
	scanned   int64
	bytes     int64
	calls     int
	capped    bool
}

func (f *fakeSearcher) ClusterID(context.Context) (string, error) {
	return f.clusterID, nil
}

func (f *fakeSearcher) EndOffsets(context.Context, string) (map[int32]int64, error) {
	out := make(map[int32]int64, len(f.hwm))
	for k, v := range f.hwm {
		out[k] = v
	}
	return out, nil
}

func (f *fakeSearcher) DeepSearch(_ context.Context, _ string, _ kafka.SearchParams) (
	<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
) {
	f.calls++
	matchCh := make(chan kafka.ConsumedMessage, len(f.matches))
	progCh := make(chan kafka.DeepSearchProgress, 1)
	errCh := make(chan error, 1)
	go func() {
		defer close(matchCh)
		defer close(progCh)
		defer close(errCh)
		for _, m := range f.matches {
			matchCh <- m
		}
		progCh <- kafka.DeepSearchProgress{
			Scanned:        f.scanned,
			Bytes:          f.bytes,
			Matches:        len(f.matches),
			Partitions:     len(f.hwm),
			DonePartitions: len(f.hwm),
			Capped:         f.capped,
			Done:           true,
		}
	}()
	return matchCh, progCh, errCh
}

func sampleParams() kafka.SearchParams {
	return kafka.SearchParams{
		Pattern:    regexp.MustCompile("(?i)foo"),
		Scope:      kafka.ScopeKey | kafka.ScopeValue,
		Partitions: []int32{0, 1},
	}
}

func collectAll(matchCh <-chan kafka.ConsumedMessage, progCh <-chan kafka.DeepSearchProgress, errCh <-chan error) (
	[]kafka.ConsumedMessage, kafka.DeepSearchProgress, error,
) {
	var matches []kafka.ConsumedMessage
	var last kafka.DeepSearchProgress
	var err error
	for matchCh != nil || progCh != nil || errCh != nil {
		select {
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			matches = append(matches, m)
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
			if e != nil {
				err = e
			}
		}
	}
	return matches, last, err
}

func TestSearch_MissThenHit_HWMUnchanged(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	fake := &fakeSearcher{
		clusterID: "cluster-A",
		hwm:       map[int32]int64{0: 100, 1: 200},
		matches: []kafka.ConsumedMessage{
			{Topic: "t", Partition: 0, Offset: 1, Key: "k", Value: "foo"},
		},
		scanned: 300,
		bytes:   1234,
	}

	matches, prog, err := collectAll(cache.Search(context.Background(), fake, "t", sampleParams()))
	if err != nil {
		t.Fatalf("first search err = %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("first matches = %d, want 1", len(matches))
	}
	if prog.Source != kafka.SourceScan {
		t.Errorf("first Source = %v, want SourceScan", prog.Source)
	}
	if fake.calls != 1 {
		t.Errorf("first calls = %d, want 1", fake.calls)
	}

	// Second invocation with unchanged HWM should hit the cache and not
	// call DeepSearch again.
	matches, prog, err = collectAll(cache.Search(context.Background(), fake, "t", sampleParams()))
	if err != nil {
		t.Fatalf("second search err = %v", err)
	}
	if len(matches) != 1 {
		t.Errorf("second matches = %d, want 1", len(matches))
	}
	if prog.Source != kafka.SourceCache {
		t.Errorf("second Source = %v, want SourceCache", prog.Source)
	}
	if fake.calls != 1 {
		t.Errorf("expected cache hit; DeepSearch was called %d times", fake.calls)
	}
}

func TestSearch_HWMAdvanced_ForcesRescan(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	fake := &fakeSearcher{
		clusterID: "cluster-A",
		hwm:       map[int32]int64{0: 100, 1: 200},
		matches:   []kafka.ConsumedMessage{{Topic: "t", Offset: 1}},
	}

	_, _, _ = collectAll(cache.Search(context.Background(), fake, "t", sampleParams()))
	if fake.calls != 1 {
		t.Fatalf("first calls = %d, want 1", fake.calls)
	}

	// Move one partition's HWM forward — the cached entry is now stale.
	fake.hwm[0] = 101

	_, prog, _ := collectAll(cache.Search(context.Background(), fake, "t", sampleParams()))
	if fake.calls != 2 {
		t.Errorf("expected re-scan, calls = %d, want 2", fake.calls)
	}
	if prog.Source != kafka.SourceScan {
		t.Errorf("Source = %v, want SourceScan after HWM advance", prog.Source)
	}
}

func TestSearch_CappedScansDoNotSeedCache(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	fake := &fakeSearcher{
		clusterID: "cluster-A",
		hwm:       map[int32]int64{0: 100},
		matches:   []kafka.ConsumedMessage{{Topic: "t"}},
		capped:    true,
	}

	_, _, _ = collectAll(cache.Search(context.Background(), fake, "t", sampleParams()))
	if cache.Len() != 0 {
		t.Errorf("capped scan seeded the cache; Len = %d, want 0", cache.Len())
	}
}

func TestBuildKey_PartitionOrderDoesntMatter(t *testing.T) {
	t.Parallel()

	a := BuildKey("c", "t", kafka.SearchParams{
		Pattern:    regexp.MustCompile("foo"),
		Partitions: []int32{2, 0, 1},
	})
	b := BuildKey("c", "t", kafka.SearchParams{
		Pattern:    regexp.MustCompile("foo"),
		Partitions: []int32{0, 1, 2},
	})
	if a.Hash() != b.Hash() {
		t.Errorf("hashes differ; partition order leaked into key.\nA=%+v\nB=%+v", a, b)
	}
}

func TestCache_LRUEvictsOldestFromMemory(t *testing.T) {
	t.Parallel()

	// Use a nil-root cache so disk fallback is disabled — that lets us
	// test in-memory eviction in isolation. Disk-rehydrate is exercised
	// by TestCache_PersistsAcrossInstances.
	cache := &Cache{
		root:   nil,
		lru:    list.New(),
		byHash: make(map[string]*list.Element),
		maxMem: 3,
	}

	for i := range 5 {
		k := Key{Cluster: "c", Topic: "t", Pattern: string(rune('a' + i))}
		cache.Put(&Entry{Key: k, CreatedAt: time.Now()})
	}
	if cache.Len() != 3 {
		t.Errorf("Len = %d, want 3", cache.Len())
	}
	// Earliest entries should be gone from the in-memory map.
	if _, ok := cache.Get(Key{Cluster: "c", Topic: "t", Pattern: "a"}); ok {
		t.Errorf("entry 'a' should have been evicted (oldest)")
	}
	if _, ok := cache.Get(Key{Cluster: "c", Topic: "t", Pattern: "b"}); ok {
		t.Errorf("entry 'b' should have been evicted (second oldest)")
	}
	if _, ok := cache.Get(Key{Cluster: "c", Topic: "t", Pattern: "e"}); !ok {
		t.Errorf("entry 'e' should still be present")
	}
}

func TestCache_PersistsAcrossInstances(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := state.NewRoot(dir)

	first := New(root)
	first.Put(&Entry{
		Key:       Key{Cluster: "c", Topic: "t", Pattern: "foo"},
		Matches:   []kafka.ConsumedMessage{{Topic: "t", Offset: 1}},
		HWM:       map[int32]int64{0: 10},
		CreatedAt: time.Now(),
	})

	// Fresh cache pointed at the same disk dir should find the entry on disk.
	second := New(root)
	entry, ok := second.Get(Key{Cluster: "c", Topic: "t", Pattern: "foo"})
	if !ok {
		t.Fatal("expected disk round-trip to find the entry")
	}
	if len(entry.Matches) != 1 {
		t.Errorf("matches = %d, want 1", len(entry.Matches))
	}
}

func TestCache_SchemaMismatchOnDisk_DropsEntry(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	root := state.NewRoot(dir)
	cache := New(root)
	k := Key{Cluster: "c", Topic: "t", Pattern: "foo"}

	// Hand-write a file with a bogus schema_version.
	if err := os.MkdirAll(cache.dirPath(), 0o700); err != nil {
		t.Fatal(err)
	}
	bad := []byte(`{"schema_version":9999,"key":{"Pattern":"foo"}}`)
	if err := os.WriteFile(cache.diskPath(k.Hash()), bad, 0o600); err != nil {
		t.Fatal(err)
	}

	_, ok := cache.Get(k)
	if ok {
		t.Errorf("entry with mismatched schema_version should not be served")
	}
	if _, err := os.Stat(cache.diskPath(k.Hash())); !os.IsNotExist(err) {
		t.Errorf("bad-schema file should have been removed, stat err = %v", err)
	}
}

func TestCache_Clear_RemovesMemoryAndDisk(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	cache := New(root)
	cache.Put(&Entry{
		Key:       Key{Cluster: "c", Topic: "t", Pattern: "foo"},
		CreatedAt: time.Now(),
	})

	if err := cache.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if cache.Len() != 0 {
		t.Errorf("Len after Clear = %d, want 0", cache.Len())
	}
	if _, err := os.Stat(cache.dirPath()); !os.IsNotExist(err) {
		t.Errorf("dir after Clear stat err = %v, want NotExist", err)
	}
}

func TestSearch_ContextCanceled_StopsCleanly(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	fake := &fakeSearcher{
		clusterID: "c",
		hwm:       map[int32]int64{0: 100},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before issuing
	_, _, _ = collectAll(cache.Search(ctx, fake, "t", sampleParams()))
	// We don't assert specific cache state here — just that we don't hang
	// or panic. The test's mere completion is the assertion.
}

// ---------------------------------------------------------------------------
// IsValidAgainst — closed-window relaxation (issue #48)
// ---------------------------------------------------------------------------

func TestIsValidAgainst_OpenWindow_StrictEquality(t *testing.T) {
	t.Parallel()

	entry := &Entry{
		HWM:       map[int32]int64{0: 100, 1: 200},
		CreatedAt: time.Now().UTC(),
	}

	if !entry.IsValidAgainst(map[int32]int64{0: 100, 1: 200}, time.Time{}) {
		t.Error("open window with identical HWM should be valid")
	}
	if entry.IsValidAgainst(map[int32]int64{0: 101, 1: 200}, time.Time{}) {
		t.Error("open window with advanced HWM should be stale")
	}
}

func TestIsValidAgainst_ClosedPastWindow_AcceptsAdvancedHWM(t *testing.T) {
	t.Parallel()

	until := time.Now().Add(-1 * time.Hour) // closed window in the past
	entry := &Entry{
		HWM:       map[int32]int64{0: 100, 1: 200},
		CreatedAt: until.Add(10 * time.Minute), // scan completed well after until
	}

	// HWM advanced — open-window logic would reject this. Closed past
	// window accepts it because the new records can't fall inside
	// [since, until].
	if !entry.IsValidAgainst(map[int32]int64{0: 200, 1: 300}, until) {
		t.Error("closed past window with advanced HWM should still be valid")
	}
}

func TestIsValidAgainst_ClosedPastWindow_RejectsShrunkHWM(t *testing.T) {
	t.Parallel()

	until := time.Now().Add(-1 * time.Hour)
	entry := &Entry{
		HWM:       map[int32]int64{0: 100, 1: 200},
		CreatedAt: until.Add(10 * time.Minute),
	}

	// A partition that shrank below the cached HWM means records covered
	// by the scan no longer exist on the broker. Treat as stale.
	if entry.IsValidAgainst(map[int32]int64{0: 50, 1: 200}, until) {
		t.Error("closed past window with shrunk HWM should be stale")
	}
}

func TestIsValidAgainst_ClosedWindowWithoutMargin_StaysStrict(t *testing.T) {
	t.Parallel()

	// until is in the future relative to CreatedAt — the scan finished
	// before the window closed. We can't tell if matching records
	// arrived after the scan; fall back to strict equality.
	until := time.Now().Add(time.Hour)
	entry := &Entry{
		HWM:       map[int32]int64{0: 100},
		CreatedAt: time.Now(),
	}

	if !entry.IsValidAgainst(map[int32]int64{0: 100}, until) {
		t.Error("identical HWM should still be valid even when window isn't safely past")
	}
	if entry.IsValidAgainst(map[int32]int64{0: 101}, until) {
		t.Error("advanced HWM with window not safely past should be stale")
	}
}

func TestIsValidAgainst_PartitionCountChanged(t *testing.T) {
	t.Parallel()

	until := time.Now().Add(-1 * time.Hour)
	entry := &Entry{
		HWM:       map[int32]int64{0: 100, 1: 200},
		CreatedAt: until.Add(10 * time.Minute),
	}

	// A new partition appeared (size mismatch) — we have no data for
	// it, so we can't tell whether it would have matched. Always stale,
	// even with a generous closed window.
	if entry.IsValidAgainst(map[int32]int64{0: 100, 1: 200, 2: 50}, until) {
		t.Error("partition count change should always invalidate")
	}
}
