// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", d, msg)
}

func TestAsync_FlushErrorsAreReported(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	// Close the underlying indexer out from under the drain goroutine so the
	// next IndexBatch fails — the class of invisible flush failure.
	if err := ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	type flushErr struct {
		err     error
		stage   string
		records int
	}
	errs := make(chan flushErr, 8)
	a := NewAsyncWithOptions(ix, AsyncOptions{
		OnFlushError: func(stage string, records int, err error) {
			errs <- flushErr{stage: stage, records: records, err: err}
		},
	})
	t.Cleanup(func() { _ = a.Stop() })

	// A full batch forces an immediate flush without waiting for the ticker.
	base := time.Now()
	for i := range asyncBatchSize {
		a.Submit(sampleMsg(0, int64(i), "k", "v", base))
	}

	select {
	case e := <-errs:
		if e.stage != "index" {
			t.Errorf("stage = %q, want index", e.stage)
		}
		if e.records != asyncBatchSize {
			t.Errorf("records = %d, want %d", e.records, asyncBatchSize)
		}
		if e.err == nil {
			t.Error("expected a non-nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush error was never reported")
	}
}

func TestAsync_LastFlushAdvancesAndBufferDrains(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	a := NewAsync(ix)
	t.Cleanup(func() { _ = a.Stop() })

	start := a.LastFlush()
	if start.IsZero() {
		t.Fatal("LastFlush should be initialized at construction")
	}

	base := time.Now()
	for i := range asyncBatchSize {
		if !a.Submit(sampleMsg(0, int64(i), "k", "v", base)) {
			t.Fatalf("Submit(%d) rejected", i)
		}
	}

	waitFor(t, 5*time.Second, func() bool {
		return a.Buffered() == 0 && a.LastFlush().After(start)
	}, "drain should empty the buffer and stamp LastFlush")

	docs, err := a.Inner().DocCount()
	if err != nil {
		t.Fatalf("DocCount: %v", err)
	}
	if docs != asyncBatchSize {
		t.Errorf("docs = %d, want %d", docs, asyncBatchSize)
	}
}
