// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// TestSearch_CancelUnblocksAbandonedReader: a search whose reader stopped
// reading (a TUI background search that was stopped or replaced) must still
// end when its context is canceled. The relay used to block on the match
// send forever, leaking itself and the scan under it.
func TestSearch_CancelUnblocksAbandonedReader(t *testing.T) {
	t.Parallel()

	fake := &fakeSearcher{clusterID: "cluster-A", hwm: map[int32]int64{0: 1000}}
	// More matches than the cache's 256-slot buffer, so the relay blocks.
	for i := range 400 {
		fake.matches = append(fake.matches, kafka.ConsumedMessage{
			Topic: "t", Offset: int64(i), Value: fmt.Sprintf("foo %d", i),
		})
	}
	cache := New(state.NewRoot(t.TempDir()))
	ctx, cancel := context.WithCancel(context.Background())
	matchCh, progCh, _ := cache.Search(ctx, fake, "t", sampleParams())

	// Let the relay fill the buffer and block, then walk away.
	deadline := time.Now().Add(2 * time.Second)
	for len(matchCh) < cap(matchCh) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(matchCh) < cap(matchCh) {
		t.Fatalf("relay buffered %d of %d; the reader-gone case was not reached", len(matchCh), cap(matchCh))
	}
	cancel()

	// Watch the progress channel, never the match channel: reading matches
	// would unblock the relay's send and hide the leak. The relay closes all
	// three channels when it returns.
	done := make(chan struct{})
	go func() {
		for range progCh { //nolint:revive // wait for close
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("search did not end after cancel: the relay is blocked on a reader that left")
	}
}
