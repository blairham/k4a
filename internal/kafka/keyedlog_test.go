// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// TestKeyedLogSnapshotAndTail: a missing topic is created compacted, the
// snapshot is the latest value per key with tombstoned keys gone, and the tail
// picks up exactly the writes made after the snapshot.
func TestKeyedLogSnapshotAndTail(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(fake.Close)
	c, err := NewClient(AuthConfig{Method: AuthPlaintext}, fake.ListenAddrs()[0])
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const topic = "k4a-index.followset"
	kl, err := c.OpenKeyedLog(ctx, topic)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer kl.Close()
	policy, err := kl.CleanupPolicy(ctx)
	if err != nil || policy != "compact" {
		t.Errorf("cleanup.policy = %q (%v), want compact", policy, err)
	}
	// Opening again finds the topic rather than failing to create it.
	again, err := c.OpenKeyedLog(ctx, topic)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	again.Close()

	for _, w := range []struct {
		key   string
		value []byte
	}{{"a", []byte("1")}, {"b", []byte("2")}, {"a", []byte("3")}, {"b", nil}, {"c", []byte("4")}} {
		if err = kl.Put(ctx, w.key, w.value); err != nil {
			t.Fatalf("put %s: %v", w.key, err)
		}
	}
	table, next, err := kl.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(table) != 2 || string(table["a"]) != "3" || string(table["c"]) != "4" {
		t.Errorf("snapshot = %q, want a=3 c=4 and b deleted", table)
	}

	if err := kl.Put(ctx, "d", []byte("5")); err != nil {
		t.Fatalf("put d: %v", err)
	}
	if err := kl.Put(ctx, "a", nil); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	type write struct {
		key     string
		deleted bool
	}
	got := make(chan write, 8)
	tailCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- kl.Tail(tailCtx, next, func(key string, value []byte, _ int32, _ int64) {
			got <- write{key: key, deleted: value == nil}
		})
	}()
	want := []write{{key: "d"}, {key: "a", deleted: true}}
	for i, w := range want {
		select {
		case g := <-got:
			if g != w {
				t.Errorf("tail write %d = %+v, want %+v", i, g, w)
			}
		case <-ctx.Done():
			t.Fatalf("tail delivered %d of %d writes", i, len(want))
		}
	}
	stop()
	if err := <-done; err != nil {
		t.Errorf("tail returned %v after cancel", err)
	}
	select {
	case g := <-got:
		t.Errorf("tail delivered %+v, which the snapshot already covered", g)
	default:
	}
}
