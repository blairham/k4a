// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// TestReplaceACL swaps one binding for another against an in-process
// cluster, and refuses a replacement that is the binding itself.
func TestReplaceACL(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	c := newScanTestClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	old := CreateACLConfig{
		ResourceType: "topic", ResourceName: "orders", PatternType: "literal",
		Principal: "User:alice", Host: "*", Operation: "read", Permission: "allow",
	}
	if err := c.CreateACL(ctx, old); err != nil {
		t.Fatalf("seed ACL: %v", err)
	}
	acls := func() []ACLEntry {
		t.Helper()
		got, err := c.FetchACLs(ctx)
		if err != nil {
			t.Fatalf("FetchACLs: %v", err)
		}
		return got
	}
	if got := acls(); len(got) != 1 || got[0] != ACLEntry(old) {
		t.Fatalf("seeded ACLs = %+v, want only %+v; the comparison below would prove nothing", got, old)
	}

	if err := c.ReplaceACL(ctx, ACLEntry(old), old); err == nil || !strings.Contains(err.Error(), "identical") {
		t.Errorf("replacing an ACL with itself: err = %v, want a refusal", err)
	}
	if got := acls(); len(got) != 1 {
		t.Fatalf("the refused replacement changed the ACLs: %+v", got)
	}

	next := old
	next.Operation = "write"
	if err := c.ReplaceACL(ctx, ACLEntry(old), next); err != nil {
		t.Fatalf("ReplaceACL: %v", err)
	}
	if got := acls(); len(got) != 1 || got[0] != ACLEntry(next) {
		t.Errorf("after replace ACLs = %+v, want only %+v", got, next)
	}

	bad := next
	bad.Operation = "fly"
	if err := c.ReplaceACL(ctx, ACLEntry(next), bad); err == nil || !strings.Contains(err.Error(), "unchanged") {
		t.Errorf("an invalid replacement: err = %v, want the old ACL reported unchanged", err)
	}
	if got := acls(); len(got) != 1 || got[0] != ACLEntry(next) {
		t.Errorf("a failed create still removed the old ACL: %+v", got)
	}
}
