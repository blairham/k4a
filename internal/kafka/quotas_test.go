// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

func TestParseQuotaEntity(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in, want string
		bad      bool
	}{
		{in: "user=alice", want: "user=alice"},
		{in: " client-id = app , user=alice", want: "user=alice,client-id=app"},
		{in: "user=<default>", want: "user=<default>"},
		{in: "ip=10.0.0.1", want: "ip=10.0.0.1"},
		{in: "", bad: true},
		{in: "user", bad: true},
		{in: "user=", bad: true},
		{in: "group=x", bad: true},
		{in: "user=a,user=b", bad: true},
	} {
		e, err := ParseQuotaEntity(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("ParseQuotaEntity(%q) = %s, want an error", tc.in, e)
			}
			continue
		}
		if err != nil || e.String() != tc.want {
			t.Errorf("ParseQuotaEntity(%q) = %q, %v; want %q", tc.in, e, err, tc.want)
		}
	}
	if e, _ := ParseQuotaEntity("user=<default>"); e[0].Name != nil {
		t.Error("<default> parsed as a literal name rather than the default entity")
	}
}

func TestFormatQuotaValue(t *testing.T) {
	t.Parallel()
	for v, want := range map[float64]string{1048576: "1048576", 12.5: "12.5", 0: "0"} {
		if got := FormatQuotaValue(v); got != want {
			t.Errorf("FormatQuotaValue(%v) = %q, want %q", v, got, want)
		}
	}
}

// TestClientQuotas sets, lists and removes quotas against an in-process
// cluster, including a default entity.
func TestClientQuotas(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	c := newScanTestClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if got, ferr := c.FetchClientQuotas(ctx); ferr != nil || len(got) != 0 {
		t.Fatalf("fresh cluster quotas = %+v, %v; want none", got, ferr)
	}

	alice, _ := ParseQuotaEntity("user=alice,client-id=app")
	users, _ := ParseQuotaEntity("user=<default>")
	rate, pct := 1048576.0, 25.0
	for _, set := range []struct {
		v   *float64
		key string
		e   QuotaEntity
	}{{e: alice, key: "producer_byte_rate", v: &rate}, {e: users, key: "request_percentage", v: &pct}} {
		if aerr := c.AlterClientQuota(ctx, set.e, set.key, set.v); aerr != nil {
			t.Fatalf("set %s %s: %v", set.e, set.key, aerr)
		}
	}

	got, err := c.FetchClientQuotas(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"user=<default> request_percentage=25", "user=alice,client-id=app producer_byte_rate=1048576"}
	if len(got) != len(want) {
		t.Fatalf("quotas = %+v, want %q", got, want)
	}
	for i, q := range got {
		if s := q.Entity.String() + " " + q.Key + "=" + FormatQuotaValue(q.Value); s != want[i] {
			t.Errorf("quota %d = %q, want %q", i, s, want[i])
		}
	}

	if err := c.AlterClientQuota(ctx, alice, "producer_byte_rate", nil); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if got, _ := c.FetchClientQuotas(ctx); len(got) != 1 || got[0].Entity.String() != "user=<default>" {
		t.Errorf("after removing alice's quota: %+v", got)
	}
	if err := c.AlterClientQuota(ctx, nil, "producer_byte_rate", &rate); err == nil {
		t.Error("a quota with no entity was accepted")
	}
}
