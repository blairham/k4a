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

// dynamicBrokerValue returns the broker's dynamic override for key, if any.
func dynamicBrokerValue(t *testing.T, c *Client, brokerID, key string) (string, bool) {
	t.Helper()
	entries, err := c.FetchBrokerConfigs(context.Background(), brokerID)
	if err != nil {
		t.Fatalf("FetchBrokerConfigs: %v", err)
	}
	for _, e := range entries {
		if e.Name == key && e.Source == "DYNAMIC_BROKER" {
			return e.Value, true
		}
	}
	return "", false
}

// TestAlterBrokerConfig sets a per-broker override, then deletes it, against
// an in-process cluster.
func TestAlterBrokerConfig(t *testing.T) {
	fake, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	c := newScanTestClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const broker, key = "0", "log.retention.ms"
	if _, ok := dynamicBrokerValue(t, c, broker, key); ok {
		t.Fatalf("%s already has a dynamic override; the test would prove nothing", key)
	}

	v := "3600000"
	if err := c.AlterBrokerConfig(ctx, broker, key, &v); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, ok := dynamicBrokerValue(t, c, broker, key); !ok || got != v {
		t.Fatalf("after set: override = %q (present %v), want %q", got, ok, v)
	}

	if err := c.AlterBrokerConfig(ctx, broker, key, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, ok := dynamicBrokerValue(t, c, broker, key); ok {
		t.Fatalf("after delete: override %q still present", got)
	}

	// The broker, not k4a, decides which keys are dynamic: kfake accepts
	// keys a real broker would refuse, so that refusal is not tested here.
	if err := c.AlterBrokerConfig(ctx, "b1", key, &v); err == nil || !strings.Contains(err.Error(), "invalid broker ID") {
		t.Errorf("non-numeric broker: err = %v, want invalid broker ID", err)
	}
}
