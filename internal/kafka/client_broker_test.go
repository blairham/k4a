// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ---------------------------------------------------------------------------
// recordToConsumed
// ---------------------------------------------------------------------------

func TestRecordToConsumed(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	r := &kgo.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-key"),
		Value:     []byte(`{"id":1}`),
		Timestamp: ts,
		Headers: []kgo.RecordHeader{
			{Key: "type", Value: []byte("created")},
			{Key: "source", Value: []byte("api")},
		},
	}

	cm := recordToConsumed(r)

	if cm.Topic != "orders" {
		t.Errorf("Topic = %q, want %q", cm.Topic, "orders")
	}
	if cm.Partition != 3 {
		t.Errorf("Partition = %d, want 3", cm.Partition)
	}
	if cm.Offset != 42 {
		t.Errorf("Offset = %d, want 42", cm.Offset)
	}
	if cm.Key != "order-key" {
		t.Errorf("Key = %q, want %q", cm.Key, "order-key")
	}
	if cm.Value != `{"id":1}` {
		t.Errorf("Value = %q, want %q", cm.Value, `{"id":1}`)
	}
	if !cm.Time.Equal(ts) {
		t.Errorf("Time = %v, want %v", cm.Time, ts)
	}
	if len(cm.Headers) != 2 {
		t.Fatalf("len(Headers) = %d, want 2", len(cm.Headers))
	}
	if cm.Headers[0].Key != "type" || cm.Headers[0].Value != "created" {
		t.Errorf("Headers[0] = {%q, %q}, want {type, created}", cm.Headers[0].Key, cm.Headers[0].Value)
	}
	if cm.Headers[1].Key != "source" || cm.Headers[1].Value != "api" {
		t.Errorf("Headers[1] = {%q, %q}, want {source, api}", cm.Headers[1].Key, cm.Headers[1].Value)
	}
}

// ---------------------------------------------------------------------------
// NewClient
// ---------------------------------------------------------------------------

func TestNewClient(t *testing.T) {
	t.Parallel()

	brokers := "broker1:9092,broker2:9092"
	c, err := NewClient(AuthConfig{Method: AuthPlaintext}, brokers)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(c.Close)

	if c.brokers != brokers {
		t.Errorf("brokers = %q, want %q", c.brokers, brokers)
	}
	if c.admin == nil {
		t.Error("admin should not be nil")
	}
	if c.kgoClient == nil {
		t.Error("kgoClient should not be nil")
	}
}

// ---------------------------------------------------------------------------
// CloseIdleConnections — rebuilds the underlying client without panicking
// ---------------------------------------------------------------------------

func TestCloseIdleConnections(t *testing.T) {
	t.Parallel()

	c, err := NewClient(AuthConfig{Method: AuthPlaintext}, "broker1:9092")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(c.Close)

	prev := c.kgoClient
	c.CloseIdleConnections()
	if c.kgoClient == prev {
		t.Error("CloseIdleConnections should have replaced the kgoClient")
	}
	if c.kgoClient == nil {
		t.Error("CloseIdleConnections left the kgoClient nil")
	}
}
