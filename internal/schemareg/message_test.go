// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/blairham/k4a/internal/kafka"
)

func TestOverlay(t *testing.T) {
	base := Config{URL: "https://ctx", Username: "u", Password: "p", CAFile: "/ctx/ca.pem"}
	if got := base.Overlay(Config{}); got != base {
		t.Errorf("empty overlay changed the config: %+v", got)
	}
	got := base.Overlay(Config{URL: "https://flag", Token: "t"})
	want := Config{URL: "https://flag", Token: "t", CAFile: "/ctx/ca.pem"}
	if got != want {
		t.Errorf("token overlay = %+v, want %+v (basic credentials dropped)", got, want)
	}
	got = Config{URL: "https://ctx", Token: "t"}.Overlay(Config{Password: "flag-pass"})
	if got.Token != "" || got.Password != "flag-pass" {
		t.Errorf("password overlay kept the context token: %+v", got)
	}
}

func TestNewIfSet(t *testing.T) {
	r, err := NewIfSet(Config{})
	if r != nil || err != nil {
		t.Errorf("unset config = %v, %v; want nil, nil", r, err)
	}
	// A nil Registry is "decoding off" and must be safe to call.
	m := kafka.ConsumedMessage{Value: "x", RawValue: []byte{0, 0, 0, 0, 1, 'x'}}
	r.DecodeMessage(context.Background(), &m)
	if m.Value != "x" || m.ValueDecoding != (kafka.Decoding{}) {
		t.Errorf("nil registry changed the message: %+v", m)
	}
}

func TestDecodeMessage(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("t-value", 4, 40, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	r := newRegistry(t, reg, Config{})

	value := frame(t, 40, nil, []byte(`{"a": 1}`))
	missing := frame(t, 41, nil, []byte("?"))
	m := kafka.ConsumedMessage{
		Topic:    "t",
		Key:      "hex:0000000029",
		RawKey:   missing,
		Value:    "hex:value",
		RawValue: value,
	}
	r.DecodeMessage(context.Background(), &m)

	if m.Value != `{"a":1}` {
		t.Errorf("Value = %q", m.Value)
	}
	if d := m.ValueDecoding; d.Raw != "hex:value" || d.Schema != "t-value v4 (id 40, JSON)" || d.Reason != "" {
		t.Errorf("ValueDecoding = %+v", d)
	}
	if m.ValueDecoding.Shown(m.Value, true) != "hex:value" {
		t.Error("raw toggle does not show the raw rendering")
	}
	// The key names a schema the registry lacks: it stays raw, with a reason.
	if m.Key != "hex:0000000029" || m.KeyDecoding.Decoded() || !strings.Contains(m.KeyDecoding.Reason, "not found") {
		t.Errorf("Key = %q, KeyDecoding = %+v", m.Key, m.KeyDecoding)
	}
	if m.KeyDecoding.Shown(m.Key, true) != m.Key {
		t.Error("raw toggle changed an undecoded key")
	}
}

func TestDecodeMessageTimeout(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("t-value", 1, 50, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	r := newRegistry(t, reg, Config{})

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	m := kafka.ConsumedMessage{Topic: "t", Value: "raw", RawValue: frame(t, 50, nil, []byte(`{}`))}
	r.DecodeMessage(ctx, &m)
	if m.Value != "raw" || !strings.Contains(m.ValueDecoding.Reason, "in time") {
		t.Errorf("expired decode = %q / %+v", m.Value, m.ValueDecoding)
	}
	// The lookup kept going without the caller, so the next record decodes.
	m = kafka.ConsumedMessage{Topic: "t", Value: "raw", RawValue: frame(t, 50, nil, []byte(`{}`))}
	r.DecodeMessage(context.Background(), &m)
	if m.Value != `{}` {
		t.Errorf("after the lookup landed: %q / %+v", m.Value, m.ValueDecoding)
	}
}
