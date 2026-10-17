// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/blairham/k4a/internal/kafka"
)

func framedMessage(t *testing.T, id int, body string) kafka.ConsumedMessage {
	t.Helper()
	raw := frame(t, id, nil, []byte(body))
	return kafka.ConsumedMessage{Topic: "k", Value: "hex:00", RawValue: raw}
}

func TestDecodeRecordSettled(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("k-value", 1, 7, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	r := newRegistry(t, reg, Config{})

	m := framedMessage(t, 7, `{"a": 1}`)
	if !r.DecodeRecord(context.Background(), &m) {
		t.Error("a decoded record reported unsettled")
	}
	if m.Value != `{"a":1}` || !m.ValueDecoding.Decoded() {
		t.Errorf("value = %q (%+v), want the decoded JSON", m.Value, m.ValueDecoding)
	}

	// A schema the registry says does not exist fails the same way every
	// time, so a scan and an index agree on it: settled.
	missing := framedMessage(t, 99, `{}`)
	if !r.DecodeRecord(context.Background(), &missing) {
		t.Error("a missing schema reported unsettled; it is a final answer")
	}
	if missing.ValueDecoding.Reason == "" {
		t.Error("a missing schema left no reason")
	}

	// So does a payload its schema cannot decode.
	corrupt := framedMessage(t, 7, `not json`)
	if !r.DecodeRecord(context.Background(), &corrupt) {
		t.Error("a payload that does not decode reported unsettled; it fails the same way every time")
	}
}

func TestDecodeRecordUnavailableIsUnsettled(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("k-value", 1, 7, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	reg.Intercept(func(w http.ResponseWriter, _ *http.Request) bool {
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	})
	r := newRegistry(t, reg, Config{})

	m := framedMessage(t, 7, `{}`)
	if r.DecodeRecord(context.Background(), &m) {
		t.Error("a record the registry could not answer for reported settled")
	}
	if !strings.Contains(m.ValueDecoding.Reason, "503") {
		t.Errorf("reason = %q, want the registry's status", m.ValueDecoding.Reason)
	}
	if m.Value != "hex:00" {
		t.Errorf("an undecoded value changed to %q", m.Value)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fresh := newRegistry(t, reg, Config{})
	gaveUp := framedMessage(t, 7, `{}`)
	if fresh.DecodeRecord(ctx, &gaveUp) {
		t.Error("a decode the caller stopped waiting for reported settled")
	}
}

func TestSearchDecoderOfNilIsNil(t *testing.T) {
	var r *Registry
	if dec := r.SearchDecoder(); dec != nil {
		t.Errorf("SearchDecoder of no registry = %v; a typed nil would read as decoding", dec)
	}
	if !r.DecodeRecord(context.Background(), &kafka.ConsumedMessage{}) {
		t.Error("a nil registry reported unsettled")
	}
}

func TestIdentity(t *testing.T) {
	a := identityFor("https://sr.example.com:8081")
	for _, same := range []string{
		"https://SR.example.com:8081/",
		"HTTPS://sr.example.com:8081",
		"https://sr.example.com:8081?x=1#frag",
	} {
		if got := identityFor(same); got != a {
			t.Errorf("%s: identity %s, want %s (the same registry)", same, got, a)
		}
	}
	for _, other := range []string{
		"https://sr.example.com:8082",
		"https://other.example.com:8081",
		"https://sr.example.com:8081/ctx",
	} {
		if identityFor(other) == a {
			t.Errorf("%s: identity matches another registry's", other)
		}
	}
	if !strings.HasPrefix(a, "schemareg/"+decodingVersion+":") || strings.Contains(a, "example") {
		t.Errorf("identity %q must name the rendering version and must not carry the URL", a)
	}

	r, err := New(Config{URL: "https://sr.example.com:8081/"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Identity() != a {
		t.Errorf("Registry.Identity = %s, want %s", r.Identity(), a)
	}
}
