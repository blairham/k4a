// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// fakeDecoder decodes every value to a fixed JSON document.
type fakeDecoder struct{}

func (fakeDecoder) DecodeMessage(_ context.Context, m *kafka.ConsumedMessage) {
	m.ValueDecoding = kafka.Decoding{Raw: m.Value, Schema: "t-value v1 (id 1, AVRO)"}
	m.Value = `{"decoded":true}`
	m.KeyDecoding = kafka.Decoding{Reason: "schema id 9: schema not found in registry"}
}

func TestDecodeStream(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan kafka.ConsumedMessage, 1)
	if got := decodeStream(ctx, in, nil); got != in {
		t.Error("with no decoder the stream must pass through untouched")
	}
	out := decodeStream(ctx, in, fakeDecoder{})
	in <- kafka.ConsumedMessage{Value: "hex:00"}
	close(in)
	m := <-out
	if m.Value != `{"decoded":true}` || m.ValueDecoding.Raw != "hex:00" {
		t.Errorf("decoded = %+v", m)
	}
	if _, ok := <-out; ok {
		t.Error("out stays open after in closes")
	}
}

func TestMessagesViewRawToggle(t *testing.T) {
	t.Parallel()
	v := NewMessagesView(nil, testTopicTail)
	m := kafka.ConsumedMessage{Topic: testTopicTail, Key: "k", Value: "hex:00"}
	fakeDecoder{}.DecodeMessage(context.Background(), &m)
	v.Update(MessageMsg(m)) //nolint:errcheck // no command to run

	value := func() string { return v.table.Rows()[0][4] }
	if value() != `{"decoded":true}` {
		t.Fatalf("decoded row = %q", value())
	}
	v.SetFilter("decoded")
	if v.Count() != 1 {
		t.Error("the filter does not match decoded text")
	}
	v.SetFilter("")
	v.HandleKey(KeyToggleRaw)
	if !v.Raw() || value() != "hex:00" {
		t.Errorf("raw row = %q (raw=%v)", value(), v.Raw())
	}
	v.HandleKey(KeyToggleRaw)
	if v.Raw() || value() != `{"decoded":true}` {
		t.Errorf("toggled back = %q", value())
	}
}

func TestMessageDetailShowsSchemaAndReason(t *testing.T) {
	t.Parallel()
	m := &kafka.ConsumedMessage{Topic: "t", Key: "k", Value: "hex:00"}
	fakeDecoder{}.DecodeMessage(context.Background(), m)
	v := NewMessageDetailView(m)
	v.Resize(160, 40)

	cards := v.renderCards()
	if !strings.Contains(cards, "t-value v1 (id 1, AVRO)") {
		t.Errorf("cards do not name the value schema:\n%s", cards)
	}
	if !strings.Contains(cards, "not decoded: schema id 9") {
		t.Errorf("cards do not give the key's reason:\n%s", cards)
	}
	if !strings.Contains(v.renderValue(), "decoded") {
		t.Errorf("value = %q", v.renderValue())
	}
	v.HandleKey(KeyToggleRaw)
	if got := v.renderValue(); !strings.Contains(got, "hex:00") {
		t.Errorf("raw value = %q", got)
	}
	if !strings.Contains(v.renderCards(), "raw; x to decode") {
		t.Error("cards do not say the value is shown raw")
	}
}
