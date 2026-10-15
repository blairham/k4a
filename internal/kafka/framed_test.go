// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecordToConsumedKeepsOnlyFramedBytes(t *testing.T) {
	framed := []byte{0, 0, 0, 0, 7, 'v'}
	m := recordToConsumed(&kgo.Record{Key: []byte("plain"), Value: framed})
	if m.RawKey != nil {
		t.Errorf("RawKey kept for a payload that cannot be framed: %q", m.RawKey)
	}
	if string(m.RawValue) != string(framed) {
		t.Fatalf("RawValue = %x", m.RawValue)
	}
	framed[5] = 'X'
	if m.RawValue[5] != 'v' {
		t.Error("RawValue aliases the record buffer")
	}
	if framedCopy([]byte{0, 0, 0, 1}) != nil {
		t.Error("a four-byte payload is too short to carry the header")
	}
}

// TestRecordToConsumedKeepsSchemaIDHeaders: a Confluent Platform 8 record
// carries its schema ID in a header and an unprefixed payload, so the bytes
// must be kept whatever they start with, along with the last header's value.
func TestRecordToConsumedKeepsSchemaIDHeaders(t *testing.T) {
	guid := append([]byte{1}, make([]byte, 16)...)
	m := recordToConsumed(&kgo.Record{
		Key:   []byte("plain-key"),
		Value: []byte("\x02avro"),
		Headers: []kgo.RecordHeader{
			{Key: ValueSchemaIDHeader, Value: []byte{9}},
			{Key: ValueSchemaIDHeader, Value: guid},
			{Key: KeySchemaIDHeader, Value: guid},
			{Key: KeySchemaIDHeader, Value: nil}, // last one null: no header
		},
	})
	if string(m.ValueSchemaID) != string(guid) {
		t.Errorf("ValueSchemaID = %x, want the last header %x", m.ValueSchemaID, guid)
	}
	if string(m.RawValue) != "\x02avro" {
		t.Errorf("RawValue = %q, want the unprefixed payload kept", m.RawValue)
	}
	if m.KeySchemaID != nil || m.RawKey != nil {
		t.Errorf("a null last key header kept KeySchemaID=%x RawKey=%q", m.KeySchemaID, m.RawKey)
	}
}
