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
