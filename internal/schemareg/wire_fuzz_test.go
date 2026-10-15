// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"bytes"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

// FuzzParseWire feeds arbitrary record bytes (which come straight off a
// broker) to the wire-format parser. It must never panic, never return a
// body that is not a suffix of its input, and agree with franz-go's own
// Confluent header decoder whenever that one accepts the input. The same bytes
// also go to the schema-ID header parser, whose input is a record header.
func FuzzParseWire(f *testing.F) {
	f.Add(append([]byte{1}, make([]byte, 17)...))
	f.Add([]byte{0, 0, 0, 0, 1, 0})
	f.Add([]byte{0, 0, 0, 0, 1, 2, 2, 'x'})
	f.Add([]byte{0, 0, 0, 0, 1, 4, 2, 0, 'x'})
	f.Add([]byte{0, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0f})
	f.Add([]byte(`{"plain":"json"}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		checkSchemaIDHeader(t, b)
		id, body, ok := ParseHeader(b)
		if !ok {
			return
		}
		if id <= 0 || len(body) != len(b)-headerSize {
			t.Fatalf("id %d body %d of %d", id, len(body), len(b))
		}
		var h sr.ConfluentHeader
		if srID, _, err := h.DecodeID(b); err == nil && srID != id {
			t.Fatalf("id %d, franz-go says %d", id, srID)
		}
		idx, rest, err := ParseMessageIndexes(body)
		if err != nil {
			return
		}
		if len(idx) == 0 || len(idx) > maxMessageIndexes || len(rest) > len(body) {
			t.Fatalf("indexes %v rest %d of %d", idx, len(rest), len(body))
		}
		for _, i := range idx {
			if i < 0 || i > maxIndexValue {
				t.Fatalf("index %d out of range", i)
			}
		}
		if srIdx, srRest, err := h.DecodeIndex(body, maxMessageIndexes); err == nil && len(srRest) == len(rest) &&
			!slices.Equal(srIdx, idx) {
			t.Fatalf("indexes %v, franz-go says %v", idx, srIdx)
		}
	})
}

func checkSchemaIDHeader(t *testing.T, b []byte) {
	t.Helper()
	id, guid, rest, ok := ParseSchemaIDHeader(b)
	if !ok {
		return
	}
	if (id > 0) == (guid != "") || len(rest) > len(b) || !bytes.HasSuffix(b, rest) {
		t.Fatalf("id %d guid %q rest %d of %d", id, guid, len(rest), len(b))
	}
	if guid != "" {
		raw, err := hex.DecodeString(strings.ReplaceAll(guid, "-", ""))
		if err != nil || len(guid) != 36 || !bytes.Equal(raw, b[1:guidHeaderSize]) {
			t.Fatalf("guid %q from %x", guid, b[:guidHeaderSize])
		}
	}
}
