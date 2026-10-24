// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/hex"
	"slices"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// bytesToDisplay converts raw record bytes into a string safe to render in the
// TUI. Plain UTF-8 text round-trips unchanged; anything containing invalid
// UTF-8 or embedded control bytes (NUL etc.) is returned as a `hex:` prefixed
// hex dump so it cannot scramble terminal cell-width measurement or smear
// replacement glyphs across the column.
//
// This is a stopgap until a pluggable SerDe layer lands. The hex
// fallback fires on MirrorMaker2 internal topics (mm2-offset-syncs,
// mm2-checkpoints, mm2-status) whose keys and values are length-prefixed
// binary blobs.
func bytesToDisplay(b []byte) string {
	if displayable(b) {
		return string(b)
	}
	return "hex:" + hex.EncodeToString(b)
}

// displayable reports whether b renders as itself: valid UTF-8 with no
// control byte that bytesToDisplay would hex-encode.
func displayable(b []byte) bool {
	return utf8.Valid(b) && !hasControlBytes(b)
}

// displayBytes is bytesToDisplay for a matcher: b itself when it renders as
// itself (no copy on the scan's hot path), else the hex: rendering. A search
// matches this text everywhere, so a scan and an index, which stores the
// rendering, select the same records (#144).
func displayBytes(b []byte) []byte {
	if displayable(b) {
		return b
	}
	return []byte(bytesToDisplay(b))
}

// displayHeaders renders every header value as displayBytes does, copying
// the slice only when some value changes.
func displayHeaders(hs []kgo.RecordHeader) []kgo.RecordHeader {
	var out []kgo.RecordHeader
	for i, h := range hs {
		if displayable(h.Value) {
			continue
		}
		if out == nil {
			out = slices.Clone(hs)
		}
		out[i].Value = displayBytes(h.Value)
	}
	if out == nil {
		return hs
	}
	return out
}

// hasControlBytes reports whether b contains any C0 control byte other than
// \t, \n, \r, or any DEL (0x7F). Embedded NULs in otherwise-valid UTF-8 ASCII
// are the common failure mode for binary payloads that happen to start with
// printable bytes.
func hasControlBytes(b []byte) bool {
	for _, c := range b {
		if c == '\t' || c == '\n' || c == '\r' {
			continue
		}
		if c < 0x20 || c == 0x7F {
			return true
		}
	}
	return false
}

// The record headers Confluent Platform 8 serializers write a key's or value's
// schema ID into when it is not prefixed to the payload.
const (
	KeySchemaIDHeader   = "__key_schema_id"
	ValueSchemaIDHeader = "__value_schema_id"
)

// schemaIDHeaders returns copies of the last key and value schema-ID header
// values, the ones Confluent's deserializer reads. A last header with a null
// value means none, as it does there: the payload prefix applies.
func schemaIDHeaders(hs []kgo.RecordHeader) (key, value []byte) {
	for _, h := range hs {
		switch h.Key {
		case KeySchemaIDHeader:
			key = cloneOrNil(h.Value)
		case ValueSchemaIDHeader:
			value = cloneOrNil(h.Value)
		}
	}
	return key, value
}

func cloneOrNil(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

// rawCopy is framedCopy, except that a payload whose schema ID travels in a
// header is kept whatever its first byte: it carries no prefix.
func rawCopy(b, schemaID []byte) []byte {
	if schemaID != nil {
		return append([]byte{}, b...)
	}
	return framedCopy(b)
}

// framedCopy returns a copy of b when it could carry the Confluent
// schema-registry header (a zero magic byte and a four-byte ID), else nil.
// A copy, because a record's slices pin its whole fetch buffer; and only
// candidates, because nothing else needs the bytes.
func framedCopy(b []byte) []byte {
	if len(b) < 5 || b[0] != 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
