// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/hex"
	"unicode/utf8"
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
	if len(b) == 0 {
		return ""
	}
	if utf8.Valid(b) && !hasControlBytes(b) {
		return string(b)
	}
	return "hex:" + hex.EncodeToString(b)
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
