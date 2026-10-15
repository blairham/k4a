// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package jsonpath

import "testing"

// FuzzParsePath feeds arbitrary predicate text and record bytes. A predicate
// arrives from the CLI, the TUI and MCP clients, and the record from any
// producer, so neither may panic; and an accepted predicate's canonical
// String must parse back to itself and match exactly what the original does,
// because the search cache is keyed on that string.
func FuzzParsePath(f *testing.F) {
	for _, s := range []string{
		`.order.status == "failed"`, `.a[0].b >= 1.5`, `.["x y"] != null`, `.`, `.a exists`,
		`.a < "m"`, `.id == 9007199254740993`, `.a == 1e308`, `.[""]`, `.a == "\u0000"`,
	} {
		f.Add(s, []byte(`{"order":{"status":"failed"},"a":[{"b":2}],"x y":1,"id":9007199254740993,"":0}`))
	}
	f.Fuzz(func(t *testing.T, s string, doc []byte) {
		e, err := Parse(s)
		if err != nil {
			return
		}
		canon := e.String()
		again, err := Parse(canon)
		if err != nil {
			t.Fatalf("canonical form %q of %q does not parse: %v", canon, s, err)
		}
		if again.String() != canon {
			t.Fatalf("canonical form is not a fixed point: %q -> %q", canon, again.String())
		}
		if e.Match(doc) != again.Match(doc) {
			t.Fatalf("%q and its canonical form %q disagree on %q", s, canon, doc)
		}
	})
}
