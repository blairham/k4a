// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package jsonpath

import (
	"slices"
	"testing"
)

// FuzzTerms is the index's recall proof for equality predicates: whenever
// Match says a document satisfies an indexable predicate, the predicate's
// Term must be among the document's Terms (or the document must be marked
// partial, which makes it a candidate for everything). A miss here is a
// record the index would silently fail to return.
func FuzzTerms(f *testing.F) {
	for _, s := range []struct{ doc, expr string }{
		{`{"a":1}`, `.a == 1`},
		{`{"a":1.0}`, `.a == 1`},
		{`{"a":-0}`, `.a == 0`},
		{`{"a":4.2e1}`, `.a == 42`},
		{`{"a":12345678901234567890123}`, `.a == 12345678901234567890123`},
		{`{"a":"x","a":"y"}`, `.a == "y"`},
		{`{"a":{"b":[null,true,"s"]}}`, `.a.b[2] == "s"`},
		{`{"a":{"b":[null,true,"s"]}}`, `.a.b[0] == null`},
		{`{"a.b":1}`, `.["a.b"] == 1`},
		{`{"":1}`, `.[""] == 1`},
		{`["x"]`, `.[0] == "x"`},
		{`"x"`, `. == "x"`},
		{`{"é":"é"}`, `.["é"] == "é"`},
		{`{"a":"\ud800"}`, `.a == "�"`},
	} {
		f.Add([]byte(s.doc), s.expr)
	}
	f.Fuzz(func(t *testing.T, doc []byte, src string) {
		e, err := Parse(src)
		if err != nil {
			return
		}
		term, ok := e.Term()
		if !ok || !e.Match(doc) {
			return
		}
		terms, complete := Terms(doc, 1<<20)
		if !complete {
			t.Fatalf("a %d-byte document overflowed the leaf cap", len(doc))
		}
		if !slices.Contains(terms, term) {
			t.Fatalf("%s matches %q but its term %q is not among the document's %q", e, doc, term, terms)
		}
	})
}
