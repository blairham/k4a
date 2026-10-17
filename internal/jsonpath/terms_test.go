// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package jsonpath

import (
	"slices"
	"strings"
	"testing"
)

func TestShorthand(t *testing.T) {
	for in, want := range map[string]string{
		"orderId:42":        `.orderId == 42`,
		"status:failed":     `.status == "failed"`,
		"order.id: 7":       `.order.id == 7`,
		`flag:"true"`:       `.flag == "true"`,
		"flag:true":         `.flag == true`,
		"id:007":            `.id == "007"`,
		"note:a b: c":       `.note == "a b: c"`,
		"v:null":            `.v == null`,
		`name:"John Smith"`: `.name == "John Smith"`,
	} {
		e, err := Parse(in)
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if got := e.String(); got != want {
			t.Errorf("%q = %s, want %s", in, got, want)
		}
	}
	for _, bad := range []string{"orderId:", ":42", "9a:1", "a..b:1", "a-b:1", "plain text"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("%q parsed; want an error", bad)
		}
	}
}

func TestTermsMatchTheirPredicate(t *testing.T) {
	doc := []byte(`{"order":{"id":42,"tags":["a",null,true]},"a.b":"x","n":-0.0,"big":1e999999}`)
	terms, complete := Terms(doc, 100)
	if !complete {
		t.Fatal("small document marked partial")
	}
	for _, src := range []string{
		`.order.id == 42`, `.order.id == 42.0`, `.order.id == 4.2e1`, `order.id:42`,
		`.order.tags[0] == "a"`, `.order.tags[1] == null`, `.order.tags[2] == true`,
		`.["a.b"] == "x"`, `.n == 0`, `.big == 1e999999`,
	} {
		e, err := Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		term, ok := e.Term()
		if !ok {
			t.Errorf("%s: not indexable", src)
			continue
		}
		if !e.Match(doc) {
			t.Fatalf("%s does not match the document; the test is wrong", src)
		}
		if !slices.Contains(terms, term) {
			t.Errorf("%s: term %q missing from %q", src, term, terms)
		}
	}
	for _, src := range []string{`.order.id == 43`, `.order.id == "42"`, `.order.tags[0] == "b"`} {
		e, _ := Parse(src)
		term, _ := e.Term()
		if slices.Contains(terms, term) {
			t.Errorf("%s: a non-matching predicate's term is in the document", src)
		}
	}
}

func TestTermsLimits(t *testing.T) {
	if terms, complete := Terms(
		[]byte(`{"a":1,"b":2,"c":3}`),
		2,
	); complete ||
		!slices.Equal(terms, []string{PartialTerm}) {
		t.Errorf("over the cap: %q, %v; want only the partial marker", terms, complete)
	}
	for _, notJSON := range []string{"", "hex:00", `{"a":1} trailing`, "plain"} {
		if terms, complete := Terms([]byte(notJSON), 10); len(terms) != 0 || !complete {
			t.Errorf("%q: %q, %v; want no terms (nothing can match it)", notJSON, terms, complete)
		}
	}
	long := strings.Repeat("x", MaxTermValue+1)
	e, err := Parse(`.a == "` + long + `"`)
	if err != nil {
		t.Fatal(err)
	}
	if e.Indexable() {
		t.Error("a literal longer than MaxTermValue is indexable; its leaf has no term")
	}
	for _, src := range []string{`.a != 1`, `.a < 1`, `.a exists`, `.a`} {
		if e, _ := Parse(src); e.Indexable() {
			t.Errorf("%s is indexable; only == is", src)
		}
	}
}
