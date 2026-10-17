// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package jsonpath

import (
	"bytes"
	"encoding/json"
	"math/big"
	"slices"
	"strconv"
	"strings"
)

// An index term names one scalar leaf of a JSON document, its canonical path
// and its typed canonical value, so an equality predicate can be answered
// from an index by looking up the one term it would equal (docs/design/
// local-index.md, Tier 5). The encoding only has to be identical on both
// sides: Terms emits it for every leaf and Expr.Term for the literal, and
// FuzzTerms checks that whenever Match is true the predicate's term is among
// the document's.
const (
	// MaxTermValue is the longest string leaf (in bytes) that gets a term. A
	// longer leaf can only equal a longer literal, and Term declines those,
	// so skipping them costs no recall.
	MaxTermValue = 256
	// PartialTerm marks a document whose leaves were not all enumerated; an
	// index must treat it as a candidate for every predicate. It cannot
	// collide with a real term, which starts with the path's '.'.
	PartialTerm = "\x00partial"
)

// Indexable reports whether the predicate can be answered from Terms: an
// equality with a string no longer than MaxTermValue, a number, a boolean or
// null. Everything else (orderings, !=, exists) needs the document.
func (e *Expr) Indexable() bool {
	_, ok := e.Term()
	return ok
}

// Term is the index term a document leaf equal to the predicate carries, or
// false when the predicate is not an equality the terms can answer.
func (e *Expr) Term() (string, bool) {
	if e.op != OpEq {
		return "", false
	}
	v, ok := canonicalScalar(e.lit)
	if !ok {
		return "", false
	}
	return pathText(e.steps) + "\x00" + v, true
}

// Terms decodes data as Match does and returns the term of every scalar leaf
// reachable through object fields and array indexes. complete is false when
// the document had more than limit leaves, in which case terms holds only
// PartialTerm. Data that is not a single JSON document has no terms: no
// predicate can match it.
func Terms(data []byte, limit int) (terms []string, complete bool) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, true
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil || dec.InputOffset() != int64(len(data)) {
		return nil, true
	}
	w := &walker{max: limit, seen: map[string]struct{}{}}
	w.walk(doc, nil)
	if w.over {
		return []string{PartialTerm}, false
	}
	return w.terms, true
}

type walker struct {
	seen  map[string]struct{}
	terms []string
	max   int
	over  bool
}

func (w *walker) walk(v any, steps []step) {
	if w.over {
		return
	}
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			w.walk(t[k], append(steps, step{name: k}))
		}
	case []any:
		for i, el := range t {
			w.walk(el, append(steps, step{index: i, isIndex: true}))
		}
	default:
		val, ok := canonicalScalar(t)
		if !ok {
			return
		}
		term := pathText(steps) + "\x00" + val
		if _, dup := w.seen[term]; dup {
			return
		}
		if len(w.terms) >= w.max {
			w.over = true
			return
		}
		w.seen[term] = struct{}{}
		w.terms = append(w.terms, term)
	}
}

// canonicalScalar renders a scalar so that two values Match treats as equal
// render identically: strings as themselves, numbers by their value at the
// comparison precision (so 42, 42.0 and 4.2e1 agree, and -0 is 0), booleans
// and null by name. ok is false for a string too long to index and a
// number Match cannot compare.
func canonicalScalar(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		if len(t) > MaxTermValue {
			return "", false
		}
		return "s" + t, true
	case json.Number:
		f, ok := parseNum(t)
		if !ok {
			return "", false
		}
		return "n" + numText(f), true
	case bool:
		return "b" + strconv.FormatBool(t), true
	case nil:
		return "z", true
	}
	return "", false
}

// numText renders f exactly in binary ('p': hex mantissa, binary exponent).
// Decimal would cost time in the exponent, which a record controls; every
// number is parsed at the same precision, so equal values render alike.
func numText(f *big.Float) string {
	if f.Sign() == 0 {
		return "0" // -0 compares equal to 0
	}
	return f.Text('p', 0)
}

// pathText is the path in canonical form, as String renders it.
func pathText(steps []step) string {
	var b strings.Builder
	writePath(&b, steps)
	return b.String()
}
