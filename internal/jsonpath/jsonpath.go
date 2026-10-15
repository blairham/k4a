// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package jsonpath is the JSON-path predicate deep search matches records
// with: a small, closed subset of jq's path syntax plus one comparison, e.g.
//
//	.order.status == "failed"
//	.items[0].qty >= 10
//	.["content-type"] != "text/plain"
//	.trace.id exists
//
// The subset is deliberately tiny so its semantics fit in one paragraph and
// can be matched exactly on raw JSON: a path is `.` followed by `.name`,
// `["any name"]` or `[N]` steps; a predicate is `exists` (also what a bare
// path means) or one of == != < <= > >= against a JSON scalar. See
// docs/design/deep-search.md ("JSON path search") for why not JSONPath.
package jsonpath

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// Op is the comparison a predicate applies at the end of its path.
type Op uint8

const (
	// OpExists matches when the path resolves to any value, null included.
	OpExists Op = iota
	// OpEq matches a value of the literal's type equal to it.
	OpEq
	// OpNe matches a value that exists and is not equal to the literal.
	OpNe
	// OpLt and the other orderings compare numbers numerically and strings
	// bytewise; a value of any other type, or of a different type from the
	// literal, never matches.
	OpLt
	// OpLe is <=.
	OpLe
	// OpGt is >.
	OpGt
	// OpGe is >=.
	OpGe
)

var opText = [...]string{OpExists: "exists", OpEq: "==", OpNe: "!=", OpLt: "<", OpLe: "<=", OpGt: ">", OpGe: ">="}

// maxIndex bounds an array index so a typed index can never be a resource
// problem; no real payload has a two-billion-element array.
const maxIndex = 1<<31 - 1

// numPrec is the mantissa precision numbers are compared at: exact for any
// integer below 2^256 and for decimals of ~77 significant digits, so IDs
// beyond float64's 2^53 still compare exactly.
const numPrec = 256

// step is one path component: a field name, or an array index when isIndex.
type step struct {
	name    string
	index   int
	isIndex bool
}

// Expr is a parsed predicate. The zero value is not valid; use Parse.
type Expr struct {
	lit   any // string, json.Number, bool or nil (JSON null)
	num   *big.Float
	steps []step
	op    Op
}

// Parse parses a predicate. Surrounding whitespace is ignored.
func Parse(s string) (*Expr, error) {
	p := &parser{s: strings.TrimSpace(s)}
	e, err := p.parse()
	if err != nil {
		return nil, fmt.Errorf("json path %q: %w", s, err)
	}
	return e, nil
}

type parser struct {
	s string
	i int
}

func (p *parser) parse() (*Expr, error) {
	if p.s == "" {
		return nil, errors.New("empty expression")
	}
	steps, err := p.path()
	if err != nil {
		return nil, err
	}
	e := &Expr{steps: steps}
	rest := strings.TrimSpace(p.s[p.i:])
	if rest == "" || rest == "exists" {
		return e, nil
	}
	e.op, rest = cutOp(rest)
	if e.op == OpExists {
		return nil, fmt.Errorf("expected ==, !=, <, <=, >, >= or exists after the path, got %q", rest)
	}
	if err := e.setLiteral(strings.TrimSpace(rest)); err != nil {
		return nil, err
	}
	return e, nil
}

// cutOp strips a leading comparison operator from s, returning OpExists when
// there is none. Two-byte operators are tried first so "<=" is not "<".
func cutOp(s string) (Op, string) {
	for _, op := range []Op{OpEq, OpNe, OpLe, OpGe, OpLt, OpGt} {
		if rest, ok := strings.CutPrefix(s, opText[op]); ok {
			return op, rest
		}
	}
	return OpExists, s
}

// path parses the leading path. It must start with '.', and a '.' must be
// followed by a name or '[' unless it is the whole path (the root).
func (p *parser) path() ([]step, error) {
	if p.s[0] != '.' {
		return nil, errors.New("a path starts with '.'")
	}
	var steps []step
	for p.i < len(p.s) {
		switch c := p.s[p.i]; c {
		case '.':
			p.i++
			switch {
			case p.i < len(p.s) && isIdentStart(p.s[p.i]):
				steps = append(steps, step{name: p.ident()})
			case p.i < len(p.s) && p.s[p.i] == '[':
				// ".[...]": the bracket is parsed on the next iteration.
			case p.i == 1 && (p.i == len(p.s) || p.s[p.i] != '.'):
				// The root "." on its own.
			default:
				return nil, fmt.Errorf("'.' at offset %d must be followed by a name or '['", p.i-1)
			}
		case '[':
			st, err := p.bracket()
			if err != nil {
				return nil, err
			}
			steps = append(steps, st)
		default:
			return steps, nil
		}
	}
	return steps, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdent(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') }

func (p *parser) ident() string {
	start := p.i
	for p.i < len(p.s) && isIdent(p.s[p.i]) {
		p.i++
	}
	return p.s[start:p.i]
}

// bracket parses `["name"]` (a JSON string) or `[N]` (a non-negative index).
func (p *parser) bracket() (step, error) {
	p.i++ // '['
	if p.i < len(p.s) && p.s[p.i] == '"' {
		dec := json.NewDecoder(strings.NewReader(p.s[p.i:]))
		var name string
		if err := dec.Decode(&name); err != nil {
			return step{}, fmt.Errorf("bad quoted name at offset %d: %w", p.i, err)
		}
		p.i += int(dec.InputOffset())
		if p.i >= len(p.s) || p.s[p.i] != ']' {
			return step{}, fmt.Errorf("expected ']' at offset %d", p.i)
		}
		p.i++
		return step{name: name}, nil
	}
	start := p.i
	for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		p.i++
	}
	if p.i == start || p.i >= len(p.s) || p.s[p.i] != ']' {
		return step{}, fmt.Errorf("expected a quoted name or a non-negative index in [] at offset %d", start)
	}
	n, err := strconv.Atoi(p.s[start:p.i])
	if err != nil || n > maxIndex {
		return step{}, fmt.Errorf("array index %q out of range", p.s[start:p.i])
	}
	p.i++
	return step{index: n, isIndex: true}, nil
}

// setLiteral parses the comparison's right-hand side, which must be exactly
// one JSON scalar; the orderings take only a number or a string.
func (e *Expr) setLiteral(s string) error {
	if s == "" {
		return fmt.Errorf("%s needs a value", opText[e.op])
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return fmt.Errorf("value %q is not a JSON scalar: %w", s, err)
	}
	if strings.TrimSpace(s[dec.InputOffset():]) != "" {
		return fmt.Errorf("unexpected %q after the value", s[dec.InputOffset():])
	}
	switch lit := v.(type) {
	case json.Number:
		f, ok := parseNum(lit)
		if !ok {
			return fmt.Errorf("number %s is out of range", lit)
		}
		e.num = f
	case string:
	case bool, nil:
		if e.op != OpEq && e.op != OpNe {
			return fmt.Errorf("%s compares numbers or strings, not %s", opText[e.op], s)
		}
	default:
		return fmt.Errorf("value %s is not a scalar (string, number, true, false or null)", s)
	}
	e.lit = v
	return nil
}

func parseNum(n json.Number) (*big.Float, bool) {
	f, _, err := big.ParseFloat(string(n), 10, numPrec, big.ToNearestEven)
	if err != nil || f.IsInf() {
		return nil, false
	}
	return f, true
}

// String is the predicate in canonical form, so equivalent spellings
// (`.a=="x"`, `.["a"] == "x"`) render, and cache, identically.
func (e *Expr) String() string {
	var b strings.Builder
	if len(e.steps) == 0 || !e.steps[0].isIdentName() {
		b.WriteByte('.')
	}
	for _, st := range e.steps {
		switch {
		case st.isIndex:
			fmt.Fprintf(&b, "[%d]", st.index)
		case st.isIdentName():
			b.WriteString("." + st.name)
		default:
			q, _ := json.Marshal(st.name) //nolint:errcheck // marshaling a string cannot fail
			b.WriteString("[" + string(q) + "]")
		}
	}
	if e.op == OpExists {
		return b.String()
	}
	lit, _ := json.Marshal(e.lit) //nolint:errcheck // a scalar always marshals
	return b.String() + " " + opText[e.op] + " " + string(lit)
}

// isIdentName reports whether the step can be written as `.name`.
func (st step) isIdentName() bool {
	return !st.isIndex && st.name != "" && isIdentStart(st.name[0]) && strings.IndexFunc(st.name, notIdentRune) < 0
}

func notIdentRune(r rune) bool { return r > 0x7f || !isIdent(byte(r)) }

// Match reports whether data is JSON whose value at the path satisfies the
// predicate. Data that is not a single valid JSON document never matches.
func (e *Expr) Match(data []byte) bool {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return false
	}
	// A non-root path can only resolve inside an object or array, so skip
	// decoding scalars and plain text outright.
	if len(e.steps) > 0 && data[0] != '{' && data[0] != '[' {
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc any
	// Trailing bytes mean this is not one JSON document; More() would miss a
	// stray closing brace, so compare offsets instead.
	if err := dec.Decode(&doc); err != nil || dec.InputOffset() != int64(len(data)) {
		return false
	}
	v, ok := e.resolve(doc)
	if !ok {
		return false
	}
	return e.compare(v)
}

func (e *Expr) resolve(v any) (any, bool) {
	for _, st := range e.steps {
		if st.isIndex {
			arr, ok := v.([]any)
			if !ok || st.index >= len(arr) {
				return nil, false
			}
			v = arr[st.index]
			continue
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = obj[st.name]; !ok {
			return nil, false
		}
	}
	return v, true
}

func (e *Expr) compare(v any) bool {
	if e.op == OpExists {
		return true
	}
	c, ok := e.cmp(v)
	switch e.op {
	case OpEq:
		return ok && c == 0
	case OpNe:
		return !ok || c != 0
	case OpLt:
		return ok && c < 0
	case OpLe:
		return ok && c <= 0
	case OpGt:
		return ok && c > 0
	case OpGe:
		return ok && c >= 0
	}
	return false
}

// cmp orders v against the literal. ok is false when the two are of
// different JSON types, or for an ordering between two booleans or nulls.
func (e *Expr) cmp(v any) (c int, ok bool) {
	switch lit := e.lit.(type) {
	case string:
		s, ok := v.(string)
		if !ok {
			return 0, false
		}
		return strings.Compare(s, lit), true
	case json.Number:
		n, ok := v.(json.Number)
		if !ok {
			return 0, false
		}
		f, ok := parseNum(n)
		if !ok {
			return 0, false
		}
		return f.Cmp(e.num), true
	case bool:
		b, ok := v.(bool)
		if !ok || b != lit {
			return 1, ok
		}
		return 0, true
	case nil:
		if v != nil {
			return 1, false
		}
		return 0, true
	}
	return 0, false
}
