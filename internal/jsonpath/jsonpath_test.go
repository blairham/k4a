// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package jsonpath

import "testing"

func TestMatch(t *testing.T) {
	t.Parallel()
	const order = `{"order":{"status":"failed","id":9007199254740993,"total":12.50,` +
		`"items":[{"sku":"a","qty":3},{"sku":"b","qty":10}],"note":null,"paid":false},` +
		`"content-type":"json","":1}`
	cases := []struct {
		expr string
		doc  string
		want bool
	}{
		{`.order.status == "failed"`, order, true},
		{`.order.status=="failed"`, order, true},
		{`.order.status == "FAILED"`, order, false}, // case-sensitive
		{`.order.status != "ok"`, order, true},
		{`.order.status != "failed"`, order, false},
		{`.order.missing != "failed"`, order, false}, // != needs the path to exist
		{`.order.items[1].qty >= 10`, order, true},
		{`.order.items[1].qty > 10`, order, false},
		{`.order.items.[0].sku == "a"`, order, true},
		{`.order.items[2]`, order, false},
		{`.order.total == 12.5`, order, true}, // numeric, not textual
		{`.order.total < 1e2`, order, true},
		{`.order.id == 9007199254740993`, order, true}, // beyond float64's exact range
		{`.order.id == 9007199254740992`, order, false},
		{`.order.note == null`, order, true},
		{`.order.note exists`, order, true}, // null still exists
		{`.order.paid == false`, order, true},
		{`.order.paid != true`, order, true},
		{`.order.status < "g"`, order, true},
		{`.order.status > 1`, order, false}, // type mismatch never orders
		{`.order.status != 1`, order, true},
		{`.["content-type"] == "json"`, order, true},
		{`.[""] == 1`, order, true},
		{`.order`, order, true},
		{`.nope`, order, false},
		{`.`, `"x"`, true},
		{`. == 3`, ` 3 `, true},
		{`.a`, `{"a":1} }`, false}, // trailing garbage: not one document
		{`.a`, `not json`, false},
		{`.a`, ``, false},
		{`[0]`, `[1]`, false}, // not a valid expr, guarded below
	}
	for _, c := range cases {
		e, err := Parse(c.expr)
		if c.expr == `[0]` {
			if err == nil {
				t.Fatalf("Parse(%q) accepted a path without a leading '.'", c.expr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.expr, err)
		}
		if got := e.Match([]byte(c.doc)); got != c.want {
			t.Errorf("%q on %s = %v, want %v", c.expr, c.doc, got, c.want)
		}
	}
}

func TestParseRejects(t *testing.T) {
	t.Parallel()
	for _, s := range []string{
		``, `a`, `..a`, `.a.`, `.a[`, `.a[-1]`, `.a[x]`, `.a["x"`, `.a = 1`, `.a ==`,
		`.a == {}`, `.a == [1]`, `.a < true`, `.a >= null`, `.a == 1 2`, `.a == "x" junk`,
		`.a[99999999999]`, `.a == 1e999999999999`, `.a exists 1`,
	} {
		if e, err := Parse(s); err == nil {
			t.Errorf("Parse(%q) = %v, want an error", s, e)
		}
	}
}

func TestStringCanonical(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]string{
		`.a=="x"`:         `.a == "x"`,
		` .["a"] == "x" `: `.a == "x"`,
		`.a.[0]["b c"]`:   `.a[0]["b c"]`,
		`.a exists`:       `.a`,
		`.`:               `.`,
		`.a != null`:      `.a != null`,
		`.a>=1.50`:        `.a >= 1.50`,
		`.["é"] == "é"`:   `.["é"] == "é"`,
	} {
		e, err := Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if got := e.String(); got != want {
			t.Errorf("Parse(%q).String() = %q, want %q", in, got, want)
		}
	}
}
