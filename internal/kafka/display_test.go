// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/jsonpath"
)

func TestBytesToDisplay(t *testing.T) {
	cases := []struct {
		name string
		want string
		in   []byte
	}{
		{name: "empty", want: "", in: nil},
		{name: "ascii", want: "hello", in: []byte("hello")},
		{name: "utf8 multibyte", want: "héllo", in: []byte("héllo")},
		{name: "json", want: `{"a":1}`, in: []byte(`{"a":1}`)},
		{name: "newline kept", want: "a\nb", in: []byte("a\nb")},
		{name: "tab kept", want: "a\tb", in: []byte("a\tb")},
		{name: "invalid utf8", want: "hex:fffefd", in: []byte{0xff, 0xfe, 0xfd}},
		{name: "embedded nul", want: "hex:00016162", in: []byte{0x00, 0x01, 'a', 'b'}},
		{name: "del byte", want: "hex:617f62", in: []byte{'a', 0x7f, 'b'}},
		{name: "control byte", want: "hex:610162", in: []byte{'a', 0x01, 'b'}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := bytesToDisplay(tc.in)
			if got != tc.want {
				t.Fatalf("bytesToDisplay(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestDisplayBytes(t *testing.T) {
	plain := []byte(`{"a":1}`)
	if got := displayBytes(plain); &got[0] != &plain[0] {
		t.Fatalf("displayable bytes were copied")
	}
	if got := displayBytes([]byte{'a', 0x7f, 'b'}); string(got) != "hex:617f62" {
		t.Fatalf("displayBytes(del) = %q", got)
	}
	if got := displayBytes(nil); len(got) != 0 {
		t.Fatalf("displayBytes(nil) = %q", got)
	}
}

func TestDisplayHeaders(t *testing.T) {
	plain := []kgo.RecordHeader{{Key: "a", Value: []byte("x")}, {Key: "b", Value: []byte("y")}}
	if got := displayHeaders(plain); &got[0] != &plain[0] {
		t.Fatalf("displayable headers were copied")
	}
	mixed := []kgo.RecordHeader{{Key: "a", Value: []byte("x")}, {Key: "b", Value: []byte{0xff}}}
	got := displayHeaders(mixed)
	if &got[0] == &mixed[0] {
		t.Fatalf("a hex-rendered header did not copy the slice")
	}
	if string(got[0].Value) != "x" || string(got[1].Value) != "hex:ff" {
		t.Fatalf("displayHeaders = %q", got)
	}
	if string(mixed[1].Value) != "\xff" {
		t.Fatalf("the input slice was modified: %q", mixed)
	}
}

// TestDeepScanMatchesDisplayText is #144: a scan matches the text Messages
// shows and the indexes store, so a payload the display renders as hex: is
// matched as that rendering, not as its bytes. Every row disagrees between
// the bytes and the rendering, and the bytes verdict is asserted too, so a
// scan that still read bytes fails every row rather than passing by luck.
func TestDeepScanMatchesDisplayText(t *testing.T) {
	t.Parallel()
	rec := &kgo.Record{
		Key:     []byte("k"),
		Value:   []byte("{\"id\":\"a\x7fb\",\"tag\":\"x\"}"),
		Headers: []kgo.RecordHeader{{Key: "h", Value: []byte{0, 1}}},
	}
	shown := recordToConsumed(rec)
	if !strings.HasPrefix(shown.Value, "hex:") || !strings.HasPrefix(shown.Headers[0].Value, "hex:") {
		t.Fatalf("test premise broken: the record does not render as hex: %+v", shown)
	}
	for _, tc := range []struct {
		pat       string
		scope     SearchScope
		bytesOnly bool // the bytes match and the rendering does not; else the reverse
	}{
		{pat: `"tag":"x"`, scope: ScopeValue, bytesOnly: true},
		{pat: "a\x7fb", scope: ScopeValue, bytesOnly: true},
		{pat: `^hex:7b`, scope: ScopeValue},
		{pat: "h=\x00", scope: ScopeHeaders, bytesOnly: true},
		{pat: `h=hex:0001`, scope: ScopeHeaders},
	} {
		re := regexp.MustCompile(tc.pat)
		if raw := fieldsMatch(rec.Key, rec.Value, rec.Headers, re, nil, tc.scope); raw != tc.bytesOnly {
			t.Fatalf("test premise broken: %q on the bytes = %v", tc.pat, raw)
		}
		ds := &deepScan{re: re, prefilter: extractPrefilter(re), scope: tc.scope}
		if _, got := ds.match(context.Background(), rec); got == tc.bytesOnly {
			t.Errorf("%q in scope %d: the scan matched the bytes, not the rendering", tc.pat, tc.scope)
		}
	}
	// A path parses the rendering, which is not JSON, so it never matches
	// such a record; a scan that parsed the bytes would.
	e, err := jsonpath.Parse(`.tag == "x"`)
	if err != nil {
		t.Fatal(err)
	}
	if !e.Match(rec.Value) {
		t.Fatalf("test premise broken: the path does not match the bytes")
	}
	ds := &deepScan{path: e, scope: ScopeValue}
	if _, ok := ds.match(context.Background(), rec); ok {
		t.Errorf("the path matched a hex-rendered value through its bytes")
	}
}
