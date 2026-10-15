// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseHeader(t *testing.T) {
	t.Parallel()
	h, err := ParseHeader("trace=a=b")
	if err != nil || h.Key != "trace" || string(h.Value) != "a=b" {
		t.Errorf("ParseHeader(trace=a=b) = %+v, %v; the value keeps later '='", h, err)
	}
	if h, err := ParseHeader("empty="); err != nil || h.Key != "empty" || len(h.Value) != 0 {
		t.Errorf("ParseHeader(empty=) = %+v, %v", h, err)
	}
	for _, bad := range []string{"novalue", "=v", ""} {
		if _, err := ParseHeader(bad); err == nil {
			t.Errorf("ParseHeader(%q) accepted", bad)
		}
	}
}

func TestParseHeaderList(t *testing.T) {
	t.Parallel()
	hs, err := ParseHeaderList(" a=1, b=2 ,,")
	if err != nil || len(hs) != 2 || hs[0].Key != "a" || string(hs[1].Value) != "2" {
		t.Errorf("ParseHeaderList = %+v, %v", hs, err)
	}
	if hs, err := ParseHeaderList(""); err != nil || len(hs) != 0 {
		t.Errorf("empty list = %+v, %v", hs, err)
	}
	if _, err := ParseHeaderList("a=1,b"); err == nil {
		t.Error("a malformed entry was accepted")
	}
}

func TestCustomRecords(t *testing.T) {
	t.Parallel()
	hs := []kgo.RecordHeader{{Key: "h", Value: []byte("v")}}
	rs := CustomRecords("t", "k", "val", hs, 3)
	if len(rs) != 3 {
		t.Fatalf("len = %d, want 3", len(rs))
	}
	if r := rs[0]; r.Topic != "t" || string(r.Key) != "k" || string(r.Value) != "val" || len(r.Headers) != 1 {
		t.Errorf("record = %+v", r)
	}
	if r := CustomRecords("t", "", "v", nil, 1)[0]; r.Key != nil {
		t.Errorf("empty key = %q, want a null key", r.Key)
	}
}

func TestReadLineRecords(t *testing.T) {
	t.Parallel()
	rs, err := ReadLineRecords(strings.NewReader("one\r\n\ntwo\n"), "t", "", nil)
	if err != nil || len(rs) != 2 || string(rs[0].Value) != "one" || rs[0].Key != nil || string(rs[1].Value) != "two" {
		t.Fatalf("values = %+v, %v", rs, err)
	}

	rs, err = ReadLineRecords(strings.NewReader("k1\t{\"a\":1}\n\tnokey\nk3\tx\ty\n"), "t", "\t", nil)
	if err != nil || len(rs) != 3 {
		t.Fatalf("keyed = %+v, %v", rs, err)
	}
	if string(rs[0].Key) != "k1" || string(rs[0].Value) != `{"a":1}` {
		t.Errorf("line 1 = %q/%q", rs[0].Key, rs[0].Value)
	}
	if rs[1].Key != nil || string(rs[1].Value) != "nokey" {
		t.Errorf("an empty key = %q, want null", rs[1].Key)
	}
	if string(rs[2].Key) != "k3" || string(rs[2].Value) != "x\ty" {
		t.Errorf("only the first separator splits: %q/%q", rs[2].Key, rs[2].Value)
	}

	if _, err := ReadLineRecords(strings.NewReader("k1\tv\nno-sep\n"), "t", "\t", nil); err == nil ||
		!strings.Contains(err.Error(), "line 2") {
		t.Errorf("a line without the separator: err = %v, want a line-2 error", err)
	}
	long := strings.Repeat("x", MaxLineRecordBytes+1)
	if _, err := ReadLineRecords(strings.NewReader(long), "t", "", nil); err == nil {
		t.Error("an over-long line was accepted")
	}
}
