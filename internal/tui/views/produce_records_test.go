// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import "testing"

func TestBuildProduceRecords(t *testing.T) {
	t.Parallel()

	// A typed value is sent once by default, with its key and headers.
	rs, err := buildProduceRecords("orders", "", "k", `{"id":1}`, "src=tui, trace=x", "")
	if err != nil || len(rs) != 1 {
		t.Fatalf("custom = %d records, %v; want 1", len(rs), err)
	}
	r := rs[0]
	if string(r.Key) != "k" || string(r.Value) != `{"id":1}` || len(r.Headers) != 2 || r.Headers[1].Key != "trace" {
		t.Errorf("record = key %q value %q headers %v", r.Key, r.Value, r.Headers)
	}
	if three, _ := buildProduceRecords("orders", "", "", "v", "", "3"); len(three) != 3 {
		t.Errorf("count 3 = %d records", len(three))
	}

	// Blank value: generated messages, ten by default, headers still added.
	rs, err = buildProduceRecords("orders", "", "", "", "src=tui", "")
	if err != nil || len(rs) != 10 {
		t.Fatalf("generated = %d records, %v; want 10", len(rs), err)
	}
	if h := rs[0].Headers; len(h) == 0 || h[len(h)-1].Key != "src" {
		t.Errorf("generated headers = %v", h)
	}

	if _, err := buildProduceRecords("orders", "", "", "v", "broken", ""); err == nil {
		t.Error("a malformed header list was accepted")
	}
}
