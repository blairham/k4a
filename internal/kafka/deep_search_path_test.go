// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"regexp"
	"testing"

	"github.com/blairham/k4a/internal/jsonpath"
)

// TestDeepSearchJSONPath scans real records on kfake with a JSON-path
// predicate, alone and ANDed with a regex. The values are
// {"tag":"scan-record","seq":N}, so ".seq" selects by number, which no
// substring of the raw bytes can do ("seq":39 is a prefix of "seq":390).
func TestDeepSearchJSONPath(t *testing.T) {
	t.Parallel()

	const topic = "deep.search.jsonpath.v1"
	const n = 400
	seeds := newScanFakeCluster(t, topic)
	produceScanRecords(t, seeds, topic, n)
	client := newScanTestClient(t, seeds)

	cases := []struct {
		pattern *regexp.Regexp
		path    string
		scope   SearchScope
		want    int
	}{
		{path: `.seq >= 390`, want: 10},
		{path: `.seq == 39`, want: 1},
		{path: `.tag == "scan-record"`, want: n},
		{path: `.tag != "scan-record"`, want: 0},
		{path: `.missing`, want: 0},
		// The key is not JSON, so a key-only path never matches.
		{path: `.seq == 39`, scope: ScopeKey, want: 0},
		// AND: the regex hits keys 0-9, the path keeps seq < 5.
		{pattern: regexp.MustCompile(`key-00000\d`), path: `.seq < 5`, want: 5},
	}
	for _, c := range cases {
		expr, err := jsonpath.Parse(c.path)
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.path, err)
		}
		msgs, prog := runScanWithProgress(t, client, topic, SearchParams{
			Pattern: c.pattern, Path: expr, Scope: c.scope,
		})
		if len(msgs) != c.want {
			t.Errorf("pattern %v path %q scope %d: %d matches, want %d", c.pattern, c.path, c.scope, len(msgs), c.want)
		}
		if prog.Incomplete() || prog.Scanned != n {
			t.Errorf("path %q: scanned %d of %d records (incomplete=%v); a path query must read them all",
				c.path, prog.Scanned, n, prog.Incomplete())
		}
	}
}
