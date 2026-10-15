// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// keyedSearch answers every run with one match per key, recording each
// pattern it was asked for.
func keyedSearch(patterns *[]string, keys ...string) SearchFunc {
	return func(_ context.Context, topic string, p kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		*patterns = append(*patterns, p.Pattern.String())
		matchCh := make(chan kafka.ConsumedMessage, len(keys))
		progCh := make(chan kafka.DeepSearchProgress, 1)
		errCh := make(chan error)
		for i, k := range keys {
			matchCh <- kafka.ConsumedMessage{Topic: topic, Key: k, Value: "v-" + k, Offset: int64(i)}
		}
		progCh <- kafka.DeepSearchProgress{Done: true, Matches: len(keys)}
		close(errCh)
		close(progCh)
		close(matchCh)
		return matchCh, progCh, errCh
	}
}

// TestSearchView_Filter narrows the results locally, including matches that
// arrive after the filter is set, and enter opens the row under the cursor
// rather than the match at the same index of the unfiltered list.
func TestSearchView_Filter(t *testing.T) {
	var patterns []string
	v := NewSearchView(keyedSearch(&patterns, "alpha", "beta", "alphabet"), "t", "a")
	v.Resize(120, 20)
	v.SetFilter("^beta$")
	drainView(t, v, v.Init())

	if v.Count() != 1 {
		t.Fatalf("Count() = %d with filter ^beta$, want 1", v.Count())
	}
	if m, ok := v.SelectedMessage(); !ok || m.Key != "beta" {
		t.Errorf("selected %q, want beta", m.Key)
	}
	if got := v.VisibleMatches(); len(got) != 1 || got[0].Key != "beta" {
		t.Errorf("VisibleMatches() = %v, want only beta", got)
	}
	if action, param := v.HandleKey(KeyEnter); action != "search_message_detail" || param != "0" {
		t.Errorf("enter = (%q, %q), want the first visible row", action, param)
	}

	v.SetFilter("!beta")
	if v.Count() != 2 {
		t.Errorf("Count() = %d with !beta, want 2", v.Count())
	}
	v.SetFilter("")
	if v.Count() != 3 || len(v.Matches()) != 3 {
		t.Errorf("cleared filter: Count() = %d, matches %d; want 3 and 3", v.Count(), len(v.Matches()))
	}
}

// TestSearchView_Rerun starts the scan again from an empty result set, with
// the edited query when there is one, and ignores what the old scan sends.
func TestSearchView_Rerun(t *testing.T) {
	var patterns []string
	v := NewSearchView(keyedSearch(&patterns, "a", "b"), "t", "first")
	drainView(t, v, v.Init())
	if v.Count() != 2 {
		t.Fatalf("first run: Count() = %d, want 2", v.Count())
	}
	oldID := v.scanID

	if action, _ := v.HandleKey(KeyRerunSearch); action != ActionRerunSearch {
		t.Errorf("R = %q, want %q", action, ActionRerunSearch)
	}
	v.SetQuery("second")
	cmd := v.Rerun()
	if v.Count() != 0 || v.state != searchStateRunning {
		t.Errorf("after Rerun: Count() = %d, state %d; want an empty running scan", v.Count(), v.state)
	}
	// A match still in flight from the first scan must not land in the second.
	v.Update(SearchMatchMsg{ScanID: oldID, Msg: kafka.ConsumedMessage{Key: "stale"}}) //nolint:errcheck
	drainView(t, v, cmd)

	if v.Count() != 2 {
		t.Errorf("second run: Count() = %d, want 2 (a stale match leaked or the rerun found nothing)", v.Count())
	}
	if len(patterns) != 2 || patterns[1] != "(?i)second" {
		t.Errorf("patterns searched = %q, want the second run to use the edited query", patterns)
	}
}
