// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"testing"

	"github.com/blairham/k4a/internal/jsonpath"
	"github.com/blairham/k4a/internal/kafka"
)

// TestIndexParamsCarryJSONPath: the local dispatcher's CanServe can only
// decline a JSON-path query if the translation hands it the path.
func TestIndexParamsCarryJSONPath(t *testing.T) {
	t.Parallel()
	expr, err := jsonpath.Parse(`.a == 1`)
	if err != nil {
		t.Fatal(err)
	}
	if got := indexParams(kafka.SearchParams{Path: expr}).JSONPath; got != `.a == 1` {
		t.Errorf("indexParams dropped the JSON path: got %q", got)
	}
}
