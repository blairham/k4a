// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import "charm.land/bubbles/v2/table"

// colName is the title of the name column the topics, topic-config and ACL
// tables share.
const colName = "NAME"

// setTableRows replaces a table's rows and repairs the cursor.
//
// bubbles' Model.SetRows clamps the cursor *down* when the row set shrinks —
// and an empty row set clamps it to -1 (`cursor = len(rows)-1`). The clamp is
// one-directional: when rows come back, the guard `cursor > len(rows)-1` is
// false for -1, so the cursor is never restored. SelectedRow() returns nil for
// a negative cursor, which silently kills every row action (enter, o, p,
// ctrl-d) for the rest of the session.
//
// Any filter that matches nothing reaches that state — in the topics list,
// the messages buffer, groups, ACLs — so a single empty filter would otherwise
// wedge the view even after the filter is cleared.
func setTableRows(t *table.Model, rows []table.Row) {
	t.SetRows(rows)
	if len(rows) > 0 && t.Cursor() < 0 {
		t.SetCursor(0)
	}
}
