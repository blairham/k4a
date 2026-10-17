// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"fmt"
	"strings"
	"time"
)

// Notes every archive result carries: the archive and Kafka cover
// overlapping but different windows, and an archive search sees only one.
const (
	NoteSinkLag   = "records newer than the sink's flush interval may exist only in Kafka; search them without --archive"
	NoteRetention = "archived records may have been deleted from Kafka by retention or compaction, so their offsets may not be consumable"
	NoteNoDedupe  = "the archive has no partition and offset columns, so sink redeliveries were not de-duplicated"
)

// Coverage is what an archive result covered. It never describes Kafka:
// an archive search does not read the cluster.
type Coverage struct {
	Since             time.Time `json:"since"`
	Until             time.Time `json:"until"`
	EstimatedBytes    *int64    `json:"estimated_bytes,omitempty"`
	FirstPartition    string    `json:"first_partition"`
	LastPartition     string    `json:"last_partition"`
	CandidateLiteral  string    `json:"candidate_literal"`
	QueryExecutionID  string    `json:"query_execution_id"`
	Fields            []string  `json:"fields"`
	MissingFields     []string  `json:"missing_fields,omitempty"`
	Notes             []string  `json:"notes"`
	ScannedBytes      int64     `json:"scanned_bytes"`
	Partitions        int       `json:"partitions"`
	Candidates        int       `json:"candidates"`
	DuplicatesDropped int       `json:"duplicates_dropped"`
	Deduplicated      bool      `json:"deduplicated"`
	Capped            bool      `json:"capped"`
}

// NewCoverage describes a finished query. estimate is nil when no
// location was configured to estimate from.
func NewCoverage(p Plan, estimate *int64, r Result) Coverage {
	c := Coverage{
		Since:             p.Since,
		Until:             p.Until,
		EstimatedBytes:    estimate,
		FirstPartition:    p.FirstPartition,
		LastPartition:     p.LastPartition,
		CandidateLiteral:  p.Literal,
		QueryExecutionID:  r.QueryID,
		Fields:            p.Fields,
		MissingFields:     p.MissingFields,
		ScannedBytes:      r.ScannedBytes,
		Partitions:        p.PartitionCount,
		Candidates:        r.Candidates,
		DuplicatesDropped: r.DuplicatesDropped,
		Deduplicated:      p.CanDedupe(),
		Capped:            r.Capped,
		Notes:             []string{NoteSinkLag, NoteRetention},
	}
	if !c.Deduplicated {
		c.Notes = append(c.Notes, NoteNoDedupe)
	}
	if len(p.MissingFields) > 0 {
		c.Notes = append(c.Notes, fmt.Sprintf("--scope asked for %s, which the archive does not store",
			strings.Join(p.MissingFields, ", ")))
	}
	return c
}

// Lines renders the coverage for a human, one fact per line.
func (c Coverage) Lines(matches int) []string {
	est := "no estimate"
	if c.EstimatedBytes != nil {
		est = "estimated " + FormatSize(*c.EstimatedBytes)
	}
	out := []string{
		fmt.Sprintf("archive · %d matches · scanned %s (%s) · query %s",
			matches, FormatSize(c.ScannedBytes), est, c.QueryExecutionID),
		fmt.Sprintf("window %s → %s · %d partitions (%s … %s) · fields %s · literal %q",
			c.Since.UTC().Format(time.RFC3339), c.Until.UTC().Format(time.RFC3339),
			c.Partitions, c.FirstPartition, c.LastPartition, strings.Join(c.Fields, "+"), c.CandidateLiteral),
	}
	if c.Capped {
		out = append(out, fmt.Sprintf("capped at %d candidate rows — narrow the search or raise --cap", c.Candidates))
	}
	if c.DuplicatesDropped > 0 {
		out = append(out, fmt.Sprintf("%d duplicate rows dropped", c.DuplicatesDropped))
	}
	for _, n := range c.Notes {
		out = append(out, "note: "+n)
	}
	return out
}
