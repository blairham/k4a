// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// tokenLayouts maps each partition-format token to its Go time layout.
var tokenLayouts = map[string]string{
	"{yyyy}": "2006",
	"{MM}":   "01",
	"{dd}":   "02",
	"{HH}":   "15",
}

var braceRE = regexp.MustCompile(`\{[^}]*\}`)

// checkFormat rejects a brace group that is not a known token, which is
// almost always a typo ({YYYY}, {mm}) that would otherwise be matched as
// literal text and prune every partition away.
func checkFormat(f string) error {
	for _, m := range braceRE.FindAllString(f, -1) {
		if _, ok := tokenLayouts[m]; !ok {
			return fmt.Errorf("unknown token %s in %q (use {yyyy}, {MM}, {dd}, {HH})", m, f)
		}
	}
	return nil
}

func hasToken(f string) bool {
	for tok := range tokenLayouts {
		if strings.Contains(f, tok) {
			return true
		}
	}
	return false
}

// formatAt renders a partition format or location template at t.
func formatAt(f string, t time.Time) string {
	return braceRE.ReplaceAllStringFunc(f, func(tok string) string {
		if layout, ok := tokenLayouts[tok]; ok {
			return t.Format(layout)
		}
		return tok
	})
}

// sampleStep is the spacing of sample instants. Every time zone in use
// offsets UTC by a multiple of 15 minutes, so every local partition
// boundary falls on a UTC quarter hour.
const sampleStep = 15 * time.Minute

// samples returns since, every UTC quarter hour inside (since, until], and
// until. A partition that intersects [since, until] either starts inside
// (since, until], on a quarter hour that is sampled, or started at or
// before since and so contains it. That holds for any zone, half- and
// quarter-hour offsets, and DST changes that shorten a local hour.
func samples(since, until time.Time) []time.Time {
	out := []time.Time{since}
	for q := since.UTC().Truncate(sampleStep).Add(sampleStep); !q.After(until); q = q.Add(sampleStep) {
		out = append(out, q)
	}
	if until.After(since) {
		out = append(out, until)
	}
	return out
}

// partitionTuples is every distinct tuple of time-partition values the
// range [since, until] touches, oldest first.
func partitionTuples(parts []TimePartition, loc *time.Location, since, until time.Time) [][]string {
	seen := map[string]bool{}
	var out [][]string
	for _, t := range samples(since, until) {
		lt := t.In(loc)
		tuple := make([]string, len(parts))
		for i, p := range parts {
			tuple[i] = formatAt(p.Format, lt)
		}
		k := strings.Join(tuple, "\x00")
		if !seen[k] {
			seen[k] = true
			out = append(out, tuple)
		}
	}
	return out
}

// locationPrefixes is every distinct S3 prefix of the location template the
// range touches.
func locationPrefixes(location string, loc *time.Location, since, until time.Time) []string {
	if location == "" {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, t := range samples(since, until) {
		p := formatAt(location, t.In(loc))
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// partitionPredicate constrains the partition columns to exactly tuples.
// One column is an IN list; several are a nested OR of ANDs grouped by the
// leading columns, which keeps the SQL short (a month of hourly partitions
// is ~30 day groups, not 720 four-way conjunctions).
func partitionPredicate(parts []TimePartition, tuples [][]string) string {
	return groupPredicate(parts, tuples, 0)
}

func groupPredicate(parts []TimePartition, tuples [][]string, level int) string {
	p := parts[level]
	col := quoteIdent(p.Column)
	if level == len(parts)-1 {
		vals := make([]string, 0, len(tuples))
		seen := map[string]bool{}
		for _, t := range tuples {
			if !seen[t[level]] {
				seen[t[level]] = true
				vals = append(vals, partitionLiteral(p, t[level]))
			}
		}
		return col + " IN (" + strings.Join(vals, ", ") + ")"
	}
	var order []string
	groups := map[string][][]string{}
	for _, t := range tuples {
		v := t[level]
		if _, ok := groups[v]; !ok {
			order = append(order, v)
		}
		groups[v] = append(groups[v], t)
	}
	terms := make([]string, 0, len(order))
	for _, v := range order {
		terms = append(terms, fmt.Sprintf("(%s = %s AND %s)",
			col, partitionLiteral(p, v), groupPredicate(parts, groups[v], level+1)))
	}
	if len(terms) == 1 {
		return terms[0]
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

func partitionLiteral(p TimePartition, v string) string {
	if p.Type == PartitionInteger {
		n, err := strconv.Atoi(v)
		if err == nil {
			return strconv.Itoa(n)
		}
	}
	return quoteString(v)
}

// quoteIdent double-quotes a validated identifier.
func quoteIdent(s string) string { return `"` + s + `"` }

// quoteString is a SQL string literal: the only escape Athena needs inside
// single quotes is a doubled quote.
func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
