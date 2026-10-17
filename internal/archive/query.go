// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// maxQueryBytes keeps the generated SQL under Athena's 262144-byte query
// string limit with room to spare.
const maxQueryBytes = 250_000

// Field names used in coverage reports.
const (
	FieldKey     = "key"
	FieldValue   = "value"
	FieldHeaders = "headers"
)

// Request is one archive search.
type Request struct {
	Since      time.Time
	Until      time.Time
	Pattern    *regexp.Regexp
	Topic      string
	Partitions []int32
	Scope      kafka.SearchScope
	Cap        int
}

// Plan is a validated, ready-to-run archive query plus what it will cover.
type Plan struct {
	Since          time.Time
	Until          time.Time
	pattern        *regexp.Regexp
	cols           Columns
	FirstPartition string
	Topic          string
	Literal        string
	LastPartition  string
	SQL            string
	Prefixes       []string
	// Fields are the columns searched; MissingFields are fields the scope
	// asked for that the archive does not have.
	Fields         []string
	MissingFields  []string
	PartitionCount int
	Limit          int
	scope          kafka.SearchScope
}

// BuildPlan turns a request into SQL against the topic's table. catalog
// must already be validated (Config.Topic does).
func BuildPlan(catalog string, t Topic, req Request) (Plan, error) {
	if req.Pattern == nil {
		return Plan{}, errors.New("archive search needs a pattern")
	}
	if req.Since.IsZero() {
		return Plan{}, errors.New("archive search needs --since: an unbounded search would scan the whole archive")
	}
	if !req.Until.After(req.Since) {
		return Plan{}, errors.New("--until must be after --since")
	}
	lit, err := CandidateLiteral(req.Pattern.String())
	if err != nil {
		return Plan{}, err
	}
	if len(req.Partitions) > 0 && t.Columns.Partition == "" {
		return Plan{}, errors.New("--partitions needs columns.partition in the archive config")
	}
	loc, err := t.location()
	if err != nil {
		return Plan{}, err
	}

	p := Plan{
		Topic:   req.Topic,
		Since:   req.Since,
		Until:   req.Until,
		Literal: lit,
		Limit:   req.Cap,
		pattern: req.Pattern,
		cols:    t.Columns,
		scope:   req.Scope,
	}
	if p.Limit <= 0 {
		p.Limit = kafka.SearchParams{}.EffectiveCap()
	}
	if p.scope == 0 {
		p.scope = kafka.ScopeKey | kafka.ScopeValue
	}
	p.setFields()
	if len(p.Fields) == 0 {
		return Plan{}, fmt.Errorf("the archive has none of the fields --scope asks for (%s)",
			strings.Join(p.MissingFields, ", "))
	}

	tuples := partitionTuples(t.TimePartitions, loc, req.Since, req.Until)
	p.PartitionCount = len(tuples)
	p.FirstPartition = strings.Join(tuples[0], "/")
	p.LastPartition = strings.Join(tuples[len(tuples)-1], "/")
	p.Prefixes = locationPrefixes(t.Location, loc, req.Since, req.Until)

	p.SQL = p.buildSQL(catalog, t, tuples, req.Partitions)
	if len(p.SQL) > maxQueryBytes {
		return Plan{}, fmt.Errorf("the time range selects %d partitions, too many for one query; narrow --since/--until",
			len(tuples))
	}
	return p, nil
}

func (p *Plan) setFields() {
	for _, f := range []struct {
		name  string
		col   string
		scope kafka.SearchScope
	}{
		{FieldKey, p.cols.Key, kafka.ScopeKey},
		{FieldValue, p.cols.Value, kafka.ScopeValue},
		{FieldHeaders, p.cols.Headers, kafka.ScopeHeaders},
	} {
		if p.scope&f.scope == 0 {
			continue
		}
		if f.col == "" {
			p.MissingFields = append(p.MissingFields, f.name)
		} else {
			p.Fields = append(p.Fields, f.name)
		}
	}
}

// selected is the column order of the SELECT list; parsing depends on it.
func (p *Plan) selected() []string {
	out := []string{p.cols.Value, p.cols.Timestamp}
	for _, c := range []string{p.cols.Key, p.cols.Headers, p.cols.Partition, p.cols.Offset} {
		if c != "" {
			out = append(out, c)
		}
	}
	return out
}

func (p *Plan) fieldColumn(name string) string {
	switch name {
	case FieldKey:
		return p.cols.Key
	case FieldHeaders:
		return p.cols.Headers
	default:
		return p.cols.Value
	}
}

func (p *Plan) buildSQL(catalog string, t Topic, tuples [][]string, partitions []int32) string {
	sel := p.selected()
	quoted := make([]string, len(sel))
	for i, c := range sel {
		quoted[i] = quoteIdent(c)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "SELECT %s\nFROM %s.%s.%s\nWHERE %s\n  AND %s",
		strings.Join(quoted, ", "),
		quoteIdent(catalog), quoteIdent(t.Database), quoteIdent(t.Table),
		partitionPredicate(t.TimePartitions, tuples),
		p.timePredicate())

	cands := make([]string, 0, len(p.Fields))
	for _, f := range p.Fields {
		cands = append(cands, fmt.Sprintf("strpos(lower(%s), %s) > 0",
			quoteIdent(p.fieldColumn(f)), quoteString(p.Literal)))
	}
	fmt.Fprintf(&b, "\n  AND (%s)", strings.Join(cands, " OR "))

	if len(partitions) > 0 {
		ids := make([]string, len(partitions))
		for i, id := range partitions {
			ids[i] = strconv.Itoa(int(id))
		}
		fmt.Fprintf(&b, "\n  AND %s IN (%s)", quoteIdent(p.cols.Partition), strings.Join(ids, ", "))
	}
	fmt.Fprintf(&b, "\nORDER BY %s DESC\nLIMIT %d", quoteIdent(p.cols.Timestamp), p.Limit)
	return b.String()
}

const athenaTimestampLayout = "2006-01-02 15:04:05.000"

func (p *Plan) timePredicate() string {
	col := quoteIdent(p.cols.Timestamp)
	if p.cols.TimestampType == TimestampTimestamp {
		return fmt.Sprintf("%s BETWEEN TIMESTAMP '%s' AND TIMESTAMP '%s'", col,
			p.Since.UTC().Format(athenaTimestampLayout), p.Until.UTC().Format(athenaTimestampLayout))
	}
	return fmt.Sprintf("%s BETWEEN %d AND %d", col, p.Since.UnixMilli(), p.Until.UnixMilli())
}

// CanDedupe reports whether rows carry (partition, offset), the only key
// that identifies a record across a sink's at-least-once redelivery.
func (p *Plan) CanDedupe() bool { return p.cols.Partition != "" && p.cols.Offset != "" }
