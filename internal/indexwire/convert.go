// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package indexwire bridges k4a's in-process search domain types
// (internal/index, internal/kafka) and the generated gRPC contract
// (internal/indexwire/indexv1). It is the ONLY place proto types meet domain
// types — internal/index stays proto-free so the same indexer implementation
// backs the local index and the shared daemon (docs/design/shared-index-service.md).
//
// The load-bearing property is coverage parity: CoverageFromBookkeeper and
// BookkeeperFromCoverage round-trip the exact fields index.Bookkeeper.CanServe
// reads, so a client fed a daemon's wire coverage reaches the identical
// index-vs-scan decision it would reach locally.
package indexwire

import (
	"fmt"
	"regexp"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
)

// CoverageFromBookkeeper projects a Bookkeeper snapshot onto the wire Coverage
// message. Only the fields the coverage decision depends on cross the wire —
// per-partition offset/time ranges and gaps. following/backfilling are daemon
// state the caller stamps separately (they are not derivable from the book).
func CoverageFromBookkeeper(b index.Bookkeeper) *indexv1.Coverage {
	cov := &indexv1.Coverage{
		Partitions: make([]*indexv1.PartitionCoverage, 0, len(b.NewestOffsetPerPartition)),
	}
	for p, hiOff := range b.NewestOffsetPerPartition {
		cov.Partitions = append(cov.Partitions, &indexv1.PartitionCoverage{
			Partition:     p,
			OffsetLo:      b.OldestOffsetPerPartition[p],
			OffsetHi:      hiOff,
			TimestampLoMs: b.OldestTimePerPartition[p],
			TimestampHiMs: b.NewestTimePerPartition[p],
		})
	}
	for _, g := range b.Gaps {
		cov.Gaps = append(cov.Gaps, &indexv1.Gap{
			Partition: g.Partition,
			Start:     g.Start,
			End:       g.End,
			Reason:    g.Reason,
		})
	}
	return cov
}

// BookkeeperFromCoverage is the inverse: it rebuilds the subset of a
// Bookkeeper that Bookkeeper.CanServe consults, so the client can run that
// exact decision on a remote coverage snapshot. Fields CanServe never reads
// (ByteSize, FirstIndexedAt, …) are left zero — they are not part of the
// decision and must not be invented here.
func BookkeeperFromCoverage(cov *indexv1.Coverage) index.Bookkeeper {
	b := index.Bookkeeper{
		SchemaVersion:            index.SchemaVersion,
		NewestOffsetPerPartition: make(map[int32]int64),
		OldestOffsetPerPartition: make(map[int32]int64),
		NewestTimePerPartition:   make(map[int32]int64),
		OldestTimePerPartition:   make(map[int32]int64),
	}
	if cov == nil {
		return b
	}
	for _, pc := range cov.GetPartitions() {
		b.NewestOffsetPerPartition[pc.GetPartition()] = pc.GetOffsetHi()
		b.OldestOffsetPerPartition[pc.GetPartition()] = pc.GetOffsetLo()
		b.NewestTimePerPartition[pc.GetPartition()] = pc.GetTimestampHiMs()
		b.OldestTimePerPartition[pc.GetPartition()] = pc.GetTimestampLoMs()
	}
	for _, g := range cov.GetGaps() {
		b.Gaps = append(b.Gaps, index.Gap{
			Partition: g.GetPartition(),
			Start:     g.GetStart(),
			End:       g.GetEnd(),
			Reason:    g.GetReason(),
		})
	}
	return b
}

// RequestToQueryParams converts a wire SearchRequest into the index layer's
// QueryParams, compiling the regex source. A malformed pattern is a client
// error surfaced here rather than at query time.
func RequestToQueryParams(req *indexv1.SearchRequest) (index.QueryParams, error) {
	params := index.QueryParams{
		Scopes:     req.GetScopes(),
		Partitions: req.GetPartitions(),
		Limit:      int(req.GetLimit()),
	}
	if p := req.GetPattern(); p != "" {
		re, err := regexp.Compile(p)
		if err != nil {
			return index.QueryParams{}, fmt.Errorf("compile pattern %q: %w", p, err)
		}
		params.Pattern = re
	}
	params.Since = msToTime(req.GetSinceMs())
	params.Until = msToTime(req.GetUntilMs())
	return params, nil
}

// QueryParamsToRequest is the inverse, for the client side. A nil Pattern
// becomes the empty string (match-all), matching RequestToQueryParams.
func QueryParamsToRequest(topic string, params index.QueryParams) *indexv1.SearchRequest {
	req := &indexv1.SearchRequest{
		Topic:      topic,
		Scopes:     params.Scopes,
		Partitions: params.Partitions,
		Limit:      int32(params.Limit), //nolint:gosec // search limit fits in int32
		SinceMs:    timeToMS(params.Since),
		UntilMs:    timeToMS(params.Until),
	}
	if params.Pattern != nil {
		req.Pattern = params.Pattern.String()
	}
	return req
}

// MatchFromConsumed converts an indexed record to its wire form.
func MatchFromConsumed(m kafka.ConsumedMessage) *indexv1.Match {
	match := &indexv1.Match{
		Partition:   int32(m.Partition), //nolint:gosec // partition IDs fit in int32
		Offset:      m.Offset,
		TimestampMs: timeToMS(m.Time),
		Key:         m.Key,
		Value:       m.Value,
	}
	if len(m.Headers) > 0 {
		match.Headers = make([]*indexv1.Header, 0, len(m.Headers))
		for _, h := range m.Headers {
			match.Headers = append(match.Headers, &indexv1.Header{Key: h.Key, Value: h.Value})
		}
	}
	return match
}

// ConsumedFromMatch is the inverse, reconstructing the kafka.ConsumedMessage
// the client's search channels already carry.
func ConsumedFromMatch(topic string, match *indexv1.Match) kafka.ConsumedMessage {
	m := kafka.ConsumedMessage{
		Topic:     topic,
		Partition: int(match.GetPartition()),
		Offset:    match.GetOffset(),
		Time:      msToTime(match.GetTimestampMs()),
		Key:       match.GetKey(),
		Value:     match.GetValue(),
	}
	if hs := match.GetHeaders(); len(hs) > 0 {
		m.Headers = make([]kafka.MessageHeader, 0, len(hs))
		for _, h := range hs {
			m.Headers = append(m.Headers, kafka.MessageHeader{Key: h.GetKey(), Value: h.GetValue()})
		}
	}
	return m
}

// msToTime maps Unix-millis (0 = unset) to a time.Time (zero = unset), the
// convention QueryParams uses for open-ended bounds.
func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// timeToMS is the inverse; a zero time maps back to 0.
func timeToMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}
