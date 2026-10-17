// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"

	"github.com/blairham/k4a/internal/kafka"
)

// AthenaAPI is the subset of *athena.Client the archive calls, so tests can
// pass a fake.
type AthenaAPI interface {
	StartQueryExecution(context.Context, *athena.StartQueryExecutionInput, ...func(*athena.Options)) (
		*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(context.Context, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (
		*athena.GetQueryExecutionOutput, error)
	GetQueryResults(context.Context, *athena.GetQueryResultsInput, ...func(*athena.Options)) (
		*athena.GetQueryResultsOutput, error)
	StopQueryExecution(context.Context, *athena.StopQueryExecutionInput, ...func(*athena.Options)) (
		*athena.StopQueryExecutionOutput, error)
}

// ErrBytesCap is returned when the watchdog stopped a query for scanning
// more than the cap.
var ErrBytesCap = errors.New("query stopped: bytes scanned passed max-bytes")

// Runner executes plans in one workgroup.
type Runner struct {
	Athena         AthenaAPI
	Sleep          func(context.Context, time.Duration) error
	Workgroup      string
	OutputLocation string
	PollInterval   time.Duration
	MaxPoll        time.Duration
	// MaxBytes stops a running query once it has scanned more; 0 disables
	// the watchdog.
	MaxBytes int64
}

// Result is what an archive query returned and covered.
type Result struct {
	QueryID           string
	Matches           []kafka.ConsumedMessage
	ScannedBytes      int64
	Elapsed           time.Duration
	Candidates        int
	DuplicatesDropped int
	Capped            bool
}

// Run starts the query, waits for it under the watchdog, then reads and
// filters the rows. A canceled ctx stops the query in Athena too, so an
// interrupted search does not keep billing.
func (r *Runner) Run(ctx context.Context, p Plan) (Result, error) {
	start := time.Now()
	in := &athena.StartQueryExecutionInput{
		QueryString: aws.String(p.SQL),
		WorkGroup:   aws.String(r.Workgroup),
	}
	if r.OutputLocation != "" {
		in.ResultConfiguration = &types.ResultConfiguration{OutputLocation: aws.String(r.OutputLocation)}
	}
	out, err := r.Athena.StartQueryExecution(ctx, in)
	if err != nil {
		return Result{}, fmt.Errorf("starting Athena query: %w", err)
	}
	res := Result{QueryID: aws.ToString(out.QueryExecutionId)}

	scanned, err := r.wait(ctx, res.QueryID)
	res.ScannedBytes = scanned
	if err != nil {
		return res, err
	}
	if err := r.collect(ctx, p, &res); err != nil {
		return res, err
	}
	res.Elapsed = time.Since(start)
	return res, nil
}

func (r *Runner) wait(ctx context.Context, id string) (int64, error) {
	interval := r.PollInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	maxPoll := r.MaxPoll
	if maxPoll <= 0 {
		maxPoll = 2 * time.Second
	}
	sleep := r.Sleep
	if sleep == nil {
		sleep = sleepCtx
	}
	for {
		out, err := r.Athena.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: aws.String(id)})
		if err != nil {
			return 0, r.stopAfter(ctx, id, fmt.Errorf("polling Athena query %s: %w", id, err))
		}
		qe := out.QueryExecution
		var scanned int64
		if qe != nil && qe.Statistics != nil {
			scanned = aws.ToInt64(qe.Statistics.DataScannedInBytes)
		}
		var state types.QueryExecutionState
		var reason string
		if qe != nil && qe.Status != nil {
			state = qe.Status.State
			reason = aws.ToString(qe.Status.StateChangeReason)
		}
		switch state {
		case types.QueryExecutionStateSucceeded:
			return scanned, nil
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			return scanned, fmt.Errorf("archive query %s ended %s: %s", id, state, reason)
		default:
		}
		if r.MaxBytes > 0 && scanned > r.MaxBytes {
			return scanned, r.stopAfter(ctx, id, fmt.Errorf("%w (%s > %s, query %s)",
				ErrBytesCap, FormatSize(scanned), FormatSize(r.MaxBytes), id))
		}
		if err := sleep(ctx, interval); err != nil {
			return scanned, r.stopAfter(ctx, id, err)
		}
		interval = min(interval*2, maxPoll)
	}
}

// stopAfter stops a query that is being abandoned because of cause, and
// reports a failed stop alongside it: a query left running keeps billing.
// The stop ignores ctx's cancellation, since a canceled caller is exactly
// when the query most needs stopping.
func (r *Runner) stopAfter(ctx context.Context, id string, cause error) error {
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if _, err := r.Athena.StopQueryExecution(sctx, &athena.StopQueryExecutionInput{
		QueryExecutionId: aws.String(id),
	}); err != nil {
		return errors.Join(cause, fmt.Errorf("stopping query %s failed; stop it in the Athena console: %w", id, err))
	}
	return cause
}

func (r *Runner) collect(ctx context.Context, p Plan, res *Result) error {
	pager := athena.NewGetQueryResultsPaginator(r.Athena, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(res.QueryID),
	})
	seen := map[[2]int64]bool{}
	header := true
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("reading Athena results for %s: %w", res.QueryID, err)
		}
		if page.ResultSet == nil {
			continue
		}
		rows := page.ResultSet.Rows
		if header && len(rows) > 0 {
			// A SELECT's first row is the column names.
			header = false
			rows = rows[1:]
		}
		for _, row := range rows {
			if err := p.addRow(row.Data, seen, res); err != nil {
				return err
			}
		}
	}
	res.Capped = res.Candidates >= p.Limit
	return nil
}

// addRow counts one candidate row, drops a sink redelivery, and keeps it
// if the exact regex matches.
func (p *Plan) addRow(data []types.Datum, seen map[[2]int64]bool, res *Result) error {
	res.Candidates++
	m, err := p.parseRow(data)
	if err != nil {
		return err
	}
	if p.CanDedupe() {
		k := [2]int64{int64(m.Partition), m.Offset}
		if seen[k] {
			res.DuplicatesDropped++
			return nil
		}
		seen[k] = true
	}
	if p.matches(m) {
		res.Matches = append(res.Matches, m)
	}
	return nil
}

// parseRow maps a row in selected() order to a message. Partition and
// offset are -1 when the archive does not record them.
func (p *Plan) parseRow(data []types.Datum) (kafka.ConsumedMessage, error) {
	sel := p.selected()
	if len(data) != len(sel) {
		return kafka.ConsumedMessage{}, fmt.Errorf("archive result row has %d columns, want %d", len(data), len(sel))
	}
	m := kafka.ConsumedMessage{Topic: p.Topic, Partition: -1, Offset: -1}
	m.Value = aws.ToString(data[0].VarCharValue)
	ts, err := p.parseTimestamp(aws.ToString(data[1].VarCharValue))
	if err != nil {
		return m, err
	}
	m.Time = ts
	i := 2
	next := func() string {
		v := aws.ToString(data[i].VarCharValue)
		i++
		return v
	}
	if p.cols.Key != "" {
		m.Key = next()
	}
	if p.cols.Headers != "" {
		if h := next(); h != "" {
			m.Headers = []kafka.MessageHeader{{Key: p.cols.Headers, Value: h}}
		}
	}
	if p.cols.Partition != "" {
		if m.Partition, err = parseIntCol(next(), "partition"); err != nil {
			return m, err
		}
	}
	if p.cols.Offset != "" {
		v := next()
		if m.Offset, err = strconv.ParseInt(v, 10, 64); err != nil {
			return m, fmt.Errorf("archive offset %q is not an integer", v)
		}
	}
	return m, nil
}

func parseIntCol(v, name string) (int, error) {
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("archive %s %q is not an integer", name, v)
	}
	return n, nil
}

func (p *Plan) parseTimestamp(v string) (time.Time, error) {
	if p.cols.TimestampType == TimestampTimestamp {
		t, err := time.ParseInLocation("2006-01-02 15:04:05.999999999", v, time.UTC)
		if err != nil {
			return time.Time{}, fmt.Errorf("archive timestamp %q: %w", v, err)
		}
		return t, nil
	}
	ms, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("archive timestamp %q is not epoch milliseconds", v)
	}
	return time.UnixMilli(ms).UTC(), nil
}

// matches re-applies the exact regex: the SQL only selected candidates.
func (p *Plan) matches(m kafka.ConsumedMessage) bool {
	for _, f := range p.Fields {
		var s string
		switch f {
		case FieldKey:
			s = m.Key
		case FieldHeaders:
			if len(m.Headers) > 0 {
				s = m.Headers[0].Value
			}
		default:
			s = m.Value
		}
		if p.pattern.MatchString(s) {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
