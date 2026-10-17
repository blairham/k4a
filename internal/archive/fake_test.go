// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// fakeAthena plays back a sequence of query states and result pages.
type fakeAthena struct {
	startErr error
	started  *athena.StartQueryExecutionInput
	states   []fakeState
	pages    [][][]string
	stopped  []string
	polls    int
	mu       sync.Mutex
}

type fakeState struct {
	state   types.QueryExecutionState
	reason  string
	scanned int64
}

func (f *fakeAthena) StartQueryExecution(
	_ context.Context, in *athena.StartQueryExecutionInput, _ ...func(*athena.Options),
) (*athena.StartQueryExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = in
	if f.startErr != nil {
		return nil, f.startErr
	}
	return &athena.StartQueryExecutionOutput{QueryExecutionId: aws.String("q-1")}, nil
}

func (f *fakeAthena) GetQueryExecution(
	_ context.Context, _ *athena.GetQueryExecutionInput, _ ...func(*athena.Options),
) (*athena.GetQueryExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.states[min(f.polls, len(f.states)-1)]
	f.polls++
	return &athena.GetQueryExecutionOutput{QueryExecution: &types.QueryExecution{
		Status:     &types.QueryExecutionStatus{State: s.state, StateChangeReason: aws.String(s.reason)},
		Statistics: &types.QueryExecutionStatistics{DataScannedInBytes: aws.Int64(s.scanned)},
	}}, nil
}

func (f *fakeAthena) GetQueryResults(
	_ context.Context, in *athena.GetQueryResultsInput, _ ...func(*athena.Options),
) (*athena.GetQueryResultsOutput, error) {
	i := 0
	if in.NextToken != nil {
		i = int(aws.ToString(in.NextToken)[0] - '0')
	}
	if i >= len(f.pages) {
		return nil, errors.New("page out of range")
	}
	out := &athena.GetQueryResultsOutput{ResultSet: &types.ResultSet{}}
	for _, row := range f.pages[i] {
		data := make([]types.Datum, len(row))
		for j, cell := range row {
			if cell != "\x00" {
				data[j].VarCharValue = aws.String(cell)
			}
		}
		out.ResultSet.Rows = append(out.ResultSet.Rows, types.Row{Data: data})
	}
	if i+1 < len(f.pages) {
		out.NextToken = aws.String(string(rune('0' + i + 1)))
	}
	return out, nil
}

func (f *fakeAthena) StopQueryExecution(
	_ context.Context, in *athena.StopQueryExecutionInput, _ ...func(*athena.Options),
) (*athena.StopQueryExecutionOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, aws.ToString(in.QueryExecutionId))
	return &athena.StopQueryExecutionOutput{}, nil
}

// fakeS3 serves object sizes by bucket/key, two objects per page.
type fakeS3 struct {
	err     error
	objects map[string]int64 // "bucket/key" -> size
	calls   int
	mu      sync.Mutex
}

func (f *fakeS3) ListObjectsV2(
	_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	var keys []string
	for k := range f.objects {
		b, key, _ := strings.Cut(k, "/")
		if b == aws.ToString(in.Bucket) && strings.HasPrefix(key, aws.ToString(in.Prefix)) {
			keys = append(keys, key)
		}
	}
	// Deterministic order for paging.
	slices.Sort(keys)
	start := 0
	if in.ContinuationToken != nil {
		for i, k := range keys {
			if k == aws.ToString(in.ContinuationToken) {
				start = i
			}
		}
	}
	end := min(start+2, len(keys))
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys[start:end] {
		out.Contents = append(out.Contents, s3types.Object{
			Key: aws.String(k), Size: aws.Int64(f.objects[aws.ToString(in.Bucket)+"/"+k]),
		})
	}
	if end < len(keys) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(keys[end])
	}
	return out, nil
}
