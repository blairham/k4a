// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

type namedDecoder string

func (d namedDecoder) Identity() string { return string(d) }

func (namedDecoder) DecodeRecord(context.Context, *kafka.ConsumedMessage) bool { return true }

// TestWithDecoderReachesEverySource: the decoder is set once, outermost, so
// the cache key, both indexes' coverage checks and the scan read the same one.
func TestWithDecoderReachesEverySource(t *testing.T) {
	var got kafka.SearchParams
	next := func(_ context.Context, _ string, p kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		got = p
		return nil, nil, nil
	}
	withDecoder(namedDecoder("sr:a"), next)(context.Background(), "t", kafka.SearchParams{})
	if got.DecoderIdentity() != "sr:a" {
		t.Errorf("source saw decoder %q, want sr:a", got.DecoderIdentity())
	}
	if p := indexParams(got); p.Decoder != "sr:a" {
		t.Errorf("local index coverage check got decoder %q, want sr:a", p.Decoder)
	}
	withDecoder(nil, next)(context.Background(), "t", kafka.SearchParams{})
	if got.Decoder != nil {
		t.Errorf("no registry still set decoder %v", got.Decoder)
	}
}
