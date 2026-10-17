// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"errors"

	"github.com/blairham/k4a/internal/kafka"
)

// Overlay returns c with every field o sets replacing c's, the "flags
// override config" rule. Basic credentials and a bearer token are
// alternatives, so setting one kind drops the other kind from c rather than
// leaving both for New to refuse.
func (c Config) Overlay(o Config) Config {
	if o.Token != "" {
		c.Username, c.Password = "", ""
	}
	if o.Username != "" || o.Password != "" {
		c.Token = ""
	}
	for _, f := range []struct{ dst, src *string }{
		{&c.URL, &o.URL},
		{&c.Username, &o.Username},
		{&c.Password, &o.Password},
		{&c.Token, &o.Token},
		{&c.CAFile, &o.CAFile},
	} {
		if *f.src != "" {
			*f.dst = *f.src
		}
	}
	return c
}

// NewIfSet builds a Registry for cfg, or returns nil when no registry is
// configured.
func NewIfSet(cfg Config) (*Registry, error) {
	if !cfg.IsSet() {
		return nil, nil //nolint:nilnil // a nil Registry is "decoding off"; every method accepts it
	}
	return New(cfg)
}

// DecodeMessage decodes m's key and value in place when they are registry
// framed: the shown text becomes the decoded JSON and the Decoding fields
// record the schema and the raw rendering. A framed field that cannot be
// decoded keeps its raw rendering and gets a reason. A nil Registry does
// nothing, so callers need not check whether one is configured.
func (r *Registry) DecodeMessage(ctx context.Context, m *kafka.ConsumedMessage) {
	if r == nil {
		return
	}
	r.DecodeRecord(ctx, m)
}

// DecodeRecord is DecodeMessage for search and indexing (kafka.RecordDecoder):
// it reports false when a framed field was left undecoded because the
// registry could not answer, rather than because of what the record holds,
// so the same record may decode differently later. A nil Registry decodes
// nothing and is always settled.
func (r *Registry) DecodeRecord(ctx context.Context, m *kafka.ConsumedMessage) bool {
	if r == nil {
		return true
	}
	var keyUnsettled, valueUnsettled bool
	m.KeyDecoding, keyUnsettled = r.decodeField(ctx, m.Topic, true, m.KeySchemaID, m.RawKey, &m.Key)
	m.ValueDecoding, valueUnsettled = r.decodeField(ctx, m.Topic, false, m.ValueSchemaID, m.RawValue, &m.Value)
	return !keyUnsettled && !valueUnsettled
}

// SearchDecoder is r as the decoder a search and an index match with, or nil
// when no registry is configured: a nil *Registry inside a non-nil interface
// would read as "decoding" to every identity check.
func (r *Registry) SearchDecoder() kafka.RecordDecoder {
	if r == nil {
		return nil
	}
	return r
}

func (r *Registry) decodeField(
	ctx context.Context,
	topic string,
	isKey bool,
	schemaID, raw []byte,
	shown *string,
) (d kafka.Decoding, unsettled bool) {
	if raw == nil {
		return kafka.Decoding{}, false
	}
	dec, err := r.DecodeWithHeader(ctx, topic, isKey, schemaID, raw)
	switch {
	case errors.Is(err, ErrNotFramed):
		return kafka.Decoding{}, false
	case errors.Is(err, context.DeadlineExceeded):
		// The lookup carries on in the background; later records with this
		// schema decode once it lands.
		return kafka.Decoding{Reason: "schema registry did not answer in time"}, true
	case err != nil:
		return kafka.Decoding{Reason: err.Error()}, isUnavailable(err)
	}
	out := kafka.Decoding{Raw: *shown, Schema: dec.Ref()}
	*shown = dec.JSON
	return out, false
}
