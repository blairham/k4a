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
	m.KeyDecoding = r.decodeField(ctx, m.Topic, true, m.RawKey, &m.Key)
	m.ValueDecoding = r.decodeField(ctx, m.Topic, false, m.RawValue, &m.Value)
}

func (r *Registry) decodeField(
	ctx context.Context,
	topic string,
	isKey bool,
	raw []byte,
	shown *string,
) kafka.Decoding {
	if raw == nil {
		return kafka.Decoding{}
	}
	d, err := r.Decode(ctx, topic, isKey, raw)
	switch {
	case errors.Is(err, ErrNotFramed):
		return kafka.Decoding{}
	case errors.Is(err, context.DeadlineExceeded):
		// The lookup carries on in the background; later records with this
		// schema decode once it lands.
		return kafka.Decoding{Reason: "schema registry did not answer in time"}
	case err != nil:
		return kafka.Decoding{Reason: err.Error()}
	}
	out := kafka.Decoding{Raw: *shown, Schema: d.Ref()}
	*shown = d.JSON
	return out
}
