// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordDecoder renders schema-registry framed keys and values as the text
// a search matches. The scan and both indexes take the same decoder so that
// an index answers exactly what a scan would (docs/design/schema-registry.md
// § Search and the indexes). *schemareg.Registry implements it; the
// interface lives here because schemareg imports this package.
type RecordDecoder interface {
	// DecodeRecord decodes m's framed key and value in place, as the display
	// decoder does: Key and Value become the decoded JSON and KeyDecoding
	// and ValueDecoding say what happened. It returns false when a framed
	// field could not be decoded for a reason that may pass (an unreachable
	// registry), so the text left in m may differ from what the same decoder
	// produces later.
	DecodeRecord(ctx context.Context, m *ConsumedMessage) (settled bool)
	// Identity names the decoding: two decoders with the same identity turn
	// the same bytes into the same text. It is what the coverage contract
	// compares, so it is never empty and never holds a secret.
	Identity() string
}

// DecoderIdentity is the identity of the decoder a search matches with, or
// "" when it matches the bytes as consumed.
func (p SearchParams) DecoderIdentity() string {
	if p.Decoder == nil {
		return ""
	}
	return p.Decoder.Identity()
}

// couldBeFramed reports whether a decoder could change what r's key or value
// reads as: the same test recordToConsumed applies before keeping the raw
// bytes a decoder needs, so an unframed record never pays for decoding.
func couldBeFramed(r *kgo.Record) bool {
	if framedPrefix(r.Key) || framedPrefix(r.Value) {
		return true
	}
	for _, h := range r.Headers {
		if (h.Key == KeySchemaIDHeader || h.Key == ValueSchemaIDHeader) && h.Value != nil {
			return true
		}
	}
	return false
}

func framedPrefix(b []byte) bool { return len(b) >= 5 && b[0] == 0 }
