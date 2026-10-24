// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"regexp"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/jsonpath"
)

// wordDecoder decodes a framed value (zero magic byte, four ID bytes, then a
// word) to {"word":"<word>"}, and reports unsettled when told to.
type wordDecoder struct {
	calls     int
	unsettled bool
}

func (d *wordDecoder) Identity() string { return "test:word" }

func (d *wordDecoder) DecodeRecord(_ context.Context, m *ConsumedMessage) bool {
	d.calls++
	if m.RawValue == nil {
		return true
	}
	if d.unsettled {
		m.ValueDecoding = Decoding{Reason: "schema registry unreachable"}
		return false
	}
	m.ValueDecoding = Decoding{Raw: m.Value, Schema: "words-value v1 (id 1, AVRO)"}
	m.Value = `{"word":"` + string(m.RawValue[5:]) + `"}`
	return true
}

func framedRecord(word string) *kgo.Record {
	return &kgo.Record{Key: []byte("k"), Value: append([]byte{0, 0, 0, 0, 1}, word...)}
}

func decodingScan(pattern string, dec RecordDecoder) *deepScan {
	re := regexp.MustCompile(pattern)
	return &deepScan{re: re, prefilter: extractPrefilter(re), scope: ScopeKey | ScopeValue, decoder: dec}
}

func TestScanMatchesDecodedText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dec := &wordDecoder{}

	m, ok := decodingScan(`"word":"apple"`, dec).match(ctx, framedRecord("apple"))
	if !ok {
		t.Fatal("a pattern in the decoded text did not match")
	}
	if m.Value != `{"word":"apple"}` || !m.ValueDecoding.Decoded() {
		t.Errorf("emitted %q (%+v), want the decoded record", m.Value, m.ValueDecoding)
	}
	if _, ok := decodingScan(`\x00\x00`, dec).match(ctx, framedRecord("apple")); ok {
		t.Error("a pattern in the framed bytes matched a record that decoded")
	}
	// Prefilter: the literal prefix is checked against the decoded text too.
	if _, ok := decodingScan(`word.*apple`, dec).match(ctx, framedRecord("apple")); !ok {
		t.Error("a pattern whose literal prefix is only in the decoded text did not match")
	}
}

func TestScanDecodesOnlyFramedRecords(t *testing.T) {
	t.Parallel()
	dec := &wordDecoder{}
	ds := decodingScan("plain", dec)
	if _, ok := ds.match(context.Background(), &kgo.Record{Value: []byte(`{"plain":true}`)}); !ok {
		t.Error("an unframed record stopped matching its bytes")
	}
	if dec.calls != 0 {
		t.Errorf("decoder called %d times for an unframed record, want 0", dec.calls)
	}
	hdr := &kgo.Record{
		Value:   []byte("plain"),
		Headers: []kgo.RecordHeader{{Key: ValueSchemaIDHeader, Value: []byte{0, 0, 0, 0, 1}}},
	}
	ds.match(context.Background(), hdr)
	if dec.calls != 1 {
		t.Errorf("decoder called %d times for a record with a schema-ID header, want 1", dec.calls)
	}
}

func TestScanUnsettledDecodeIsTruncated(t *testing.T) {
	t.Parallel()
	ds := decodingScan("^hex:00", &wordDecoder{unsettled: true})
	// Undecoded, the value is matched as its rendering (framed bytes start
	// with NUL, so hex:), the text Messages shows and an index would store.
	if _, ok := ds.match(context.Background(), framedRecord("plum")); !ok {
		t.Error("an undecoded framed record stopped matching its rendering")
	}
	if _, ok := decodingScan("plum", &wordDecoder{unsettled: true}).match(context.Background(), framedRecord("plum")); ok {
		t.Error("an undecoded framed record matched its bytes, which the display hides")
	}
	if !ds.truncated.Load() {
		t.Error("a record the decoder could not settle left the scan looking complete")
	}

	settled := decodingScan("apple", &wordDecoder{})
	settled.match(context.Background(), framedRecord("apple"))
	if settled.truncated.Load() {
		t.Error("a settled decode marked the scan truncated")
	}
}

func TestScanPathReadsDecodedText(t *testing.T) {
	t.Parallel()
	expr, err := jsonpath.Parse(`.word == "apple"`)
	if err != nil {
		t.Fatal(err)
	}
	ds := &deepScan{path: expr, scope: ScopeValue, decoder: &wordDecoder{}}
	if _, ok := ds.match(context.Background(), framedRecord("apple")); !ok {
		t.Error("a JSON path on the decoded value did not match")
	}
	ds.decoder = nil
	if _, ok := ds.match(context.Background(), framedRecord("apple")); ok {
		t.Error("a JSON path matched framed bytes with no decoder")
	}
}
