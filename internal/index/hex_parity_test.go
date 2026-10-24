// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/jsonpath"
	"github.com/blairham/k4a/internal/kafka"
)

// TestHexRenderedPayloadMatchesScan is #144's contract: a key or value the
// display renders as hex: (a control byte, or invalid UTF-8) is matched by
// a scan exactly as the index matches its stored rendering, for a regex and
// for a JSON path alike.
func TestHexRenderedPayloadMatchesScan(t *testing.T) {
	t.Parallel()
	const topic = "hex.parity.v1"
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)
	seeds := strings.Join(fake.ListenAddrs(), ",")
	cl, err := kgo.NewClient(kgo.SeedBrokers(fake.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []*kgo.Record{
		// JSON with a raw DEL inside a string: Go's decoder accepts it, the display does not.
		{Key: []byte("k"), Value: []byte("{\"id\":\"a\x7fb\",\"tag\":\"x\"}")},
		{Key: []byte("k"), Value: []byte(`{"id":"ab","tag":"x"}`)},
		// Invalid UTF-8 before otherwise valid JSON.
		{Key: []byte("k"), Value: []byte("\xff\xfe{\"id\":\"ab\",\"tag\":\"y\"}")},
		{Key: []byte("k"), Value: []byte(`hex:7b22 literal`)},
		// A control byte in the key and in a header, with a plain value.
		{Key: []byte("k\x00"), Value: []byte(`{"tag":"z"}`), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte{1}}}},
	} {
		if perr := cl.ProduceSync(context.Background(), r).FirstErr(); perr != nil {
			t.Fatal(perr)
		}
	}
	cl.Close()

	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ix := newTestIndexer(t)
	indexAll(t, client, ix, topic)

	for _, tc := range []struct {
		pat    string
		scopes []string
	}{
		{pat: `hex:`},
		{pat: `^hex:7b`},
		{pat: `"tag":"x"`},
		{pat: `tag`},
		{pat: `7f62`},
		{pat: `hex:`, scopes: []string{"key"}},
		{pat: `h=hex:01`, scopes: []string{"headers"}},
	} {
		re := regexp.MustCompile(tc.pat)
		scanned := scanOffsets(t, client, topic, kafka.SearchParams{Pattern: re, Scope: scopeMask(tc.scopes)})
		indexed := queryOffsets(t, ix, QueryParams{Pattern: re, Scopes: tc.scopes})
		if !slices.Equal(scanned, indexed) {
			t.Errorf("%q %v: scan matched %v, index matched %v", tc.pat, tc.scopes, scanned, indexed)
		}
		if len(scanned) == 0 {
			t.Errorf("%q %v: matched nothing on either side; the case tests nothing", tc.pat, tc.scopes)
		}
	}
	for _, src := range []string{`.tag == "x"`, `tag:z`, `.id == "ab"`} {
		e, err := jsonpath.Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		scanned := scanOffsets(t, client, topic, kafka.SearchParams{Path: e})
		indexed := queryOffsets(t, ix, QueryParams{JSONPath: e.String()})
		if !slices.Equal(scanned, indexed) {
			t.Errorf("%s: scan matched %v, index matched %v", src, scanned, indexed)
		}
		if len(scanned) == 0 {
			t.Errorf("%s: matched nothing on either side; the case tests nothing", src)
		}
	}
	// Falsifiability: these match the bytes of the hex-rendered records and
	// nothing else (a regexp reads an invalid byte as U+FFFD), so a scan that
	// still read bytes would return them.
	for _, pat := range []string{"a\x7fb", `\x{FFFD}`, "h=\x01"} {
		scope := kafka.ScopeKey | kafka.ScopeValue | kafka.ScopeHeaders
		params := kafka.SearchParams{Pattern: regexp.MustCompile(pat), Scope: scope}
		if raw := scanOffsets(t, client, topic, params); len(raw) != 0 {
			t.Errorf("%q: a scan matched bytes the display hides, at %v", pat, raw)
		}
	}
}
