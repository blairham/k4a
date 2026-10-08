// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"os"
	"testing"
)

// FuzzParseTopicSchema feeds arbitrary bytes to the schema parser behind
// `k4a produce --schema`. A schema it accepts must then produce: every record
// is a JSON object carrying the sample's fields plus seq, and carries a key
// when the schema names one. A schema that parses but cannot produce is a
// validation gap -- it fails, or crashes, only after the user has started a
// run.
func FuzzParseTopicSchema(f *testing.F) {
	if example, err := os.ReadFile("../../schemas/example.orders.v1.yaml"); err == nil {
		f.Add(example)
	}
	for _, s := range []string{
		"topic: t\nsamples:\n  - {a: 1}\n",
		"topic: t\nkey: id\nsamples:\n  - {id: 7, n: {deep: [1, 2]}}\n  - {id: x}\n",
		"topic: t\nkey: id\nsamples:\n  - {n: 1}\n",
		"topic: t\nsamples: []\n",
		"samples: [{a: b}, {c: d}]\n",
		"{",
		"",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		schema, err := ParseTopicSchema(data)
		if err != nil {
			return
		}
		n := 2 * len(schema.Samples)
		recs, err := GenerateSchemaMessages(schema, n)
		if err != nil {
			t.Fatalf("schema accepted but cannot produce: %v\n%s", err, data)
		}
		if len(recs) != n {
			t.Fatalf("got %d records, want %d", len(recs), n)
		}
		for i, r := range recs {
			var obj map[string]any
			if err := json.Unmarshal(r.Value, &obj); err != nil {
				t.Fatalf("record %d is not a JSON object: %v: %s", i, err, r.Value)
			}
			if _, ok := obj["seq"]; !ok {
				t.Fatalf("record %d has no seq: %s", i, r.Value)
			}
			if schema.Key != "" && r.Key == nil {
				t.Fatalf("record %d has no key although the schema names %q", i, schema.Key)
			}
		}
	})
}
