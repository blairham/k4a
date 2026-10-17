// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/jsonpath"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// wideDoc has more leaves than maxFieldTerms, so it is indexed as partial.
func wideDoc(status string) string {
	var b strings.Builder
	b.WriteString(`{"status":"` + status + `","items":[`)
	for i := range maxFieldTerms + 10 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", i)
	}
	b.WriteString("]}")
	return b.String()
}

func TestQuery_FieldEquality(t *testing.T) {
	t.Parallel()
	ix := newTestIndexer(t)
	now := time.Now()
	msgs := []kafka.ConsumedMessage{
		sampleMsg(0, 1, `{"customer":7}`, `{"order":{"id":42,"status":"failed"}}`, now),
		sampleMsg(0, 2, "k", `{"order":{"id":42.0,"status":"ok"}}`, now),
		sampleMsg(0, 3, "k", `{"order":{"id":"42"}}`, now),
		sampleMsg(0, 4, "k", wideDoc("failed"), now),
		sampleMsg(0, 5, "k", wideDoc("ok"), now),
		sampleMsg(0, 6, "k", `order.id 42 failed`, now),
	}
	if err := ix.IndexBatch(context.Background(), msgs); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string][]int64{
		`.order.id == 42`:           {1, 2},
		`order.id:42`:               {1, 2},
		`.order.id == "42"`:         {3},
		`status:failed`:             {4},
		`.order.status == "failed"`: {1},
		`customer:7`:                {1},
		`.items[3] == 3`:            {4, 5},
		`.order.id == 43`:           {},
	} {
		e, err := jsonpath.Parse(path)
		if err != nil {
			t.Fatal(err)
		}
		got := queryOffsets(t, ix, QueryParams{JSONPath: e.String()})
		if !slices.Equal(got, want) && (len(got) != 0 || len(want) != 0) {
			t.Errorf("%s: matched %v, want %v", path, got, want)
		}
	}
	// Scoped to the value, the key's field does not count.
	if got := queryOffsets(t, ix, QueryParams{JSONPath: ".customer == 7", Scopes: []string{"value"}}); len(got) != 0 {
		t.Errorf("value-scoped path matched the key: %v", got)
	}
}

// TestFieldQueryMatchesScan is Tier 5's parity contract: the same records and
// the same equality paths through a broker scan and through the index select
// the same offsets, decoded records included.
func TestFieldQueryMatchesScan(t *testing.T) {
	t.Parallel()
	const topic = "fields.parity.v1"
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
	for _, v := range [][]byte{
		[]byte(`{"fruit":"orange","n":42}`), framedValue("orange"), []byte(`{"n":4.2e1,"fruit":"pear"}`),
		[]byte(wideDoc("failed")), []byte(`{"n":"42"}`), []byte(`not json`), framedValue("lemon"),
	} {
		if perr := cl.ProduceSync(context.Background(), &kgo.Record{Key: []byte(`{"id":1}`), Value: v}).
			FirstErr(); perr != nil {
			t.Fatal(perr)
		}
	}
	cl.Close()

	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	dec := &fakeDecoder{id: "sr:a"}
	ix := openDecoding(t, state.NewRoot(t.TempDir()), dec)
	indexAll(t, client, ix, topic)

	for _, src := range []string{
		`fruit:orange`, `.n == 42`, `.n == "42"`, `status:failed`, `.items[0] == 0`, `id:1`, `.fruit == "lemon"`,
	} {
		e, err := jsonpath.Parse(src)
		if err != nil {
			t.Fatal(err)
		}
		scanned := scanOffsets(t, client, topic, kafka.SearchParams{Path: e, Decoder: dec})
		indexed := queryOffsets(t, ix, QueryParams{JSONPath: e.String(), Decoder: dec.Identity()})
		if !slices.Equal(scanned, indexed) {
			t.Errorf("%s: scan matched %v, index matched %v", src, scanned, indexed)
		}
		if len(scanned) == 0 {
			t.Errorf("%s: matched nothing on either side; the case tests nothing", src)
		}
	}
}
