// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// frame builds a payload with franz-go's own serializer header, so the tests
// check ParseHeader against an independent encoder rather than against itself.
func frame(t *testing.T, id int, index []int, body []byte) []byte {
	t.Helper()
	var h sr.ConfluentHeader
	b, err := h.AppendEncode(nil, id, index)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, body...)
}

func newRegistry(t *testing.T, reg *srfake.Registry, cfg Config) *Registry {
	t.Helper()
	cfg.URL = reg.URL()
	r, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

const (
	addressAvro = `{"type":"record","name":"Address","namespace":"shop","fields":[{"name":"city","type":"string"}]}`
	orderAvro   = `{"type":"record","name":"Order","namespace":"shop","fields":[
		{"name":"id","type":"long"},
		{"name":"note","type":["null","string"],"default":null},
		{"name":"ship_to","type":"shop.Address"},
		{"name":"total","type":{"type":"bytes","logicalType":"decimal","precision":9,"scale":2}}]}`
)

func TestDecodeAvroWithReference(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("shop.Address", 1, 10, sr.Schema{Schema: addressAvro})
	reg.SeedSchema("orders-value", 3, 11, sr.Schema{
		Schema:     orderAvro,
		References: []sr.SchemaReference{{Name: "shop.Address", Subject: "shop.Address", Version: 1}},
	})

	// Encode with the references inlined, independently of the decoder's
	// union compilation.
	inlined := strings.Replace(orderAvro, `"shop.Address"`, addressAvro, 1)
	codec, err := goavro.NewCodec(inlined)
	if err != nil {
		t.Fatal(err)
	}
	body, err := codec.BinaryFromNative(nil, map[string]any{
		"id":      int64(42),
		"note":    goavro.Union("string", "gift"),
		"ship_to": map[string]any{"city": "Oslo"},
		"total":   mustRat(t, "12.50"),
	})
	if err != nil {
		t.Fatal(err)
	}

	r := newRegistry(t, reg, Config{})
	d, err := r.Decode(context.Background(), "orders", false, frame(t, 11, nil, body))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":42,"note":{"string":"gift"},"ship_to":{"city":"Oslo"},"total":"12.5"}`
	if d.JSON != want {
		t.Errorf("JSON = %s, want %s", d.JSON, want)
	}
	if d.Ref() != "orders-value v3 (id 11, AVRO)" {
		t.Errorf("Ref = %q", d.Ref())
	}
}

const (
	commonProto = `syntax = "proto3";
package common;
message Money { string currency = 1; int64 units = 2; }`
	orderProto = `syntax = "proto3";
package shop;
import "common/money.proto";
import "google/protobuf/timestamp.proto";
import "confluent/meta.proto";
message Header { string trace = 1; }
message Order {
  option (confluent.message_meta).doc = "an order";
  message Line { string sku = 1; common.Money price = 2; }
  string id = 1;
  repeated Line lines = 2;
  google.protobuf.Timestamp placed = 3;
}`
)

func TestDecodeProtobufMessageIndexes(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("common/money.proto", 1, 20, sr.Schema{Schema: commonProto, Type: sr.TypeProtobuf})
	reg.SeedSchema("shop-orders-value", 1, 21, sr.Schema{
		Schema:     orderProto,
		Type:       sr.TypeProtobuf,
		References: []sr.SchemaReference{{Name: "common/money.proto", Subject: "common/money.proto", Version: 1}},
	})
	fd := compileForTest(t, map[string]string{"common/money.proto": commonProto, "order.proto": orderProto})
	r := newRegistry(t, reg, Config{})

	// [1] is Order; [1, 0] is Order.Line; [0] (the 0x00 shorthand) is Header.
	order := dynamicpb.NewMessage(fd.Messages().Get(1))
	order.Set(order.Descriptor().Fields().ByName("id"), protoreflect.ValueOfString("o-1"))
	line := dynamicpb.NewMessage(fd.Messages().Get(1).Messages().Get(0))
	line.Set(line.Descriptor().Fields().ByName("sku"), protoreflect.ValueOfString("ABC"))
	header := dynamicpb.NewMessage(fd.Messages().Get(0))
	header.Set(header.Descriptor().Fields().ByName("trace"), protoreflect.ValueOfString("t1"))

	for _, tc := range []struct {
		msg   proto.Message
		want  string
		index []int
	}{
		{msg: order, index: []int{1}, want: `{"id":"o-1"}`},
		{msg: line, index: []int{1, 0}, want: `{"sku":"ABC"}`},
		{msg: header, index: []int{0}, want: `{"trace":"t1"}`},
	} {
		body, err := proto.Marshal(tc.msg)
		if err != nil {
			t.Fatal(err)
		}
		d, err := r.Decode(context.Background(), "shop-orders", false, frame(t, 21, tc.index, body))
		if err != nil {
			t.Fatalf("index %v: %v", tc.index, err)
		}
		if d.JSON != tc.want {
			t.Errorf("index %v: JSON = %s, want %s", tc.index, d.JSON, tc.want)
		}
		if d.Format != "PROTOBUF" {
			t.Errorf("Format = %q", d.Format)
		}
	}

	if _, err := r.Decode(context.Background(), "shop-orders", false, frame(t, 21, []int{5}, nil)); err == nil {
		t.Error("an index past the last message decoded")
	}
}

func compileForTest(t *testing.T, srcs map[string]string) protoreflect.FileDescriptor {
	t.Helper()
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{Accessor: protoAccessor(srcs)}),
	}
	files, err := c.Compile(context.Background(), "order.proto")
	if err != nil {
		t.Fatal(err)
	}
	return files[0]
}

func TestDecodeJSONSchema(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("events-key", 2, 30, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	r := newRegistry(t, reg, Config{})

	d, err := r.Decode(context.Background(), "events", true, frame(t, 30, nil, []byte(`{ "a" : 1 }`)))
	if err != nil {
		t.Fatal(err)
	}
	if d.JSON != `{"a":1}` || d.Ref() != "events-key v2 (id 30, JSON)" {
		t.Errorf("got %s / %s", d.JSON, d.Ref())
	}
	if _, err := r.Decode(context.Background(), "events", true, frame(t, 30, nil, []byte("not json"))); err == nil {
		t.Error("a JSON Schema payload that is not JSON decoded")
	}
}

func TestDecodeNotFramed(t *testing.T) {
	r, err := New(Config{URL: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range [][]byte{nil, []byte(`{"a":1}`), {0, 0, 0, 0}, {0, 0, 0, 0, 0}, {0, 0xff, 0xff, 0xff, 0xff}} {
		if _, err := r.Decode(context.Background(), "t", false, p); !errors.Is(err, ErrNotFramed) {
			t.Errorf("%x: err = %v, want ErrNotFramed", p, err)
		}
	}
}

func TestUnknownSchemaIsCached(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	var calls atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, req *http.Request) bool {
		if strings.HasPrefix(req.URL.Path, "/schemas/ids/") {
			calls.Add(1)
		}
		return false
	})
	r := newRegistry(t, reg, Config{})
	for range 5 {
		_, err := r.Decode(context.Background(), "t", false, frame(t, 99, nil, []byte("x")))
		if !errors.Is(err, errSchemaNotFound) {
			t.Fatalf("err = %v, want not found", err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("registry asked %d times for a missing schema, want 1", n)
	}
}

func TestTransientErrorExpires(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("k-value", 1, 7, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	var down atomic.Bool
	down.Store(true)
	reg.Intercept(func(w http.ResponseWriter, _ *http.Request) bool {
		if down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return true
		}
		return false
	})
	r := newRegistry(t, reg, Config{})
	clock := time.Now()
	r.now = func() time.Time { return clock }

	payload := frame(t, 7, nil, []byte(`{}`))
	if _, err := r.Decode(context.Background(), "k", false, payload); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want a 503", err)
	}
	down.Store(false)
	if _, err := r.Decode(context.Background(), "k", false, payload); err == nil {
		t.Fatal("a failure was retried before errorTTL")
	}
	clock = clock.Add(errorTTL + time.Second)
	if _, err := r.Decode(context.Background(), "k", false, payload); err != nil {
		t.Fatalf("after errorTTL: %v", err)
	}
}

func TestBasicAuth(t *testing.T) {
	reg := srfake.New(srfake.WithAuth("Basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))))
	defer reg.Close()
	reg.SeedSchema("a-value", 1, 1, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	payload := frame(t, 1, nil, []byte(`{}`))

	if _, err := newRegistry(t, reg, Config{Username: "u", Password: "p"}).
		Decode(context.Background(), "a", false, payload); err != nil {
		t.Errorf("with credentials: %v", err)
	}
	_, err := newRegistry(t, reg, Config{Username: "u", Password: "wrong"}).
		Decode(context.Background(), "a", false, payload)
	if err == nil || !strings.Contains(err.Error(), "403") {
		t.Errorf("wrong password: err = %v, want a 403", err)
	}
	if strings.Contains(err.Error(), "wrong") {
		t.Errorf("error leaks the password: %v", err)
	}
}

func TestBearerToken(t *testing.T) {
	reg := srfake.New(srfake.WithAuth("Bearer tok"))
	defer reg.Close()
	reg.SeedSchema("a-value", 1, 1, sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON})
	if _, err := newRegistry(t, reg, Config{Token: "tok"}).
		Decode(context.Background(), "a", false, frame(t, 1, nil, []byte(`{}`))); err != nil {
		t.Error(err)
	}
}

func TestConfigChecks(t *testing.T) {
	for _, tc := range []struct {
		cfg     Config
		wantErr string
	}{
		{Config{URL: "https://sr.example.com"}, ""},
		{Config{URL: "http://sr.example.com"}, ""},
		{Config{URL: "http://localhost:8081", Username: "u", Password: "p"}, ""},
		{Config{URL: "http://sr.example.com", Token: "t"}, "must use https"},
		{Config{URL: "https://u:p@sr.example.com"}, "must not embed credentials"},
		{Config{URL: "sr.example.com:8081"}, "absolute"},
		{Config{URL: "ftp://sr.example.com"}, "http or https"},
		{Config{URL: "https://sr.example.com", Token: "t", Username: "u"}, "not both"},
		{Config{URL: "https://sr.example.com", CAFile: "/nonexistent/ca.pem"}, "CA file"},
	} {
		_, err := New(tc.cfg)
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%+v: unexpected error %v", tc.cfg.URL, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%+v: err = %v, want %q", tc.cfg.URL, err, tc.wantErr)
		}
	}
}

func TestCorruptAvroBody(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("x-value", 1, 5, sr.Schema{Schema: addressAvro})
	r := newRegistry(t, reg, Config{})
	// A string length of 2^31 with nothing behind it.
	_, err := r.Decode(context.Background(), "x", false, frame(t, 5, nil, []byte{0xfe, 0xff, 0xff, 0xff, 0x0f}))
	if err == nil {
		t.Fatal("a truncated Avro body decoded")
	}
}

func TestAvroWithoutReferences(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("plain-value", 1, 6, sr.Schema{Schema: `{"type":"array","items":"null"}`})
	r := newRegistry(t, reg, Config{})
	d, err := r.Decode(context.Background(), "plain", false, frame(t, 6, nil, []byte{0x04, 0x00}))
	if err != nil {
		t.Fatal(err)
	}
	if d.JSON != `[null,null]` {
		t.Errorf("JSON = %s", d.JSON)
	}
	// A block count of 2^31 items that encode to zero bytes each: goavro's
	// default ceiling would preallocate 32 GiB for it.
	_, err = r.Decode(context.Background(), "plain", false, frame(t, 6, nil, []byte{0x80, 0x80, 0x80, 0x80, 0x10}))
	if err == nil || !strings.Contains(err.Error(), "MaxBlockCount") {
		t.Errorf("err = %v, want the block-count ceiling", err)
	}
}

func TestJSONSafe(t *testing.T) {
	got := jsonSafe(map[string]any{"a": []any{float32(1), mustRat(t, "-0.05")}, "b": nanF()})
	m := got.(map[string]any) //nolint:forcetypeassert // the test built a map
	if m["b"] != "NaN" {
		t.Errorf("NaN = %v", m["b"])
	}
	if s := m["a"].([]any)[1]; s != "-0.05" { //nolint:forcetypeassert // the test built a slice
		t.Errorf("decimal = %v", s)
	}
}

func nanF() float64 {
	var zero float64
	return zero / zero
}
