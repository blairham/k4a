// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blairham/k4a/internal/kafka"
)

// guidHeader builds a __value_schema_id header value the way Confluent's
// HeaderSchemaIdSerializer does: magic byte 1, the GUID's 16 bytes, then for
// Protobuf the message-index path.
func guidHeader(t *testing.T, guid string, indexBytes []byte) []byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.ReplaceAll(guid, "-", ""))
	if err != nil || len(raw) != 16 {
		t.Fatalf("bad GUID %q", guid)
	}
	return append(append([]byte{magicByteGUID}, raw...), indexBytes...)
}

// indexPath encodes a message-index path with franz-go's serializer, an
// encoder independent of ours: its header minus the 5-byte ID prefix.
func indexPath(t *testing.T, index []int) []byte {
	t.Helper()
	return frame(t, 1, index, nil)[headerSize:]
}

// guidOf asks the fake registry for the GUID it assigned a subject version.
func guidOf(t *testing.T, reg *srfake.Registry, subject string, version int) string {
	t.Helper()
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ss, err := cl.SchemaByVersion(context.Background(), subject, version)
	if err != nil || ss.GUID == "" {
		t.Fatalf("no GUID for %s v%d: %v", subject, version, err)
	}
	return ss.GUID
}

const trickyAvro = `{"type":"record","name":"Tricky","fields":[{"name":"a","type":"long"},{"name":"s","type":"string"}]}`

const pingAvro = `{"type":"record","name":"Ping","fields":[{"name":"n","type":"long"}]}`

func pingBody(t *testing.T, n int64) []byte {
	t.Helper()
	codec, err := goavro.NewCodec(pingAvro)
	if err != nil {
		t.Fatal(err)
	}
	b, err := codec.BinaryFromNative(nil, map[string]any{"n": n})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestDecodeSchemaIDHeader covers Confluent Platform 8's header-carried
// schema IDs: a GUID or a numeric ID in the header with an unprefixed
// payload, a header that wins over a prefix, and the fallbacks.
func TestDecodeSchemaIDHeader(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("pings-value", 2, 40, sr.Schema{Schema: pingAvro})
	reg.SeedSchema("tricky-value", 1, 42, sr.Schema{Schema: trickyAvro})
	guid := guidOf(t, reg, "pings-value", 2)
	r := newRegistry(t, reg, Config{})
	ctx := context.Background()
	body := pingBody(t, 7)

	d, err := r.DecodeWithHeader(ctx, "pings", false, guidHeader(t, guid, nil), body)
	if err != nil {
		t.Fatalf("GUID header: %v", err)
	}
	if d.JSON != `{"n":7}` || d.GUID != guid || d.ID != 40 {
		t.Errorf("GUID header: JSON %s GUID %q ID %d", d.JSON, d.GUID, d.ID)
	}
	if want := "pings-value v2 (guid " + guid + ", AVRO)"; d.Ref() != want {
		t.Errorf("Ref = %q, want %q", d.Ref(), want)
	}

	// A V0 header: magic 0 and the numeric ID, payload still unprefixed.
	if d, err = r.DecodeWithHeader(ctx, "pings", false, frame(t, 40, nil, nil), body); err != nil || d.JSON != `{"n":7}` {
		t.Errorf("ID header: %s, %v", d.JSON, err)
	}

	// The header wins over a payload that merely looks framed: this Avro
	// body starts 00 10 00 00 00, which as a prefix would name ID 2^28.
	codec, err := goavro.NewCodec(trickyAvro)
	if err != nil {
		t.Fatal(err)
	}
	tricky, err := codec.BinaryFromNative(nil, map[string]any{"a": int64(0), "s": "\x00\x00\x00ok!!!"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := ParseHeader(tricky); !ok {
		t.Fatalf("premise: payload %x does not look framed", tricky)
	}
	if d, err = r.DecodeWithHeader(ctx, "tricky", false, frame(t, 42, nil, nil), tricky); err != nil || d.ID != 42 {
		t.Errorf("header did not win over a framed-looking payload: %+v, %v", d, err)
	}

	// An unparseable header falls back to the payload prefix...
	if d, err = r.DecodeWithHeader(ctx, "pings", false, []byte{9, 9}, frame(t, 40, nil, body)); err != nil ||
		d.JSON != `{"n":7}` {
		t.Errorf("fallback to the prefix: %s, %v", d.JSON, err)
	}
	// ...and is reported when there is no prefix either, rather than shown as
	// plain bytes.
	if _, err = r.DecodeWithHeader(ctx, "pings", false, []byte{9, 9}, body); err == nil ||
		!strings.Contains(err.Error(), "header") {
		t.Errorf("bad header and unframed payload: err %v", err)
	}
	// An unknown GUID is the registry's "not found", cached like an ID.
	if _, err = r.DecodeWithHeader(ctx, "pings", false,
		guidHeader(t, "00000000-0000-8000-8000-000000000000", nil), body); err == nil {
		t.Error("an unknown GUID decoded")
	}

	// End to end through a consumed record.
	m := kafka.ConsumedMessage{Topic: "pings", Value: "x", RawValue: body, ValueSchemaID: guidHeader(t, guid, nil)}
	r.DecodeMessage(ctx, &m)
	if m.Value != `{"n":7}` || !strings.Contains(m.ValueDecoding.Schema, guid) {
		t.Errorf("DecodeMessage: value %q, decoding %+v", m.Value, m.ValueDecoding)
	}
}

// TestDecodeProtobufIndexesInHeader: with the ID in a header, the
// message-index path travels in the header too and the payload is the bare
// message.
func TestDecodeProtobufIndexesInHeader(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("common/money.proto", 1, 20, sr.Schema{Schema: commonProto, Type: sr.TypeProtobuf})
	reg.SeedSchema("shop-orders-value", 1, 21, sr.Schema{
		Schema:     orderProto,
		Type:       sr.TypeProtobuf,
		References: []sr.SchemaReference{{Name: "common/money.proto", Subject: "common/money.proto", Version: 1}},
	})
	guid := guidOf(t, reg, "shop-orders-value", 1)
	fd := compileForTest(t, map[string]string{"common/money.proto": commonProto, "order.proto": orderProto})
	r := newRegistry(t, reg, Config{})

	line := dynamicpb.NewMessage(fd.Messages().Get(1).Messages().Get(0))
	line.Set(line.Descriptor().Fields().ByName("sku"), protoreflect.ValueOfString("ABC"))
	header := dynamicpb.NewMessage(fd.Messages().Get(0))
	header.Set(header.Descriptor().Fields().ByName("trace"), protoreflect.ValueOfString("t1"))

	for _, tc := range []struct {
		msg   proto.Message
		want  string
		index []byte
	}{
		{msg: line, index: indexPath(t, []int{1, 0}), want: `{"sku":"ABC"}`},
		{msg: header, index: indexPath(t, []int{0}), want: `{"trace":"t1"}`},
		{msg: header, index: nil, want: `{"trace":"t1"}`}, // no path written: [0]
	} {
		body, err := proto.Marshal(tc.msg)
		if err != nil {
			t.Fatal(err)
		}
		d, err := r.DecodeWithHeader(context.Background(), "shop-orders", false, guidHeader(t, guid, tc.index), body)
		if err != nil || d.JSON != tc.want {
			t.Errorf("index %x: %s, %v; want %s", tc.index, d.JSON, err, tc.want)
		}
	}
}

const moneyProto = `syntax = "proto3";
package shop;
import "google/type/money.proto";
import "google/type/date.proto";
message Invoice { google.type.Money total = 1; google.type.Date due = 2; }`

// TestDecodeGoogleTypeImports: Confluent treats google/type/* as built in
// and never registers them, so a schema importing one must compile from the
// bundled copies, with their real field numbers. The body is hand-encoded so
// the check does not lean on those same copies.
func TestDecodeGoogleTypeImports(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("invoices-value", 1, 50, sr.Schema{Schema: moneyProto, Type: sr.TypeProtobuf})
	r := newRegistry(t, reg, Config{})

	var money []byte // google.type.Money: currency_code=1, units=2, nanos=3
	money = protowire.AppendTag(money, 1, protowire.BytesType)
	money = protowire.AppendString(money, "EUR")
	money = protowire.AppendTag(money, 2, protowire.VarintType)
	money = protowire.AppendVarint(money, 12)
	money = protowire.AppendTag(money, 3, protowire.VarintType)
	money = protowire.AppendVarint(money, 500_000_000)
	var date []byte // google.type.Date: year=1, month=2, day=3
	for i, v := range []uint64{2026, 10, 15} {
		date = protowire.AppendTag(date, protowire.Number(i+1), protowire.VarintType)
		date = protowire.AppendVarint(date, v)
	}
	var body []byte
	body = protowire.AppendTag(body, 1, protowire.BytesType)
	body = protowire.AppendBytes(body, money)
	body = protowire.AppendTag(body, 2, protowire.BytesType)
	body = protowire.AppendBytes(body, date)

	d, err := r.Decode(context.Background(), "invoices", false, frame(t, 50, []int{0}, body))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"total":{"currencyCode":"EUR","units":"12","nanos":500000000},"due":{"year":2026,"month":10,"day":15}}`
	if d.JSON != want {
		t.Errorf("JSON = %s, want %s", d.JSON, want)
	}
}

const anyProto = `syntax = "proto3";
package shop;
import "google/protobuf/any.proto";
message Envelope { google.protobuf.Any payload = 1; repeated google.protobuf.Any more = 2; }
message Inner { string name = 1; google.protobuf.Any nested = 2; }`

// TestDecodeProtobufAny: an Any whose type the schema (or the well-known
// types) defines is expanded; one it cannot name, or whose bytes do not
// parse, is shown as its type URL and base64 value instead of failing the
// whole record.
func TestDecodeProtobufAny(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()
	reg.SeedSchema("envelopes-value", 1, 60, sr.Schema{Schema: anyProto, Type: sr.TypeProtobuf})
	fd := compileForTest(t, map[string]string{"order.proto": anyProto})
	r := newRegistry(t, reg, Config{})

	pack := func(url string, value []byte) *anypb.Any { return &anypb.Any{TypeUrl: url, Value: value} }
	mustMarshal := func(m proto.Message) []byte {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	asDynamic := func(a *anypb.Any) protoreflect.Value {
		dm := dynamicpb.NewMessage(fd.Messages().Get(0).Fields().ByName("payload").Message())
		if err := proto.Unmarshal(mustMarshal(a), dm); err != nil {
			t.Fatal(err)
		}
		return protoreflect.ValueOfMessage(dm)
	}

	inner := dynamicpb.NewMessage(fd.Messages().Get(1))
	inner.Set(inner.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString("x"))
	// Nested inside a resolvable Any, an unresolvable one.
	inner.Set(inner.Descriptor().Fields().ByName("nested"), asDynamic(pack("type.googleapis.com/acme.Gone", []byte{1, 2})))

	env := dynamicpb.NewMessage(fd.Messages().Get(0))
	env.Set(
		env.Descriptor().Fields().ByName("payload"),
		asDynamic(pack("type.googleapis.com/shop.Inner", mustMarshal(inner))),
	)
	more := env.Mutable(env.Descriptor().Fields().ByName("more")).List()
	more.Append(asDynamic(pack("type.googleapis.com/google.protobuf.Timestamp", mustMarshal(timestamppb.New(time0)))))
	more.Append(asDynamic(pack("type.googleapis.com/acme.Unknown", []byte("\x0a\x01z"))))
	more.Append(asDynamic(pack("type.googleapis.com/shop.Inner", []byte{0xff}))) // resolvable, corrupt

	d, err := r.Decode(context.Background(), "envelopes", false, frame(t, 60, []int{0}, mustMarshal(env)))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"payload":{"@type":"type.googleapis.com/shop.Inner","name":"x",` +
		`"nested":{"@type":"type.googleapis.com/acme.Gone","value":"AQI="}},` +
		`"more":[{"@type":"type.googleapis.com/google.protobuf.Timestamp","value":"2026-10-15T00:00:00Z"},` +
		`{"@type":"type.googleapis.com/acme.Unknown","value":"CgF6"},` +
		`{"@type":"type.googleapis.com/shop.Inner","value":"/w=="}]}`
	if d.JSON != want {
		t.Errorf("JSON =\n%s\nwant\n%s", d.JSON, want)
	}
}

var time0 = mustTime("2026-10-15T00:00:00Z")

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

// TestBundledGoogleTypesCompile imports every google/type file Confluent
// treats as built in, so a stale or missing bundled copy fails here rather
// than on someone's topic.
func TestBundledGoogleTypesCompile(t *testing.T) {
	names := []string{
		"calendar_period", "color", "date", "datetime", "dayofweek", "decimal", "expr", "fraction",
		"interval", "latlng", "money", "month", "phone_number", "postal_address", "quaternion", "timeofday",
	}
	src := "syntax = \"proto3\";\npackage t;\n"
	for _, n := range names {
		src += "import \"google/type/" + n + ".proto\";\n"
	}
	src += "message M { google.type.PostalAddress a = 1; google.type.DateTime d = 2; }\n"
	if fd := compileForTest(t, map[string]string{"order.proto": src}); fd.Imports().Len() != len(names) {
		t.Errorf("compiled with %d imports, want %d", fd.Imports().Len(), len(names))
	}
}
