// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"bytes"
	"context"
	"embed"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/big"
	"os"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func init() {
	// goavro preallocates an array block from its declared count; its
	// default ceiling (2^31 items) lets one hostile record allocate 32 GiB.
	// A million items in one block is beyond any record a broker will carry
	// that a person would read in a terminal.
	goavro.MaxBlockCount = maxAvroBlockCount
	goavro.MaxBlockSize = maxAvroBlockSize
}

const (
	maxAvroBlockCount = 1 << 20
	maxAvroBlockSize  = 1 << 27
)

// avroSchema decodes one registry Avro schema. A schema with references is
// compiled as the union [ref..., main], the one way goavro lets a schema use
// named types defined elsewhere; a payload is then decoded as that union by
// prefixing the main branch's index, and the branch's value unwrapped.
type avroSchema struct {
	codec  *goavro.Codec
	branch int
}

func compileAvro(ctx context.Context, r *Registry, s sr.Schema) (*avroSchema, error) {
	var members []string
	err := walkReferences(ctx, r, s, map[string]bool{}, 0, func(_ string, rs sr.Schema) error {
		if rs.Type != sr.TypeAvro {
			return fmt.Errorf("an Avro schema cannot reference a %s schema", rs.Type)
		}
		members = append(members, rs.Schema)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		codec, perr := goavro.NewCodec(s.Schema)
		if perr != nil {
			return nil, fmt.Errorf("parsing Avro schema: %w", perr)
		}
		return &avroSchema{codec: codec, branch: -1}, nil
	}
	members = append(members, s.Schema)
	codec, err := goavro.NewCodec("[" + strings.Join(members, ",") + "]")
	if err != nil {
		return nil, fmt.Errorf("parsing Avro schema with its references: %w", err)
	}
	return &avroSchema{codec: codec, branch: len(members) - 1}, nil
}

func decodeAvro(s *avroSchema, body []byte) (string, error) {
	in := body
	if s.branch >= 0 {
		in = binary.AppendVarint(make([]byte, 0, len(body)+binary.MaxVarintLen64), int64(s.branch))
		in = append(in, body...)
	}
	v, _, err := s.codec.NativeFromBinary(in)
	if err != nil {
		return "", fmt.Errorf("decoding Avro: %w", err)
	}
	if s.branch >= 0 {
		v = unwrapBranch(v)
	}
	out, err := json.Marshal(jsonSafe(v))
	if err != nil {
		return "", fmt.Errorf("rendering Avro as JSON: %w", err)
	}
	return string(out), nil
}

// unwrapBranch takes the value out of goavro's {"type name": value} union
// form.
func unwrapBranch(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	for _, inner := range m {
		return inner
	}
	return v
}

// jsonSafe rewrites the Go values an Avro decode can produce that
// encoding/json cannot render, or renders unhelpfully: a non-finite float
// (an error) and a decimal *big.Rat (which would print as "num/den").
func jsonSafe(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = jsonSafe(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = jsonSafe(e)
		}
		return x
	case float64:
		return finite(x)
	case float32:
		return finite(float64(x))
	case *big.Rat:
		if x == nil {
			return nil
		}
		return ratString(x)
	default:
		return v
	}
}

func finite(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Sprint(f)
	}
	return f
}

// ratString prints a decimal exactly. big.Rat reduces 1250/100 to 25/2, so
// the scale is recovered as the fewest decimal places that make the
// denominator divide a power of ten; a denominator with another prime factor
// cannot come from an Avro decimal and prints as a fraction.
func ratString(r *big.Rat) string {
	ten := big.NewInt(10)
	p := big.NewInt(1)
	var rem big.Int
	for places := 0; places <= maxDecimalPlaces; places++ {
		if rem.Mod(p, r.Denom()).Sign() == 0 {
			return r.FloatString(places)
		}
		p.Mul(p, ten)
	}
	return r.RatString()
}

// maxDecimalPlaces is beyond the 38-digit precision of every Avro decimal a
// registry is likely to hold.
const maxDecimalPlaces = 64

//go:embed builtin
var builtinProtos embed.FS

// mainProtoName is the in-memory file name of the schema being compiled. A
// reference is named by its import path, which never looks like this.
const mainProtoName = "k4a-registry-schema.proto"

func compileProtobuf(ctx context.Context, r *Registry, s sr.Schema) (protoreflect.FileDescriptor, error) {
	srcs := map[string]string{mainProtoName: s.Schema}
	err := walkReferences(ctx, r, s, map[string]bool{}, 0, func(name string, rs sr.Schema) error {
		if rs.Type != sr.TypeProtobuf {
			return fmt.Errorf("a Protobuf schema cannot import a %s schema", rs.Type)
		}
		srcs[name] = rs.Schema
		return nil
	})
	if err != nil {
		return nil, err
	}
	c := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{Accessor: protoAccessor(srcs)}),
		// Compiling one schema is small; keep it to one goroutine so a burst
		// of new IDs does not fan out across every core.
		MaxParallelism: 1,
	}
	files, err := c.Compile(ctx, mainProtoName)
	if err != nil {
		return nil, fmt.Errorf("compiling Protobuf schema: %w", oneLine(err))
	}
	return files[0], nil
}

// protoAccessor serves the registry's sources first and Confluent's built-in
// imports (which its serializers never register) after them.
func protoAccessor(srcs map[string]string) func(string) (io.ReadCloser, error) {
	fromMap := protocompile.SourceAccessorFromMap(srcs)
	return func(path string) (io.ReadCloser, error) {
		rc, err := fromMap(path)
		if !errors.Is(err, os.ErrNotExist) {
			return rc, err
		}
		b, ferr := fs.ReadFile(builtinProtos, "builtin/"+path)
		if ferr != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(b)), nil
	}
}

func decodeProtobuf(fd protoreflect.FileDescriptor, body []byte) (string, error) {
	indexes, msg, err := ParseMessageIndexes(body)
	if err != nil {
		return "", err
	}
	md, err := messageAt(fd, indexes)
	if err != nil {
		return "", err
	}
	m := dynamicpb.NewMessage(md)
	if uerr := proto.Unmarshal(msg, m); uerr != nil {
		return "", fmt.Errorf("decoding Protobuf %s: %w", md.FullName(), uerr)
	}
	out, err := protojson.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("rendering Protobuf %s as JSON: %w", md.FullName(), err)
	}
	// protojson randomizes its whitespace on purpose; compact it so the same
	// message always renders the same way.
	return compact(out)
}

func compact(b []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// messageAt walks the message-index path: the first index picks a top-level
// message, each later one a message nested in the previous.
func messageAt(fd protoreflect.FileDescriptor, indexes []int) (protoreflect.MessageDescriptor, error) {
	msgs := fd.Messages()
	var md protoreflect.MessageDescriptor
	for depth, i := range indexes {
		if i >= msgs.Len() {
			return nil, fmt.Errorf("protobuf message index %v has no message at depth %d", indexes, depth)
		}
		md = msgs.Get(i)
		msgs = md.Messages()
	}
	if md == nil {
		return nil, errors.New("protobuf message index is empty")
	}
	return md, nil
}

// decodeJSON checks that a JSON Schema payload is JSON and compacts it. The
// schema is not applied: it constrains what a producer may write, and adds
// nothing to how the value reads.
func decodeJSON(body []byte) (string, error) {
	out, err := compact(body)
	if err != nil {
		return "", fmt.Errorf("JSON Schema payload is not JSON: %w", err)
	}
	return out, nil
}
