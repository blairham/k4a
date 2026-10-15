// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxReferenceDepth stops a reference cycle (which the registry should
// refuse, but a decoder must not trust) from recursing forever.
const maxReferenceDepth = 32

// Decoded is a payload rendered as JSON together with the schema that
// decoded it.
type Decoded struct {
	// JSON is the decoded payload as compact JSON.
	JSON string
	// Subject and Version are empty/zero when the registry did not say which
	// subject version the ID belongs to.
	Subject string
	Format  string
	ID      int
	Version int
}

// Ref is a one-line description of the schema, for a metadata card.
func (d Decoded) Ref() string {
	if d.Subject == "" {
		return fmt.Sprintf("id %d (%s)", d.ID, d.Format)
	}
	return fmt.Sprintf("%s v%d (id %d, %s)", d.Subject, d.Version, d.ID, d.Format)
}

// schema is one compiled registry schema.
type schema struct {
	avro     *avroSchema
	proto    protoreflect.FileDescriptor
	versions []sr.SubjectVersion
	typ      sr.SchemaType
	id       int
}

// Decode renders a Confluent-framed payload as JSON. topic and isKey pick the
// subject to report when the schema is registered under several (the
// default TopicNameStrategy subject, <topic>-key or <topic>-value, wins).
// A payload without the wire-format header returns ErrNotFramed.
func (r *Registry) Decode(ctx context.Context, topic string, isKey bool, payload []byte) (Decoded, error) {
	id, body, ok := ParseHeader(payload)
	if !ok {
		return Decoded{}, ErrNotFramed
	}
	sch, err := r.lookup(ctx, id)
	if err != nil {
		return Decoded{}, err
	}
	var out string
	switch sch.typ {
	case sr.TypeAvro:
		out, err = decodeAvro(sch.avro, body)
	case sr.TypeProtobuf:
		out, err = decodeProtobuf(sch.proto, body)
	case sr.TypeJSON:
		out, err = decodeJSON(body)
	default:
		err = fmt.Errorf("unsupported schema type %s", sch.typ)
	}
	if err != nil {
		return Decoded{}, fmt.Errorf("schema id %d (%s): %w", id, sch.typ, err)
	}
	d := Decoded{JSON: out, ID: id, Format: sch.typ.String()}
	d.Subject, d.Version = pickSubject(sch.versions, topic, isKey)
	return d, nil
}

func pickSubject(versions []sr.SubjectVersion, topic string, isKey bool) (string, int) {
	if len(versions) == 0 {
		return "", 0
	}
	want := topic + "-value"
	if isKey {
		want = topic + "-key"
	}
	for _, v := range versions {
		if v.Subject == want {
			return v.Subject, v.Version
		}
	}
	return versions[0].Subject, versions[0].Version
}

// compile turns a registry schema (and, recursively, its references) into
// something that can decode payloads.
func compile(ctx context.Context, r *Registry, s sr.Schema) (*schema, error) {
	switch s.Type {
	case sr.TypeAvro:
		a, err := compileAvro(ctx, r, s)
		if err != nil {
			return nil, err
		}
		return &schema{typ: s.Type, avro: a}, nil
	case sr.TypeProtobuf:
		fd, err := compileProtobuf(ctx, r, s)
		if err != nil {
			return nil, err
		}
		return &schema{typ: s.Type, proto: fd}, nil
	case sr.TypeJSON:
		// The payload is JSON already; the schema adds nothing to display.
		return &schema{typ: s.Type}, nil
	default:
		return nil, fmt.Errorf("unsupported schema type %s", s.Type)
	}
}

// walkReferences calls visit for every schema s references, depth first, so
// a referenced type is always seen before the schema that uses it. Each
// subject version is visited once.
func walkReferences(
	ctx context.Context,
	r *Registry,
	s sr.Schema,
	seen map[string]bool,
	depth int,
	visit func(name string, s sr.Schema) error,
) error {
	if depth > maxReferenceDepth {
		return fmt.Errorf("schema references nest deeper than %d", maxReferenceDepth)
	}
	for _, ref := range s.References {
		key := fmt.Sprintf("%s\x00%d", ref.Subject, ref.Version)
		if seen[key] {
			continue
		}
		seen[key] = true
		rs, err := r.reference(ctx, ref)
		if err != nil {
			return err
		}
		if err := walkReferences(ctx, r, rs, seen, depth+1, visit); err != nil {
			return err
		}
		if err := visit(ref.Name, rs); err != nil {
			return fmt.Errorf("reference %s: %w", ref.Name, err)
		}
	}
	return nil
}

// oneLine collapses a library error that spans lines (protocompile reports
// one per line) so it fits a status line.
func oneLine(err error) error {
	return fmt.Errorf("%s", strings.Join(strings.Fields(err.Error()), " "))
}
