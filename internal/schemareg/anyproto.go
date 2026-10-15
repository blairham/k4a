// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// protoSchema is a compiled Protobuf schema plus the message types it can
// name in a google.protobuf.Any: its own and those of every file it imports.
type protoSchema struct {
	fd    protoreflect.FileDescriptor
	types *protoregistry.Types
}

func newProtoSchema(fd protoreflect.FileDescriptor) *protoSchema {
	ps := &protoSchema{fd: fd, types: new(protoregistry.Types)}
	ps.register(fd, map[string]bool{})
	return ps
}

// register adds every message of f and its transitive imports. A name seen
// twice (two files defining it) keeps the first; Any resolution is a display
// nicety, never a reason to fail.
func (ps *protoSchema) register(f protoreflect.FileDescriptor, seen map[string]bool) {
	if seen[f.Path()] {
		return
	}
	seen[f.Path()] = true
	imports := f.Imports()
	for i := range imports.Len() {
		ps.register(imports.Get(i).FileDescriptor, seen)
	}
	ps.registerMessages(f.Messages())
}

func (ps *protoSchema) registerMessages(msgs protoreflect.MessageDescriptors) {
	for i := range msgs.Len() {
		md := msgs.Get(i)
		if !md.IsMapEntry() {
			_ = ps.types.RegisterMessage(dynamicpb.NewMessageType(md)) //nolint:errcheck // a duplicate keeps the first
		}
		ps.registerMessages(md.Messages())
	}
}

// The Any rendering of a payload whose type the schema cannot name: the
// unresolvable Any is repacked as this message, which protojson can render,
// and restoreUnresolvedAnys rewrites it back to {"@type": url, "value": ...}.
const (
	anyFullName        = "google.protobuf.Any"
	unresolvedAnyURL   = "type.googleapis.com/k4a.internal.UnresolvedAny"
	maxAnyNestingDepth = 32
)

var unresolvedAnyType = func() protoreflect.MessageType {
	str := descriptorpb.FieldDescriptorProto_TYPE_STRING
	byt := descriptorpb.FieldDescriptorProto_TYPE_BYTES
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("k4a/internal/unresolved_any.proto"),
		Package: proto.String("k4a.internal"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("UnresolvedAny"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:     proto.String("type_url"),
					Number:   proto.Int32(1),
					Type:     &str,
					Label:    &opt,
					JsonName: proto.String("typeUrl"),
				},
				{Name: proto.String("value"), Number: proto.Int32(2), Type: &byt, Label: &opt, JsonName: proto.String("value")},
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic(err) // a fixed descriptor; cannot fail
	}
	return dynamicpb.NewMessageType(fd.Messages().Get(0))
}()

// FindMessageByURL resolves an Any's type URL against the schema's own files,
// then the well-known google.protobuf types linked into k4a.
func (ps *protoSchema) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	if url == unresolvedAnyURL {
		return unresolvedAnyType, nil
	}
	name := url
	if i := strings.LastIndexByte(url, '/'); i >= 0 {
		name = url[i+1:]
	}
	return ps.FindMessageByName(protoreflect.FullName(name))
}

// FindMessageByName looks in the schema, then the well-known types.
func (ps *protoSchema) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if mt, err := ps.types.FindMessageByName(name); err == nil {
		return mt, nil
	}
	if strings.HasPrefix(string(name), "google.protobuf.") {
		return protoregistry.GlobalTypes.FindMessageByName(name)
	}
	return nil, protoregistry.NotFound
}

// FindExtensionByName and FindExtensionByNumber complete protojson's resolver
// interface; the schema's extensions are not registered, so they render as
// they always have.
func (ps *protoSchema) FindExtensionByName(name protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return protoregistry.GlobalTypes.FindExtensionByName(name)
}

func (ps *protoSchema) FindExtensionByNumber(
	message protoreflect.FullName, field protoreflect.FieldNumber,
) (protoreflect.ExtensionType, error) {
	return protoregistry.GlobalTypes.FindExtensionByNumber(message, field)
}

// prepareAnys walks m for google.protobuf.Any values. One whose type the
// schema can name is left for protojson to expand (after its own contents are
// prepared); one it cannot, or whose bytes do not parse as that type, is
// repacked as an UnresolvedAny so protojson renders it instead of failing the
// whole record.
func (ps *protoSchema) prepareAnys(m protoreflect.Message, depth int) {
	if depth > maxAnyNestingDepth {
		return
	}
	if m.Descriptor().FullName() == anyFullName {
		ps.prepareAny(m, depth)
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() && fd.Message() != nil:
			l := v.List()
			for i := range l.Len() {
				ps.prepareAnys(l.Get(i).Message(), depth+1)
			}
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					ps.prepareAnys(mv.Message(), depth+1)
					return true
				})
			}
		case fd.Message() != nil:
			ps.prepareAnys(v.Message(), depth+1)
		}
		return true
	})
}

func (ps *protoSchema) prepareAny(m protoreflect.Message, depth int) {
	fields := m.Descriptor().Fields()
	urlField, valueField := fields.ByNumber(1), fields.ByNumber(2)
	if urlField == nil || valueField == nil {
		return
	}
	url, value := m.Get(urlField).String(), m.Get(valueField).Bytes()
	if url == "" && len(value) == 0 {
		return // an empty Any renders as {}
	}
	if mt, err := ps.FindMessageByURL(url); err == nil && url != unresolvedAnyURL {
		inner := mt.New()
		if proto.Unmarshal(value, inner.Interface()) == nil {
			ps.prepareAnys(inner, depth+1)
			if b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(inner.Interface()); err == nil {
				m.Set(valueField, protoreflect.ValueOfBytes(b))
				return
			}
		}
	}
	u := unresolvedAnyType.New()
	ufields := u.Descriptor().Fields()
	u.Set(ufields.ByNumber(1), protoreflect.ValueOfString(url))
	u.Set(ufields.ByNumber(2), protoreflect.ValueOfBytes(value))
	b, err := proto.Marshal(u.Interface())
	if err != nil {
		return
	}
	m.Set(urlField, protoreflect.ValueOfString(unresolvedAnyURL))
	m.Set(valueField, protoreflect.ValueOfBytes(b))
}

// restoreUnresolvedAnys turns protojson's rendering of a repacked Any back
// into the Any's own type URL and base64 value. The patterns contain bare
// quotes, which protojson always escapes inside a string, so they can only
// match the object keys it wrote.
func restoreUnresolvedAnys(s string) string {
	s = strings.ReplaceAll(s, `"@type":"`+unresolvedAnyURL+`","typeUrl":`, `"@type":`)
	return strings.ReplaceAll(s, `"@type":"`+unresolvedAnyURL+`"`, `"@type":""`)
}
