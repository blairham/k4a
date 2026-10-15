// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package schemareg

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// magicByte opens every Confluent-framed payload; the 4-byte big-endian
// schema ID follows it.
const (
	magicByte  = 0
	headerSize = 5
)

// maxMessageIndexes bounds the Protobuf message-index path. A real schema
// nests messages a handful of levels deep; the cap keeps a hostile count
// from allocating gigabytes before the bytes behind it are even checked.
const maxMessageIndexes = 64

// ErrNotFramed means the payload does not carry the Confluent wire-format
// header, so it was not written by a registry-aware serializer. Callers show
// such payloads as they always have, without a reason.
var ErrNotFramed = errors.New("not schema-registry framed")

// ParseHeader splits a Confluent-framed payload into its schema ID and the
// encoded body after the header. ok is false when b is too short, does not
// start with the magic byte, or names a non-positive ID (the registry
// allocates IDs from 1, so 0 or a negative ID is a payload that merely starts
// with a zero byte).
func ParseHeader(b []byte) (id int, body []byte, ok bool) {
	if len(b) < headerSize || b[0] != magicByte {
		return 0, nil, false
	}
	raw := int32(binary.BigEndian.Uint32(b[1:headerSize])) //nolint:gosec // the wire field is a signed int32
	if raw <= 0 {
		return 0, nil, false
	}
	return int(raw), b[headerSize:], true
}

// ParseMessageIndexes reads the Protobuf message-index path that Confluent's
// serializer writes between the header and the message: a zigzag varint count
// followed by that many zigzag varint indexes. A count of zero (the single
// byte 0x00) is shorthand for [0], the first message in the file.
func ParseMessageIndexes(b []byte) (indexes []int, rest []byte, err error) {
	n, used := binary.Varint(b)
	if used <= 0 {
		return nil, nil, errors.New("truncated protobuf message-index count")
	}
	b = b[used:]
	if n == 0 {
		return []int{0}, b, nil
	}
	if n < 0 || n > maxMessageIndexes {
		return nil, nil, fmt.Errorf("protobuf message-index count %d out of range", n)
	}
	indexes = make([]int, 0, n)
	for range n {
		v, used := binary.Varint(b)
		if used <= 0 {
			return nil, nil, errors.New("truncated protobuf message index")
		}
		if v < 0 || v > maxIndexValue {
			return nil, nil, fmt.Errorf("protobuf message index %d out of range", v)
		}
		indexes = append(indexes, int(v))
		b = b[used:]
	}
	return indexes, b, nil
}

// maxIndexValue is far beyond any real schema's message count but small
// enough to fit an int on every platform.
const maxIndexValue = 1 << 20
