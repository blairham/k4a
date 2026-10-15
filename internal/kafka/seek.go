// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SeekMode says where a read starts in each partition.
type SeekMode int

const (
	// SeekStart reads from the log start offset.
	SeekStart SeekMode = iota
	// SeekOffset reads from Seek.Offset, clamped into the log.
	SeekOffset
	// SeekTime reads from the first record at or after Seek.Time, or from
	// the log end when nothing is that new.
	SeekTime
)

// Seek is the start position of a groupless read. The zero value is
// SeekStart, so it changes nothing for a caller that does not set it.
type Seek struct {
	Time   time.Time
	Offset int64
	Mode   SeekMode
}

// String describes the seek for a status line.
func (s Seek) String() string {
	switch s.Mode {
	case SeekOffset:
		return fmt.Sprintf("offset %d", s.Offset)
	case SeekTime:
		return "records since " + s.Time.Format(time.RFC3339)
	default:
		return "the beginning"
	}
}

// kgoOffset is the franz-go start offset for s. An exact offset outside a
// partition's log starts at the nearest end of it.
func (s Seek) kgoOffset() kgo.Offset {
	switch s.Mode {
	case SeekOffset:
		return kgo.NewOffset().At(s.Offset)
	case SeekTime:
		return kgo.NewOffset().AfterMilli(s.Time.UnixMilli())
	default:
		return kgo.NewOffset().AtStart()
	}
}
