// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// MaxLineRecordBytes bounds one stdin line, matching the broker's default
// message.max.bytes (1 MiB); a longer line is an error, not a silent split.
const MaxLineRecordBytes = 1 << 20

// ParseHeader reads one "name=value" header. The name must be non-empty;
// the value may be empty and may itself contain '='.
func ParseHeader(s string) (kgo.RecordHeader, error) {
	name, value, ok := strings.Cut(s, "=")
	if !ok || name == "" {
		return kgo.RecordHeader{}, fmt.Errorf("header %q: want name=value", s)
	}
	return kgo.RecordHeader{Key: name, Value: []byte(value)}, nil
}

// ParseHeaders parses each of specs with ParseHeader.
func ParseHeaders(specs []string) ([]kgo.RecordHeader, error) {
	headers := make([]kgo.RecordHeader, 0, len(specs))
	for _, s := range specs {
		h, err := ParseHeader(s)
		if err != nil {
			return nil, err
		}
		headers = append(headers, h)
	}
	return headers, nil
}

// ParseHeaderList parses a comma-separated "a=1,b=2" list, the single-line
// form the TUI's Produce form takes. Blank entries are skipped.
func ParseHeaderList(list string) ([]kgo.RecordHeader, error) {
	var specs []string
	for s := range strings.SplitSeq(list, ",") {
		if s = strings.TrimSpace(s); s != "" {
			specs = append(specs, s)
		}
	}
	return ParseHeaders(specs)
}

// CustomRecords returns count copies of one record with the given key,
// value and headers. An empty key is sent as a null key, so the default
// partitioner spreads the copies instead of pinning them to one partition.
func CustomRecords(topic, key, value string, headers []kgo.RecordHeader, count int) []*kgo.Record {
	records := make([]*kgo.Record, 0, count)
	for range count {
		r := &kgo.Record{Topic: topic, Value: []byte(value), Headers: headers}
		if key != "" {
			r.Key = []byte(key)
		}
		records = append(records, r)
	}
	return records
}

// ReadLineRecords reads one record per line from r. Each line is a value;
// with a non-empty keySep, the text before its first occurrence is the key
// and the rest the value, and a line without it is an error rather than a
// record with a guessed key. Empty lines are skipped. Every record carries
// headers.
func ReadLineRecords(r io.Reader, topic, keySep string, headers []kgo.RecordHeader) ([]*kgo.Record, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), MaxLineRecordBytes)
	var records []*kgo.Record
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSuffix(sc.Text(), "\r")
		if text == "" {
			continue
		}
		rec := &kgo.Record{Topic: topic, Headers: headers}
		if keySep == "" {
			rec.Value = []byte(text)
		} else {
			key, value, ok := strings.Cut(text, keySep)
			if !ok {
				return nil, fmt.Errorf("line %d: no key separator %q", line, keySep)
			}
			if key != "" {
				rec.Key = []byte(key)
			}
			rec.Value = []byte(value)
		}
		records = append(records, rec)
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, fmt.Errorf("a line is longer than %d bytes: %w", MaxLineRecordBytes, err)
		}
		return nil, err
	}
	return records, nil
}
