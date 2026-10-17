// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package archive searches a topic's S3 archive (written by a sink
// connector, read through a Glue table) with Athena. See
// docs/design/archive-search.md.
package archive

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Default thresholds. Deliberately low for a first release; see the design's
// open questions.
const (
	DefaultConfirmBytes int64 = 1 << 30  // 1 GiB
	DefaultMaxBytes     int64 = 50 << 30 // 50 GiB
	DefaultWorkgroup          = "primary"
	DefaultCatalog            = "AwsDataCatalog"
)

// Timestamp column encodings.
const (
	TimestampEpochMillis = "epoch-millis"
	TimestampTimestamp   = "timestamp"
)

// Partition value types.
const (
	PartitionString  = "string"
	PartitionInteger = "integer"
)

// Config is a context's `archive:` block.
type Config struct {
	Topics         map[string]Topic `yaml:"topics,omitempty"`
	Profile        string           `yaml:"profile,omitempty"`
	Region         string           `yaml:"region,omitempty"`
	Workgroup      string           `yaml:"workgroup,omitempty"`
	OutputLocation string           `yaml:"output-location,omitempty"`
	Catalog        string           `yaml:"catalog,omitempty"`
	ConfirmBytes   string           `yaml:"confirm-bytes,omitempty"`
	MaxBytes       string           `yaml:"max-bytes,omitempty"`
}

// Topic is one topic's archive table.
type Topic struct {
	Columns        Columns         `yaml:"columns"`
	Database       string          `yaml:"database"`
	Table          string          `yaml:"table"`
	TimeZone       string          `yaml:"time-zone,omitempty"`
	Location       string          `yaml:"location,omitempty"`
	TimePartitions []TimePartition `yaml:"time-partitions"`
}

// Columns maps record fields to the table's column names.
type Columns struct {
	Value         string `yaml:"value"`
	Timestamp     string `yaml:"timestamp"`
	TimestampType string `yaml:"timestamp-type,omitempty"`
	Key           string `yaml:"key,omitempty"`
	Headers       string `yaml:"headers,omitempty"`
	Partition     string `yaml:"partition,omitempty"`
	Offset        string `yaml:"offset,omitempty"`
}

// TimePartition is one time-derived partition column.
type TimePartition struct {
	Column string `yaml:"column"`
	Format string `yaml:"format"`
	Type   string `yaml:"type,omitempty"`
}

// Limits are the resolved cost thresholds.
type Limits struct {
	ConfirmBytes int64
	MaxBytes     int64
}

// Limits parses the configured thresholds, applying the defaults.
func (c Config) Limits() (Limits, error) {
	l := Limits{ConfirmBytes: DefaultConfirmBytes, MaxBytes: DefaultMaxBytes}
	var err error
	if c.ConfirmBytes != "" {
		if l.ConfirmBytes, err = ParseSize(c.ConfirmBytes); err != nil {
			return Limits{}, fmt.Errorf("archive.confirm-bytes: %w", err)
		}
	}
	if c.MaxBytes != "" {
		if l.MaxBytes, err = ParseSize(c.MaxBytes); err != nil {
			return Limits{}, fmt.Errorf("archive.max-bytes: %w", err)
		}
	}
	return l, nil
}

// WorkgroupOrDefault is the workgroup to run in.
func (c Config) WorkgroupOrDefault() string {
	if c.Workgroup == "" {
		return DefaultWorkgroup
	}
	return c.Workgroup
}

// CatalogOrDefault is the data catalog the table lives in.
func (c Config) CatalogOrDefault() string {
	if c.Catalog == "" {
		return DefaultCatalog
	}
	return c.Catalog
}

// ErrNoArchive is returned when the context has no archive for the topic.
var ErrNoArchive = errors.New("no archive configured")

// Topic returns the validated table settings for topic. Validation happens
// here, per search, so a broken archive block never stops anything else
// from reading the config file.
func (c Config) Topic(name string) (Topic, error) {
	t, ok := c.Topics[name]
	if !ok {
		return Topic{}, fmt.Errorf("%w for topic %q (add it under the context's archive.topics)", ErrNoArchive, name)
	}
	if err := t.validate(); err != nil {
		return Topic{}, fmt.Errorf("archive.topics.%s: %w", name, err)
	}
	if !identRE.MatchString(c.CatalogOrDefault()) {
		return Topic{}, fmt.Errorf("archive.catalog %q is not an identifier", c.Catalog)
	}
	return t, nil
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func (t Topic) validate() error {
	idents := []struct{ name, val string }{
		{"database", t.Database},
		{"table", t.Table},
		{"columns.value", t.Columns.Value},
		{"columns.timestamp", t.Columns.Timestamp},
	}
	for _, opt := range []struct{ name, val string }{
		{"columns.key", t.Columns.Key},
		{"columns.headers", t.Columns.Headers},
		{"columns.partition", t.Columns.Partition},
		{"columns.offset", t.Columns.Offset},
	} {
		if opt.val != "" {
			idents = append(idents, opt)
		}
	}
	for _, id := range idents {
		if !identRE.MatchString(id.val) {
			return fmt.Errorf("%s %q is not an identifier ([A-Za-z_][A-Za-z0-9_]*); use a view for anything else",
				id.name, id.val)
		}
	}
	switch t.Columns.TimestampType {
	case "", TimestampEpochMillis, TimestampTimestamp:
	default:
		return fmt.Errorf("columns.timestamp-type %q: use %s or %s",
			t.Columns.TimestampType, TimestampEpochMillis, TimestampTimestamp)
	}
	if _, err := t.location(); err != nil {
		return err
	}
	if len(t.TimePartitions) == 0 {
		return errors.New("time-partitions is required: without it every search scans the whole table")
	}
	for i, p := range t.TimePartitions {
		if err := p.validate(); err != nil {
			return fmt.Errorf("time-partitions[%d]: %w", i, err)
		}
	}
	if t.Location != "" {
		if !strings.HasPrefix(t.Location, "s3://") {
			return fmt.Errorf("location %q must be an s3:// URI", t.Location)
		}
		if err := checkFormat(t.Location); err != nil {
			return fmt.Errorf("location: %w", err)
		}
	}
	return nil
}

func (p TimePartition) validate() error {
	if !identRE.MatchString(p.Column) {
		return fmt.Errorf("column %q is not an identifier", p.Column)
	}
	if err := checkFormat(p.Format); err != nil {
		return err
	}
	if !hasToken(p.Format) {
		return fmt.Errorf("format %q has no {yyyy}, {MM}, {dd} or {HH} token", p.Format)
	}
	switch p.Type {
	case "", PartitionString:
		if strings.ContainsAny(p.Format, "'\\") {
			return fmt.Errorf("format %q contains a quote or backslash", p.Format)
		}
	case PartitionInteger:
		if _, ok := tokenLayouts[p.Format]; !ok {
			return fmt.Errorf("an integer partition's format must be a single token, not %q", p.Format)
		}
	default:
		return fmt.Errorf("type %q: use %s or %s", p.Type, PartitionString, PartitionInteger)
	}
	return nil
}

// location is the time zone partition values are written in.
func (t Topic) location() (*time.Location, error) {
	if t.TimeZone == "" {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(t.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("time-zone %q: %w", t.TimeZone, err)
	}
	return loc, nil
}

var sizeUnits = []struct {
	suffix string
	mult   int64
}{
	// Longest suffixes first so "GiB" is not read as "B".
	{"KiB", 1 << 10},
	{"MiB", 1 << 20},
	{"GiB", 1 << 30},
	{"TiB", 1 << 40},
	{"KB", 1e3},
	{"MB", 1e6},
	{"GB", 1e9},
	{"TB", 1e12},
	{"B", 1},
}

// ParseSize reads a byte size such as 500MB, 1GiB or 2048.
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	num := s
	for _, u := range sizeUnits {
		if strings.HasSuffix(s, u.suffix) {
			mult = u.mult
			num = strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			break
		}
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("invalid size %q (e.g. 500MB, 1GiB)", s)
	}
	v := f * float64(mult)
	if v >= 1<<62 {
		return 0, fmt.Errorf("size %q is too large", s)
	}
	return int64(v), nil
}

// FormatSize renders n bytes for a human, in binary units.
func FormatSize(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	f := float64(n)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
}

// ListPriceUSD is Athena's list price for n scanned bytes ($5/TB, 10 MB
// minimum). Regions differ; this is for a rough figure in prompts only.
func ListPriceUSD(n int64) float64 {
	const minBytes = 10 * 1000 * 1000
	n = max(n, minBytes)
	return float64(n) / 1e12 * 5
}
