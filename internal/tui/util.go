// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// savedMessage is the JSON structure written when saving a message to file.
type savedMessage struct {
	Value     json.RawMessage       `json:"value"`
	Topic     string                `json:"topic"`
	Timestamp string                `json:"timestamp"`
	Key       string                `json:"key"`
	Headers   []kafka.MessageHeader `json:"headers,omitempty"`
	Offset    int64                 `json:"offset"`
	Partition int                   `json:"partition"`
}

func messageFilename(msg *kafka.ConsumedMessage) string {
	return fmt.Sprintf("%s-%d-%d.json", sanitizeFilename(msg.Topic), msg.Partition, msg.Offset)
}

func saveMessageToFile(msg *kafka.ConsumedMessage, path string) error {
	var value json.RawMessage
	if json.Valid([]byte(msg.Value)) {
		value = json.RawMessage(msg.Value)
	} else {
		escaped, err := json.Marshal(msg.Value)
		if err != nil {
			return fmt.Errorf("marshaling value: %w", err)
		}
		value = escaped
	}

	out := savedMessage{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Time.Format(time.RFC3339Nano),
		Key:       msg.Key,
		Value:     value,
		Headers:   msg.Headers,
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling message: %w", err)
	}
	data = append(data, '\n')

	// Create parent directories if the user specified a path like dir/file.json.
	if dir := filepath.Dir(path); dir != "." {
		if mkErr := os.MkdirAll(dir, 0o700); mkErr != nil {
			return fmt.Errorf("creating directory: %w", mkErr)
		}
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func sanitizeFilename(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '_'
	}, s)
}

// parseDuration parses a human-friendly duration string (e.g. "1h", "3h",
// "30m", "1d", "7d", "0") and returns the equivalent in milliseconds.
// Supports suffixes: m (minutes), h (hours), d (days). "0" means purge all.
// Append "!" to make the change permanent (e.g. "1d!").
func parseDuration(s string) (ms int64, permanent bool, err error) {
	if strings.HasSuffix(s, "!") {
		permanent = true
		s = s[:len(s)-1]
	}

	if s == "0" {
		if permanent {
			return 0, true, nil // 0! means remove the retention.ms override
		}
		return 1000, false, nil // 1 second — effectively purge everything
	}
	if len(s) < 2 {
		return 0, false, fmt.Errorf("invalid duration %q — use e.g. 1h, 3h, 1d, 0 (add ! for permanent)", s)
	}

	suffix := s[len(s)-1]
	numStr := s[:len(s)-1]
	var n int
	if _, scanErr := fmt.Sscanf(numStr, "%d", &n); scanErr != nil || n <= 0 {
		return 0, false, fmt.Errorf("invalid duration %q — use e.g. 1h, 3h, 1d, 0 (add ! for permanent)", s)
	}

	switch suffix {
	case 'm':
		ms = int64(n) * 60 * 1000
	case 'h':
		ms = int64(n) * 60 * 60 * 1000
	case 'd':
		ms = int64(n) * 24 * 60 * 60 * 1000
	default:
		return 0, false, fmt.Errorf("unknown suffix %q — use m (minutes), h (hours), or d (days)", string(suffix))
	}
	return ms, permanent, nil
}

// formatRetentionMs formats milliseconds back to a human-friendly string.
func formatRetentionMs(ms int64) string {
	switch {
	case ms%(24*60*60*1000) == 0:
		return fmt.Sprintf("%dd", ms/(24*60*60*1000))
	case ms%(60*60*1000) == 0:
		return fmt.Sprintf("%dh", ms/(60*60*1000))
	case ms%(60*1000) == 0:
		return fmt.Sprintf("%dm", ms/(60*1000))
	default:
		return fmt.Sprintf("%dms", ms)
	}
}
