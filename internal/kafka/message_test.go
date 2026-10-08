// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"testing"
)

func TestGenerateMessages(t *testing.T) {
	topic := "test-topic"
	count := 5

	messages, err := GenerateMessages(topic, count)
	if err != nil {
		t.Fatalf("GenerateMessages() error: %v", err)
	}

	if len(messages) != count {
		t.Fatalf("expected %d messages, got %d", count, len(messages))
	}

	seenIDs := make(map[string]bool)
	for i, msg := range messages {
		if msg.Topic != topic {
			t.Errorf("message[%d] topic = %q, want %q", i, msg.Topic, topic)
		}

		if len(msg.Key) == 0 {
			t.Errorf("message[%d] has empty key", i)
		}

		if len(msg.Value) == 0 {
			t.Errorf("message[%d] has empty value", i)
		}

		var tm TestMessage
		if err := json.Unmarshal(msg.Value, &tm); err != nil {
			t.Errorf("message[%d] unmarshal error: %v", i, err)
			continue
		}

		if tm.Sequence != i+1 {
			t.Errorf("message[%d] sequence = %d, want %d", i, tm.Sequence, i+1)
		}

		if tm.Source != "k4a" {
			t.Errorf("message[%d] source = %q, want %q", i, tm.Source, "k4a")
		}

		if tm.ID == "" {
			t.Errorf("message[%d] has empty ID", i)
		}

		if seenIDs[tm.ID] {
			t.Errorf("message[%d] has duplicate ID %q", i, tm.ID)
		}
		seenIDs[tm.ID] = true

		if tm.Payload == "" {
			t.Errorf("message[%d] has empty payload", i)
		}

		if tm.Timestamp.IsZero() {
			t.Errorf("message[%d] has zero timestamp", i)
		}
	}
}

func TestGenerateMessagesZero(t *testing.T) {
	messages, err := GenerateMessages("topic", 0)
	if err != nil {
		t.Fatalf("GenerateMessages(0) error: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(messages))
	}
}
