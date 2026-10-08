// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// LoadTopicSchema
// ---------------------------------------------------------------------------

func TestLoadTopicSchema(t *testing.T) {
	t.Parallel()

	t.Run("valid schema", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, validSchemaYAML)

		schema, err := LoadTopicSchema(path)
		if err != nil {
			t.Fatalf("LoadTopicSchema() error: %v", err)
		}

		if schema.Topic != "test-orders" {
			t.Errorf("Topic = %q, want %q", schema.Topic, "test-orders")
		}
		if schema.Key != "orderId" {
			t.Errorf("Key = %q, want %q", schema.Key, "orderId")
		}
		if len(schema.Samples) != 2 {
			t.Fatalf("Samples count = %d, want 2", len(schema.Samples))
		}
		if got := schema.Samples[0]["customer"]; got != "Customer A" {
			t.Errorf("samples[0].customer = %v, want %q", got, "Customer A")
		}
	})

	t.Run("file not found", func(t *testing.T) {
		t.Parallel()

		_, err := LoadTopicSchema(filepath.Join(t.TempDir(), "nonexistent.yaml"))
		if err == nil {
			t.Fatal("expected error for missing file")
		}
		if !strings.Contains(err.Error(), "reading schema") {
			t.Errorf("error = %q, want it to contain %q", err.Error(), "reading schema")
		}
	})

	t.Run("invalid YAML", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, "not: [valid: yaml: {{")

		_, err := LoadTopicSchema(path)
		if err == nil {
			t.Fatal("expected error for invalid YAML")
		}
		if !strings.Contains(err.Error(), "parsing schema") {
			t.Errorf("error = %q, want it to contain %q", err.Error(), "parsing schema")
		}
	})

	t.Run("empty samples", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, "topic: empty-topic\nsamples: []\n")

		_, err := LoadTopicSchema(path)
		if err == nil {
			t.Fatal("expected error for empty samples")
		}
		if !strings.Contains(err.Error(), "no samples") {
			t.Errorf("error = %q, want it to contain %q", err.Error(), "no samples")
		}
	})

	t.Run("no samples key", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, "topic: missing-samples\n")

		_, err := LoadTopicSchema(path)
		if err == nil {
			t.Fatal("expected error when samples key is absent")
		}
		if !strings.Contains(err.Error(), "no samples") {
			t.Errorf("error = %q, want it to contain %q", err.Error(), "no samples")
		}
	})

	t.Run("key field missing from a sample", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, "topic: t\nkey: orderId\nsamples:\n  - orderId: 1\n  - status: open\n")

		_, err := LoadTopicSchema(path)
		if err == nil {
			t.Fatal("expected error when a sample lacks the key field")
		}
		if !strings.Contains(err.Error(), `samples[1] has no "orderId"`) {
			t.Errorf("error = %q, want it to name samples[1] and the key", err.Error())
		}
	})
}

// ---------------------------------------------------------------------------
// GenerateSchemaMessages
// ---------------------------------------------------------------------------

func TestGenerateSchemaMessages(t *testing.T) {
	t.Parallel()

	t.Run("generates n messages", func(t *testing.T) {
		t.Parallel()

		schema := loadValidSchema(t)

		const n = 3
		messages, err := GenerateSchemaMessages(schema, n)
		if err != nil {
			t.Fatalf("GenerateSchemaMessages() error: %v", err)
		}
		if len(messages) != n {
			t.Fatalf("message count = %d, want %d", len(messages), n)
		}

		wantKeys := []string{"12345", "67890", "12345"}
		for i, msg := range messages {
			if msg.Topic != "test-orders" {
				t.Errorf("messages[%d].Topic = %q, want %q", i, msg.Topic, "test-orders")
			}
			if string(msg.Key) != wantKeys[i] {
				t.Errorf("messages[%d].Key = %q, want %q", i, msg.Key, wantKeys[i])
			}

			var parsed map[string]any
			if err := json.Unmarshal(msg.Value, &parsed); err != nil {
				t.Errorf("messages[%d] is not valid JSON: %v", i, err)
				continue
			}
			if parsed["orderId"] == nil {
				t.Errorf("messages[%d] missing orderId", i)
			}
			if parsed["seq"] != float64(i) {
				t.Errorf("messages[%d].seq = %v, want %d", i, parsed["seq"], i)
			}
			if parsed["producedAt"] == nil {
				t.Errorf("messages[%d] missing producedAt", i)
			}
		}
	})

	t.Run("zero messages", func(t *testing.T) {
		t.Parallel()

		messages, err := GenerateSchemaMessages(loadValidSchema(t), 0)
		if err != nil {
			t.Fatalf("GenerateSchemaMessages(0) error: %v", err)
		}
		if len(messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(messages))
		}
	})

	t.Run("samples are not mutated", func(t *testing.T) {
		t.Parallel()

		schema := loadValidSchema(t)
		if _, err := GenerateSchemaMessages(schema, 4); err != nil {
			t.Fatal(err)
		}
		if _, ok := schema.Samples[0]["seq"]; ok {
			t.Error("generation wrote seq into the shared sample")
		}
	})

	t.Run("no key field leaves records unkeyed", func(t *testing.T) {
		t.Parallel()

		path := writeSchemaFile(t, "topic: t\nsamples:\n  - status: open\n")
		schema, err := LoadTopicSchema(path)
		if err != nil {
			t.Fatal(err)
		}
		messages, err := GenerateSchemaMessages(schema, 2)
		if err != nil {
			t.Fatal(err)
		}
		for i, msg := range messages {
			if msg.Key != nil {
				t.Errorf("messages[%d].Key = %q, want nil", i, msg.Key)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const validSchemaYAML = `
topic: test-orders
key: orderId
samples:
  - orderId: 12345
    customer: "Customer A"
    items:
      - sku: "SKU-1"
        quantity: 2
  - orderId: 67890
    customer: "Customer B"
`

func loadValidSchema(t *testing.T) *TopicSchema {
	t.Helper()

	schema, err := LoadTopicSchema(writeSchemaFile(t, validSchemaYAML))
	if err != nil {
		t.Fatalf("LoadTopicSchema() error: %v", err)
	}
	return schema
}

func writeSchemaFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "schema.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing schema file: %v", err)
	}
	return path
}
