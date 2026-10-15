// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// An interrupted run used to report total-minus-failed as sent, so a Ctrl+C
// after three of ten records printed "9 sent".
func TestProduceTallyInterrupted(t *testing.T) {
	t.Parallel()

	tally := produceTally{total: 10}
	tally.record(kafka.ProduceResult{Sequence: 1})
	tally.record(kafka.ProduceResult{Sequence: 2, Err: errors.New("boom")})
	tally.record(kafka.ProduceResult{Sequence: 3})

	if want := "Done: 2 sent, 1 failed, 7 not sent (interrupted)"; tally.summary() != want {
		t.Errorf("summary() = %q, want %q", tally.summary(), want)
	}
	if tally.complete() {
		t.Error("complete() = true for an interrupted run")
	}
}

func TestProduceTallyComplete(t *testing.T) {
	t.Parallel()

	tally := produceTally{total: 2}
	tally.record(kafka.ProduceResult{Sequence: 1})
	tally.record(kafka.ProduceResult{Sequence: 2})

	if want := "Done: 2 sent, 0 failed"; tally.summary() != want {
		t.Errorf("summary() = %q, want %q", tally.summary(), want)
	}
	if !tally.complete() {
		t.Error("complete() = false with every record sent")
	}
}

// --topic overrides the schema's topic; the help text used to say the
// reverse.
func TestBuildMessagesTopicFlagOverridesSchema(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "schema.yaml")
	schema := "topic: from-schema\nsamples:\n  - id: 1\n"
	if err := os.WriteFile(path, []byte(schema), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ flag, want string }{
		{"", "from-schema"},
		{"from-flag", "from-flag"},
	} {
		topic, msgs, err := buildMessages(ProduceFlags{Topic: tc.flag, Schema: path, Count: 2})
		if err != nil {
			t.Fatalf("buildMessages(--topic=%q): %v", tc.flag, err)
		}
		if topic != tc.want {
			t.Errorf("buildMessages(--topic=%q) topic = %q, want %q", tc.flag, topic, tc.want)
		}
		for i, m := range msgs {
			if m.Topic != tc.want {
				t.Errorf("--topic=%q: message %d topic = %q, want %q", tc.flag, i, m.Topic, tc.want)
			}
		}
	}
}
