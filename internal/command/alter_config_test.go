// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import "testing"

func TestAlterConfigCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&AlterConfigCmd{}).Help())
}

func TestAlterConfigCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&AlterConfigCmd{}).Synopsis())
}

func TestAlterConfigCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&AlterConfigCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

// Usage mistakes must fail before a connection is attempted, so the user gets
// the real complaint rather than a dial error.
func TestAlterConfigCmdRunRequiresTopic(t *testing.T) {
	t.Parallel()
	code := (&AlterConfigCmd{}).Run([]string{"--brokers", "localhost:9092", "--delete", "retention.ms"})
	if code == 0 {
		t.Error("expected non-zero exit when no TOPIC argument is given")
	}
}

func TestAlterConfigCmdRunRequiresAnOperation(t *testing.T) {
	t.Parallel()
	code := (&AlterConfigCmd{}).Run([]string{"--brokers", "localhost:9092", "some-topic"})
	if code == 0 {
		t.Error("expected non-zero exit when neither --set nor --delete is given")
	}
}

// Deletes must be ordered ahead of sets so "--delete k --set k=v" settles on v
// instead of depending on flag order.
func TestParseConfigOpsDeletesComeFirst(t *testing.T) {
	t.Parallel()

	ops, err := parseConfigOps([]string{"min.insync.replicas=2"}, []string{"min.insync.replicas"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}
	if !ops[0].Delete {
		t.Error("expected the delete to be applied first")
	}
	if ops[1].Delete || ops[1].Value != "2" {
		t.Errorf("expected the set to be applied second: %+v", ops[1])
	}
}

func TestParseConfigOpsSet(t *testing.T) {
	t.Parallel()

	ops, err := parseConfigOps([]string{"retention.ms=86400000"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ops) != 1 || ops[0].Key != "retention.ms" || ops[0].Value != "86400000" {
		t.Fatalf("unexpected ops: %+v", ops)
	}
}

// A value containing "=" must survive: only the first separator splits.
func TestParseConfigOpsValueWithEquals(t *testing.T) {
	t.Parallel()

	ops, err := parseConfigOps([]string{"some.key=a=b"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ops[0].Key != "some.key" || ops[0].Value != "a=b" {
		t.Errorf("expected only the first = to split: %+v", ops[0])
	}
}

// An empty value is legitimate (it clears a string config), so key= must parse.
func TestParseConfigOpsEmptyValue(t *testing.T) {
	t.Parallel()

	ops, err := parseConfigOps([]string{"some.key="}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ops[0].Key != "some.key" || ops[0].Value != "" {
		t.Errorf("expected an empty value to be accepted: %+v", ops[0])
	}
}

func TestParseConfigOpsRejectsMalformedSet(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"no-equals-sign", "=novalue"} {
		if _, err := parseConfigOps([]string{bad}, nil); err == nil {
			t.Errorf("expected an error for --set %q", bad)
		}
	}
}

func TestParseConfigOpsRejectsBlankDelete(t *testing.T) {
	t.Parallel()

	if _, err := parseConfigOps(nil, []string{"   "}); err == nil {
		t.Error("expected an error for a blank --delete key")
	}
}

func TestParseConfigOpsEmpty(t *testing.T) {
	t.Parallel()

	ops, err := parseConfigOps(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ops) != 0 {
		t.Errorf("expected no ops, got %d", len(ops))
	}
}
