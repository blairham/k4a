// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// VersionCommand
// ---------------------------------------------------------------------------

func TestVersionCommandSynopsis(t *testing.T) {
	t.Parallel()

	cmd := &VersionCommand{Version: "1.0.0", Commit: "abc", Date: "2026-01-01"}
	if s := cmd.Synopsis(); s == "" {
		t.Error("Synopsis() returned empty")
	}
}

func TestVersionCommandHelp(t *testing.T) {
	t.Parallel()

	cmd := &VersionCommand{}
	if h := cmd.Help(); h == "" {
		t.Error("Help() returned empty")
	}
}

func TestVersionCommandRun(t *testing.T) {
	t.Parallel()

	cmd := &VersionCommand{Version: "1.0.0", Commit: "abc123", Date: "2026-01-01"}
	exit := cmd.Run(nil)
	if exit != 0 {
		t.Errorf("Run() = %d, want 0", exit)
	}
}

// ---------------------------------------------------------------------------
// ContextsCommand
// ---------------------------------------------------------------------------

func TestContextsCommandSynopsis(t *testing.T) {
	t.Parallel()

	cmd := &ContextsCommand{}
	if s := cmd.Synopsis(); s == "" {
		t.Error("Synopsis() returned empty")
	}
}

func TestContextsCommandHelp(t *testing.T) {
	t.Parallel()

	cmd := &ContextsCommand{}
	if h := cmd.Help(); h == "" {
		t.Error("Help() returned empty")
	}
}

func TestContextsCommandRunWithConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	yaml := `current-context: staging
contexts:
  staging:
    brokers: broker:9092
    auth: scram
  production:
    brokers: prod:9092
    auth: iam
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := &ContextsCommand{}
	exit := cmd.Run([]string{"--config=" + cfgPath})
	if exit != 0 {
		t.Errorf("Run() = %d, want 0", exit)
	}
}

func TestContextsCommandRunNoConfig(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "nonexistent", "config.yaml")

	cmd := &ContextsCommand{}
	exit := cmd.Run([]string{"--config=" + cfgPath})
	// Should succeed with empty output (no file = no contexts).
	if exit != 0 {
		t.Errorf("Run() = %d, want 0", exit)
	}
}

// ---------------------------------------------------------------------------
// UseContextCommand
// ---------------------------------------------------------------------------

func TestUseContextCommandSynopsis(t *testing.T) {
	t.Parallel()

	cmd := &UseContextCommand{}
	if s := cmd.Synopsis(); s == "" {
		t.Error("Synopsis() returned empty")
	}
}

func TestUseContextCommandHelp(t *testing.T) {
	t.Parallel()

	cmd := &UseContextCommand{}
	if h := cmd.Help(); h == "" {
		t.Error("Help() returned empty")
	}
}

func TestUseContextCommandRunNoArgs(t *testing.T) {
	t.Parallel()

	cmd := &UseContextCommand{}
	exit := cmd.Run(nil)
	if exit != 1 {
		t.Errorf("Run() = %d, want 1 for no args", exit)
	}
}

// ---------------------------------------------------------------------------
// UICommand
// ---------------------------------------------------------------------------

func TestUICommandSynopsis(t *testing.T) {
	t.Parallel()

	cmd := &UICommand{}
	if s := cmd.Synopsis(); s == "" {
		t.Error("Synopsis() returned empty")
	}
}

func TestUICommandHelp(t *testing.T) {
	t.Parallel()

	cmd := &UICommand{}
	h := cmd.Help()
	if h == "" {
		t.Error("Help() returned empty")
	}
	if !strings.Contains(h, ":q") {
		t.Error("Help() should mention :q for quit")
	}
	if !strings.Contains(h, "k4a") {
		t.Error("Help() should mention k4a")
	}
}

// ---------------------------------------------------------------------------
// Help/Synopsis for all remaining commands
// ---------------------------------------------------------------------------

func TestConsumeCommandHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&ConsumeCommand{}).Help())
}

func TestConsumeCommandSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&ConsumeCommand{}).Synopsis())
}

func TestProduceCommandHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&ProduceCommand{}).Help())
}

func TestProduceCommandSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&ProduceCommand{}).Synopsis())
}

func TestVerifyCommandHelp(t *testing.T) { t.Parallel(); assertNonEmpty(t, (&VerifyCommand{}).Help()) }

func TestVerifyCommandSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&VerifyCommand{}).Synopsis())
}

func TestCreateTopicCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&CreateTopicCmd{}).Help())
}

func TestCreateTopicCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&CreateTopicCmd{}).Synopsis())
}

func TestDeleteTopicCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&DeleteTopicCmd{}).Help())
}

func TestDeleteTopicCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&DeleteTopicCmd{}).Synopsis())
}

func TestCreateACLCmdHelp(t *testing.T) { t.Parallel(); assertNonEmpty(t, (&CreateACLCmd{}).Help()) }

func TestCreateACLCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&CreateACLCmd{}).Synopsis())
}
func TestListACLsCmdHelp(t *testing.T) { t.Parallel(); assertNonEmpty(t, (&ListACLsCmd{}).Help()) }
func TestListACLsCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&ListACLsCmd{}).Synopsis())
}

func TestAlterReplicationCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&AlterReplicationCmd{}).Help())
}

func TestAlterReplicationCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&AlterReplicationCmd{}).Synopsis())
}

func assertNonEmpty(t *testing.T, s string) {
	t.Helper()
	if s == "" {
		t.Error("expected non-empty string")
	}
}

// ---------------------------------------------------------------------------
// Run with bad flags — all commands should return non-zero
// ---------------------------------------------------------------------------

func TestConsumeCommandRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&ConsumeCommand{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestProduceCommandRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&ProduceCommand{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestVerifyCommandRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&VerifyCommand{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestCreateTopicCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&CreateTopicCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestDeleteTopicCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&DeleteTopicCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestCreateACLCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&CreateACLCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestListACLsCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&ListACLsCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

// ---------------------------------------------------------------------------
// Run with missing connection — no brokers, nonexistent config
// ---------------------------------------------------------------------------

func TestConsumeCommandRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&ConsumeCommand{}).Run([]string{"--topic", "test", "--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestVerifyCommandRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&VerifyCommand{}).Run([]string{"--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestCreateTopicCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&CreateTopicCmd{}).Run([]string{"--topic", "t", "--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestDeleteTopicCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&DeleteTopicCmd{}).Run([]string{"--topic", "t", "--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestCreateACLCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&CreateACLCmd{}).Run([]string{
		"--resource-type", "topic", "--resource-name", "t",
		"--principal", "User:x", "--operation", "read", "--permission", "allow",
		"--config", "/nonexistent/config.yaml",
	})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestListACLsCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&ListACLsCmd{}).Run([]string{"--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

// ---------------------------------------------------------------------------
// buildMessages
// ---------------------------------------------------------------------------

func TestBuildMessagesRandom(t *testing.T) {
	t.Parallel()

	flags := ProduceFlags{Topic: "test-topic", Count: 5}
	topic, msgs, err := buildMessages(flags)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", topic)
	}
	if len(msgs) != 5 {
		t.Errorf("len(msgs) = %d, want 5", len(msgs))
	}
}

func TestBuildMessagesNoTopic(t *testing.T) {
	t.Parallel()

	flags := ProduceFlags{Count: 5}
	_, _, err := buildMessages(flags)
	if err == nil {
		t.Error("expected error for missing topic")
	}
}

func TestBuildMessagesSchemaNotFound(t *testing.T) {
	t.Parallel()

	flags := ProduceFlags{Topic: "t", Count: 5, Schema: "/nonexistent/schema.yaml"}
	_, _, err := buildMessages(flags)
	if err == nil {
		t.Error("expected error for missing schema file")
	}
}

func TestBuildMessagesSchemaValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.yaml")
	yaml := `topic: schema-topic
samples:
  - orderId: 1
    status: "open"
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	flags := ProduceFlags{Count: 3, Schema: path}
	topic, msgs, err := buildMessages(flags)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "schema-topic" {
		t.Errorf("topic = %q, want schema-topic", topic)
	}
	if len(msgs) != 3 {
		t.Errorf("len(msgs) = %d, want 3", len(msgs))
	}
}

// ---------------------------------------------------------------------------
// UseContextCommand — valid context
// ---------------------------------------------------------------------------

func TestUseContextCommandRunValidContext(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, ".k4a", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}

	yaml := `current-context: staging
contexts:
  staging:
    brokers: broker:9092
    auth: scram
  production:
    brokers: prod:9092
    auth: iam
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	// UseContextCommand uses config.DefaultConfigPath() which reads $HOME.
	// Override HOME so it resolves to our temp dir.
	t.Setenv("HOME", dir)

	cmd := &UseContextCommand{}
	exit := cmd.Run([]string{"production"})
	if exit != 0 {
		t.Errorf("Run() = %d, want 0", exit)
	}
}

// ---------------------------------------------------------------------------
// UseContextCommand — missing context
// ---------------------------------------------------------------------------

func TestUseContextCommandRunMissingContext(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, ".k4a", "config.yaml")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}

	yaml := `current-context: staging
contexts:
  staging:
    brokers: broker:9092
    auth: scram
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", dir)

	cmd := &UseContextCommand{}
	exit := cmd.Run([]string{"nonexistent"})
	if exit != 1 {
		t.Errorf("Run() = %d, want 1 for missing context", exit)
	}
}

// ---------------------------------------------------------------------------
// ProduceCommand — no topic
// ---------------------------------------------------------------------------

func TestProduceCommandRunNoTopic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yaml := `current-context: dev
contexts:
  dev:
    brokers: localhost:9092
    auth: plaintext
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	// No --topic and no --schema: buildMessages should fail.
	code := (&ProduceCommand{}).Run([]string{"--config", cfgPath, "--context", "dev"})
	if code == 0 {
		t.Error("expected non-zero exit for missing topic")
	}
}

// ---------------------------------------------------------------------------
// AlterReplicationCmd — bad flags
// ---------------------------------------------------------------------------

func TestAlterReplicationCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&AlterReplicationCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

// ---------------------------------------------------------------------------
// AlterReplicationCmd — missing connection
// ---------------------------------------------------------------------------

func TestAlterReplicationCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&AlterReplicationCmd{}).Run([]string{
		"--topic", "t", "--replication-factor", "3",
		"--config", "/nonexistent/config.yaml",
	})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}
