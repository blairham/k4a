// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"slices"
	"testing"
)

// The regression: after :upgrade the TUI re-exec'd with only --context, so a
// session started with -b/--auth restarted into the picker or an error, and
// --readonly was silently lost.
func TestRestartCommandReplaysDirectConnection(t *testing.T) {
	t.Parallel()
	argv := []string{"k4a", "-b", "localhost:9092", "--auth", "plaintext", "--readonly"}
	env := []string{"KAFKA_BROKERS=x:1", "HOME=/h"}
	flags := &UIFlags{Brokers: "localhost:9092", Auth: "plaintext", Readonly: true}

	gotArgv, gotEnv := restartCommand(argv, env, flags, "direct", "direct")
	if !slices.Equal(gotArgv, argv) {
		t.Errorf("argv = %q, want %q", gotArgv, argv)
	}
	if !slices.Equal(gotEnv, env) {
		t.Errorf("env = %q, want %q", gotEnv, env)
	}
}

func TestRestartCommandReplaysUnswitchedContext(t *testing.T) {
	t.Parallel()
	argv := []string{"k4a", "ui", "-c", "prod", "--profile", "P"}
	got, _ := restartCommand(argv, nil, &UIFlags{Context: "prod", Profile: "P"}, "prod", "prod")
	if !slices.Equal(got, argv) {
		t.Errorf("argv = %q, want %q", got, argv)
	}
}

// After a switch in the picker the original connection flags name the wrong
// cluster; the restart follows the active context and keeps display flags.
func TestRestartCommandFollowsSwitchedContext(t *testing.T) {
	t.Parallel()
	argv := []string{"k4a", "-b", "localhost:9092", "--readonly", "--config", "/c.yaml"}
	env := []string{"KAFKA_BROKERS=localhost:9092", "HOME=/h"}
	flags := &UIFlags{Brokers: "localhost:9092", Readonly: true, ConfigFile: "/c.yaml"}

	gotArgv, gotEnv := restartCommand(argv, env, flags, "direct", "staging")
	wantArgv := []string{"k4a", "--context", "staging", "--config", "/c.yaml", "--readonly"}
	if !slices.Equal(gotArgv, wantArgv) {
		t.Errorf("argv = %q, want %q", gotArgv, wantArgv)
	}
	if want := []string{"HOME=/h"}; !slices.Equal(gotEnv, want) {
		t.Errorf("env = %q, want %q", gotEnv, want)
	}
}
