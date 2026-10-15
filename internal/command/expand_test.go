// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import "testing"

func TestExpandEnvFlags(t *testing.T) {
	t.Setenv("K4A_TEST_HOST", "broker.example.com")
	t.Setenv("K4A_TEST_USER", "admin")

	type flags struct {
		Host     string
		User     string
		Static   string
		IntField int // should be untouched
	}

	f := &flags{
		Host:     "$K4A_TEST_HOST",
		User:     "${K4A_TEST_USER}",
		Static:   "literal",
		IntField: 42,
	}

	expandEnvFlags(f)

	if f.Host != "broker.example.com" {
		t.Errorf("Host = %q, want %q", f.Host, "broker.example.com")
	}
	if f.User != "admin" {
		t.Errorf("User = %q, want %q", f.User, "admin")
	}
	if f.Static != "literal" {
		t.Errorf("Static = %q, want %q", f.Static, "literal")
	}
	if f.IntField != 42 {
		t.Errorf("IntField = %d, want 42", f.IntField)
	}
}

func TestExpandEnvFlagsNoEnvVar(t *testing.T) {
	t.Setenv("K4A_UNSET_VAR_12345", "")

	type flags struct {
		Val string
	}

	f := &flags{Val: "$K4A_NONEXISTENT_VAR_99999"}
	expandEnvFlags(f)

	// os.ExpandEnv replaces unknown vars with empty string.
	if f.Val != "" {
		t.Errorf("Val = %q, want empty", f.Val)
	}
}

// A `$` inside a value is literal: os.ExpandEnv turned `pa$$word` into `pa`
// plus whatever `$word` held. Only a whole-value reference expands, except
// for --brokers, which also expands embedded references.
func TestExpandEnvFlagsKeepsLiteralDollar(t *testing.T) {
	t.Setenv("word", "LEAK")
	t.Setenv("K4A_TEST_HOST", "broker.example.com")

	f := &struct {
		Brokers  string `long:"brokers"`
		Password string `long:"password"`
		Pattern  string `long:"pattern"`
	}{
		Brokers:  "$K4A_TEST_HOST:9092",
		Password: "pa$$word", // pragma: allowlist secret
		Pattern:  "^err$word",
	}
	expandEnvFlags(f)

	if f.Brokers != "broker.example.com:9092" {
		t.Errorf("Brokers = %q, want broker.example.com:9092", f.Brokers)
	}
	if f.Password != "pa$$word" { // pragma: allowlist secret
		t.Errorf("Password = %q, want it unchanged", f.Password)
	}
	if f.Pattern != "^err$word" {
		t.Errorf("Pattern = %q, want it unchanged", f.Pattern)
	}
}
