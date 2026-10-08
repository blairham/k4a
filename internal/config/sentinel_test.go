// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"testing"
)

// Resolve's two failures mean different things to the caller: one is
// recoverable inside the TUI (open the picker), the other is a bad argument.
// They were indistinguishable when both were bare fmt.Errorf strings, which
// is how the recoverable one ended up taking the fatal path.

func TestResolveDistinguishesNothingCurrentFromNotFound(t *testing.T) {
	t.Parallel()

	cfg := &Config{Contexts: map[string]Context{
		"staging-us":    {Brokers: "stg:9098"},
		"production-us": {Brokers: "prod:9098"},
	}}

	_, err := cfg.Resolve("")
	if !errors.Is(err, ErrNoCurrentContext) {
		t.Errorf("Resolve(\"\") = %v, want ErrNoCurrentContext", err)
	}
	if errors.Is(err, ErrContextNotFound) {
		t.Error("an unset current-context must not read as a missing context")
	}

	_, err = cfg.Resolve("nope")
	if !errors.Is(err, ErrContextNotFound) {
		t.Errorf("Resolve(\"nope\") = %v, want ErrContextNotFound", err)
	}
	if errors.Is(err, ErrNoCurrentContext) {
		t.Error("a missing context must not read as an unset current-context")
	}
}

func TestResolveStillFindsAPresentContext(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		CurrentContext: "staging-us",
		Contexts:       map[string]Context{"staging-us": {Brokers: "stg:9098"}},
	}
	ctx, err := cfg.Resolve("")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if ctx.Brokers != "stg:9098" {
		t.Errorf("brokers = %q", ctx.Brokers)
	}
}

// The names go in front of a human who has to spell one back at us, so map
// order would reshuffle the list on every run.
func TestContextNamesAreSorted(t *testing.T) {
	t.Parallel()

	cfg := &Config{Contexts: map[string]Context{
		"staging-us":    {},
		"production-us": {},
		"production-eu": {},
	}}
	if got, want := cfg.ContextNames(), "production-eu, production-us, staging-us"; got != want {
		t.Errorf("ContextNames() = %q, want %q", got, want)
	}
}
