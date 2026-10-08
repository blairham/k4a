// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

func TestIsCleanShutdown(t *testing.T) {
	t.Parallel()

	// io.EOF sentinel.
	if !isCleanShutdown(context.Background(), io.EOF) {
		t.Error("io.EOF should be clean shutdown")
	}

	// Substring fallback for SDK-wrapped EOF.
	wrapped := errors.New("read error: EOF received from client")
	if !isCleanShutdown(context.Background(), wrapped) {
		t.Error("wrapped EOF string should be clean shutdown")
	}

	// Real error.
	if isCleanShutdown(context.Background(), errors.New("kafka connect: refused")) {
		t.Error("genuine error should NOT be clean shutdown")
	}

	// Canceled context — treated as clean shutdown regardless of err.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !isCleanShutdown(ctx, errors.New("anything")) {
		t.Error("canceled context should be clean shutdown")
	}
}

func TestBuildMCPParams(t *testing.T) {
	t.Parallel()

	p, err := buildMCPParams(mcpSearchInput{
		Topic: "orders",
		Query: "needle",
		Scope: "key+value",
	})
	if err != nil {
		t.Fatalf("buildMCPParams: %v", err)
	}
	if p.Pattern == nil || !p.Pattern.MatchString("xneedley") {
		t.Error("Pattern did not match")
	}
	if p.Scope != kafka.ScopeKey|kafka.ScopeValue {
		t.Errorf("Scope = %d, want key+value", p.Scope)
	}
}

func TestBuildMCPParamsLiteralFallback(t *testing.T) {
	t.Parallel()

	p, err := buildMCPParams(mcpSearchInput{Query: "foo[bar"})
	if err != nil {
		t.Fatalf("buildMCPParams: %v", err)
	}
	if !p.Pattern.MatchString("xx foo[bar yy") {
		t.Error("literal fallback did not match")
	}
}

func TestBuildMCPParamsInvalidScope(t *testing.T) {
	t.Parallel()

	if _, err := buildMCPParams(mcpSearchInput{Query: "x", Scope: "bogus"}); err == nil {
		t.Error("invalid scope should error")
	}
}

func TestBuildMCPParamsInvalidTime(t *testing.T) {
	t.Parallel()

	if _, err := buildMCPParams(mcpSearchInput{Query: "x", Since: "not-a-time"}); err == nil {
		t.Error("invalid since should error")
	}
	if _, err := buildMCPParams(mcpSearchInput{Query: "x", Until: "not-a-time"}); err == nil {
		t.Error("invalid until should error")
	}
}
