// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildDecoder(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	none, err := buildDecoder(&flags{Brokers: "localhost:9092"})
	if err != nil || none != nil {
		t.Fatalf("no registry = %v, %v; want nil, nil", none, err)
	}

	direct, err := buildDecoder(&flags{Brokers: "localhost:9092", Registry: registryFlags{URL: "https://sr.example.com"}})
	if err != nil || direct == nil || direct.Identity() == "" {
		t.Fatalf("--schema-registry-url = %v, %v; want a decoder", direct, err)
	}

	// From a context's block, as the CLI resolves it, with a flag over it.
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	body := "current-context: prod\ncontexts:\n  prod:\n    brokers: b:9092\n" +
		"    schema-registry:\n      url: https://sr.example.com\n"
	if werr := os.WriteFile(cfg, []byte(body), 0o600); werr != nil {
		t.Fatal(werr)
	}
	fromCtx, err := buildDecoder(&flags{ConfigFile: cfg})
	if err != nil || fromCtx == nil {
		t.Fatalf("context block = %v, %v; want a decoder", fromCtx, err)
	}
	if fromCtx.Identity() != direct.Identity() {
		t.Errorf("the context's registry decodes as %s, the same URL by flag as %s", fromCtx.Identity(), direct.Identity())
	}
	over, err := buildDecoder(&flags{ConfigFile: cfg, Registry: registryFlags{URL: "https://other.example.com"}})
	if err != nil || over == nil || over.Identity() == direct.Identity() {
		t.Errorf("a flag did not override the context's registry: %v, %v", over, err)
	}

	if _, err := buildDecoder(&flags{Brokers: "b:9092", Registry: registryFlags{URL: "ftp://sr"}}); err == nil {
		t.Error("an unusable registry URL was accepted")
	}
}
