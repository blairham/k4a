// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// The OAuth and no-tls fields sit flat in a context, beside password, and the
// secrets resolve a whole-value $VAR exactly as password does.
func TestLoadOAuthAndNoTLSContext(t *testing.T) {
	t.Setenv("TEST_OAUTH_SECRET", "from-env")

	path := filepath.Join(t.TempDir(), "config.yaml")
	const yaml = `current-context: dev
contexts:
  dev:
    brokers: localhost:9092
    auth: oauthbearer
    no-tls: true
    oauth-token-url: https://idp.example/token
    oauth-client-id: k4a
    oauth-client-secret: ${TEST_OAUTH_SECRET}
    oauth-scopes: kafka,read
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	ctx, err := cfg.Resolve("")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	got := ctx.ToAuthConfig()
	want := kafka.AuthConfig{
		Method: kafka.AuthOAuthBearer,
		NoTLS:  true,
		OAuth: kafka.OAuthConfig{
			TokenURL:     "https://idp.example/token",
			ClientID:     "k4a",
			ClientSecret: "from-env",
			Scopes:       "kafka,read",
		},
	}
	if got != want {
		t.Fatalf("ToAuthConfig = %+v\nwant          %+v", got, want)
	}

	// A re-save (the TUI persists current-context) must keep the fields,
	// and must keep the $VAR reference rather than the resolved secret.
	if err = Save(path, cfg); err != nil {
		t.Fatalf("Save: %v", err)
	}
	again, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if c := again.Contexts["dev"]; c.OAuth.ClientSecret != "${TEST_OAUTH_SECRET}" || !c.NoTLS || c.OAuth.TokenURL == "" {
		t.Fatalf("round trip lost fields: %+v", c)
	}
}
