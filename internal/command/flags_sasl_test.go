// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

// The OAuth flags live in a nested struct; this proves go-flags still sees
// them (an unscanned nested struct would parse as "unknown flag"), and that a
// --brokers connection with only a token means oauthbearer.
func TestConnectionFlags_ParseOAuthAndNoTLS(t *testing.T) {
	t.Setenv("TEST_OAUTH_TOKEN", "from-env")

	var f ConnectionFlags
	args := []string{"-b", "localhost:9092", "--oauth-token", "$TEST_OAUTH_TOKEN", "--no-tls"}
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	expandEnvFlags(&f)
	_, got, err := f.Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Method != kafka.AuthOAuthBearer || !got.NoTLS || got.OAuth.Token != "from-env" {
		t.Fatalf("auth = %+v, want inferred oauthbearer, no-tls, token from env", got)
	}
}

func TestUIFlags_ParseOAuthAndNoTLS(t *testing.T) {
	t.Parallel()

	var f UIFlags
	args := []string{
		"-b", "h:9092", "-a", "plain", "--username", "u", "--password", "p", "--no-tls",
		"--oauth-scopes", "a,b",
	}
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, got, _, err := resolveConnection(f)
	if err != nil {
		t.Fatalf("resolveConnection: %v", err)
	}
	if got.Method != kafka.AuthPlain || !got.NoTLS || got.OAuth.Scopes != "a,b" {
		t.Fatalf("auth = %+v", got)
	}
}

const saslCtxCfg = `current-context: dev
contexts:
  dev:
    brokers: dev:9092
    auth: oauthbearer
    oauth-token-url: https://idp.example/token
    oauth-client-id: ctx-client
    oauth-client-secret: ctx-secret
`

// Flags override a context's OAuth settings field by field, and --no-tls
// turns TLS off for a context that did not.
func TestResolve_OAuthFlagsOverlayContext(t *testing.T) {
	t.Parallel()

	f := &ConnectionFlags{ConfigFile: writeCfg(t, saslCtxCfg), NoTLS: true}
	f.OAuth.ClientSecret = "flag-secret"
	_, got, err := f.Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := kafka.OAuthConfig{
		TokenURL:     "https://idp.example/token",
		ClientID:     "ctx-client",
		ClientSecret: "flag-secret",
	}
	if got.Method != kafka.AuthOAuthBearer || got.OAuth != want || !got.NoTLS {
		t.Fatalf("auth = %+v, want oauthbearer with %+v and no-tls", got, want)
	}

	// No flags: the context is untouched and keeps TLS.
	_, kept, err := (&ConnectionFlags{ConfigFile: f.ConfigFile}).Resolve()
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if kept.NoTLS || kept.OAuth.ClientSecret != "ctx-secret" {
		t.Fatalf("auth without flags = %+v", kept)
	}
}

func TestInferAuthMethod_SASLAndOAuth(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		want kafka.AuthMethod
		auth kafka.AuthConfig
	}{
		{kafka.AuthSCRAM, kafka.AuthConfig{Username: "u", Password: "p"}},
		{kafka.AuthOAuthBearer, kafka.AuthConfig{OAuth: kafka.OAuthConfig{Token: "t"}}},
		{kafka.AuthOAuthBearer, kafka.AuthConfig{OAuth: kafka.OAuthConfig{TokenFile: "/t"}}},
		{kafka.AuthOAuthBearer, kafka.AuthConfig{OAuth: kafka.OAuthConfig{ClientID: "c"}}},
		// --no-tls alone carries no credentials, so it implies no method.
		{kafka.AuthPlaintext, kafka.AuthConfig{NoTLS: true}},
	} {
		if got := kafka.InferAuthMethod(tc.auth); got != tc.want {
			t.Errorf("InferAuthMethod(%+v) = %s, want %s", tc.auth, got, tc.want)
		}
	}
}
