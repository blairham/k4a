// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
)

// The daemon takes the same OAUTHBEARER and --no-tls settings as the CLI, by
// flag or by the KAFKA_OAUTH_* environment a Helm chart would set.
func TestResolveConnection_DirectOAuthAndNoTLS(t *testing.T) {
	t.Setenv("KAFKA_OAUTH_CLIENT_SECRET", "env-secret")

	var f flags
	args := []string{
		"--allow", "orders.*", "-b", "kafka:9092", "-a", "oauthbearer", "--no-tls",
		"--oauth-token-url", "https://idp.example/token", "--oauth-client-id", "k4a-index",
	}
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, got, err := resolveConnection(&f)
	if err != nil {
		t.Fatalf("resolveConnection: %v", err)
	}
	want := kafka.OAuthConfig{TokenURL: "https://idp.example/token", ClientID: "k4a-index", ClientSecret: "env-secret"}
	if got.Method != kafka.AuthOAuthBearer || !got.NoTLS || got.OAuth != want {
		t.Fatalf("auth = %+v", got)
	}
	if _, err := kafka.NewClientOpts(got, "kafka:9092"); err != nil {
		t.Fatalf("NewClientOpts: %v", err)
	}
}

const daemonOAuthCfg = `current-context: dev
contexts:
  dev:
    brokers: dev:9092
    auth: oauthbearer
    oauth-token-url: https://idp.example/token
    oauth-client-id: ctx-client
    oauth-client-secret: ctx-secret
`

// Over a context, each OAuth flag overrides its own field and --no-tls turns
// TLS off; with no flags the context, and its TLS, are untouched.
func TestResolveConnection_OAuthFlagsOverlayContext(t *testing.T) {
	t.Parallel()

	path := writeDaemonCfg(t, daemonOAuthCfg)
	f := &flags{ConfigFile: path, NoTLS: true}
	f.OAuth.ClientSecret = "flag-secret"
	_, got, err := resolveConnection(f)
	if err != nil {
		t.Fatalf("resolveConnection: %v", err)
	}
	want := kafka.OAuthConfig{TokenURL: "https://idp.example/token", ClientID: "ctx-client", ClientSecret: "flag-secret"}
	if got.Method != kafka.AuthOAuthBearer || got.OAuth != want || !got.NoTLS {
		t.Fatalf("auth = %+v", got)
	}

	_, kept, err := resolveConnection(&flags{ConfigFile: path})
	if err != nil {
		t.Fatalf("resolveConnection: %v", err)
	}
	if kept.NoTLS || kept.OAuth.ClientSecret != "ctx-secret" {
		t.Fatalf("auth without flags = %+v", kept)
	}
}

// The daemon passes SASL extensions and an authorization ID through, by flag
// or env, but refuses a context that would need a person to log in.
func TestResolveConnection_ExtensionsAndNoInteractiveLogin(t *testing.T) {
	t.Setenv("KAFKA_OAUTH_EXTENSIONS", "logicalCluster=lkc-1")

	var f flags
	args := []string{
		"--allow", "orders.*", "-b", "kafka:9092", "-a", "oauthbearer", "--oauth-token", "t",
		"--oauth-authzid", "svc",
	}
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs(args); err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, got, err := resolveConnection(&f)
	if err != nil {
		t.Fatalf("resolveConnection: %v", err)
	}
	if got.OAuth.Extensions != "logicalCluster=lkc-1" || got.OAuth.AuthzID != "svc" {
		t.Fatalf("oauth = %+v", got.OAuth)
	}

	path := writeDaemonCfg(t, `current-context: sso
contexts:
  sso:
    brokers: sso:9092
    auth: oauthbearer
    oauth-issuer: https://idp.example
    oauth-client-id: k4a
`)
	if _, _, err := resolveConnection(&flags{ConfigFile: path}); err == nil {
		t.Fatal("resolveConnection accepted an interactive OIDC context")
	}
}
