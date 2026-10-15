// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// The daemon resolves its connection itself rather than through
// internal/command, so it kept the bug #63 fixed there: with a context
// selected only --auth, --profile and a non-default --region reached the
// connection, and --region us-east-1 could not override a context in another
// region because us-east-1 doubled as the "not set" sentinel.

const daemonOverrideCfg = `current-context: prod
contexts:
  prod:
    brokers: prod:9096
    auth: scram
    username: ctx-user
    password: ctx-pass
    cert: /ctx/cert.pem
    key: /ctx/key.pem
    ca: /ctx/ca.pem
    region: us-west-2
    profile: CtxProfile
`

func writeDaemonCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestResolveConnectionFlagsOverrideContext(t *testing.T) {
	t.Parallel()
	want := kafka.AuthConfig{
		Method:   kafka.AuthMTLS,
		Username: "flag-user",
		Password: "flag-pass", // pragma: allowlist secret
		CertFile: "/flag/cert.pem",
		KeyFile:  "/flag/key.pem",
		CAFile:   "/flag/ca.pem",
		Region:   "us-east-1", // the old default, once a "not set" sentinel
		Profile:  "FlagProfile",
		Insecure: true,
	}
	f := &flags{
		ConfigFile: writeDaemonCfg(t, daemonOverrideCfg),
		Auth:       string(want.Method),
		Username:   want.Username,
		Password:   want.Password,
		CertFile:   want.CertFile,
		KeyFile:    want.KeyFile,
		CAFile:     want.CAFile,
		Region:     want.Region,
		Profile:    want.Profile,
		Insecure:   want.Insecure,
	}
	brokers, got, err := resolveConnection(f)
	if err != nil {
		t.Fatal(err)
	}
	if brokers != "prod:9096" {
		t.Errorf("brokers = %q, want prod:9096", brokers)
	}
	if got != want {
		t.Errorf("auth = %+v\nwant   %+v", got, want)
	}
}

func TestResolveConnectionNoFlagsKeepsContext(t *testing.T) {
	t.Parallel()
	_, got, err := resolveConnection(&flags{ConfigFile: writeDaemonCfg(t, daemonOverrideCfg)})
	if err != nil {
		t.Fatal(err)
	}
	want := kafka.AuthConfig{
		Method:   kafka.AuthSCRAM,
		Username: "ctx-user",
		Password: "ctx-pass", // pragma: allowlist secret
		CertFile: "/ctx/cert.pem",
		KeyFile:  "/ctx/key.pem",
		CAFile:   "/ctx/ca.pem",
		Region:   "us-west-2",
		Profile:  "CtxProfile",
	}
	if got != want {
		t.Errorf("auth = %+v\nwant   %+v", got, want)
	}
}

func TestResolveConnectionDirectDefaults(t *testing.T) {
	t.Parallel()
	_, got, err := resolveConnection(&flags{Brokers: "b:9092"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != kafka.AuthSCRAM || got.Region != "us-east-1" {
		t.Errorf("direct defaults = method %q region %q, want scram us-east-1", got.Method, got.Region)
	}
}
