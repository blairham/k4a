// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// The regression these cover: with a context selected, only --auth, --profile
// and a non-default --region reached the connection; --username, --password,
// --cert, --key, --ca and -k were parsed and dropped, and --region us-east-1
// could not override a context in another region.

const overrideCfg = `current-context: prod
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

func overrideFlags() kafka.AuthConfig {
	return kafka.AuthConfig{
		Method:   kafka.AuthMTLS,
		Username: "flag-user",
		Password: "flag-pass", // pragma: allowlist secret
		CertFile: "/flag/cert.pem",
		KeyFile:  "/flag/key.pem",
		CAFile:   "/flag/ca.pem",
		Region:   "us-east-1", // the old default, used as a "not set" sentinel
		Profile:  "FlagProfile",
		Insecure: true,
	}
}

func assertAuth(t *testing.T, got, want kafka.AuthConfig) {
	t.Helper()
	if got != want {
		t.Errorf("auth = %+v\nwant   %+v", got, want)
	}
}

func TestConnectionFlagsResolve_FlagsOverrideContext(t *testing.T) {
	t.Parallel()
	want := overrideFlags()
	f := &ConnectionFlags{
		ConfigFile: writeCfg(t, overrideCfg),
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
	_, got, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	assertAuth(t, got, want)
}

func TestResolveConnection_FlagsOverrideContext(t *testing.T) {
	t.Parallel()
	want := overrideFlags()
	_, got, _, err := resolveConnection(UIFlags{
		ConfigFile: writeCfg(t, overrideCfg),
		Auth:       string(want.Method),
		Username:   want.Username,
		Password:   want.Password,
		CertFile:   want.CertFile,
		KeyFile:    want.KeyFile,
		CAFile:     want.CAFile,
		Region:     want.Region,
		Profile:    want.Profile,
		Insecure:   want.Insecure,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertAuth(t, got, want)
}

// No flags at all must leave every context value alone.
func TestResolve_NoFlagsKeepContext(t *testing.T) {
	t.Parallel()
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
	path := writeCfg(t, overrideCfg)

	_, got, err := (&ConnectionFlags{ConfigFile: path}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	assertAuth(t, got, want)

	_, got, _, err = resolveConnection(UIFlags{ConfigFile: path})
	if err != nil {
		t.Fatal(err)
	}
	assertAuth(t, got, want)
}

// A direct connection has no context to inherit a region from, so it still
// gets the IAM default.
func TestResolve_DirectDefaultsRegion(t *testing.T) {
	t.Parallel()
	_, got, err := (&ConnectionFlags{Brokers: "b:9092"}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if got.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", got.Region)
	}
}
