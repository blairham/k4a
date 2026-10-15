// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/blairham/k4a/internal/endpointauth"
)

// writeIndexConfig writes a config whose current context names a shared index
// with a CA and a token reference, and returns its path.
func writeIndexConfig(t *testing.T, extra string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := `current-context: c
contexts:
  c:
    brokers: b:9092
    auth: plaintext
    index-endpoint: ctx-index:443
    index-ca: /ctx/ca.crt
    index-token: $CTX_INDEX_TOKEN
` + extra
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResolveIndexFromContext(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "")
	t.Setenv("CTX_INDEX_TOKEN", "ctx-token")
	f := &ConnectionFlags{ConfigFile: writeIndexConfig(t, "")}

	ep, opts, err := f.ResolveIndex()
	if err != nil {
		t.Fatal(err)
	}
	want := endpointauth.ClientOptions{CAFile: "/ctx/ca.crt", Token: "ctx-token"}
	if ep != "ctx-index:443" || opts != want {
		t.Errorf("got %q %+v, want ctx-index:443 %+v", ep, opts, want)
	}
}

func TestResolveIndexFlagsOverlayContext(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "")
	t.Setenv("CTX_INDEX_TOKEN", "ctx-token")
	tokenFile := filepath.Join(t.TempDir(), "tok")
	if err := os.WriteFile(tokenFile, []byte("file-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f := &ConnectionFlags{ConfigFile: writeIndexConfig(t, ""), IndexCA: "/flag/ca.crt", IndexToken: tokenFile}

	_, opts, err := f.ResolveIndex()
	if err != nil {
		t.Fatal(err)
	}
	if opts.CAFile != "/flag/ca.crt" || opts.Token != "file-token" {
		t.Errorf("flags did not override the context: %+v", opts)
	}
}

func TestResolveIndexEnvTokenOverridesContext(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "env-token")
	t.Setenv("CTX_INDEX_TOKEN", "ctx-token")
	f := &ConnectionFlags{ConfigFile: writeIndexConfig(t, "")}

	_, opts, err := f.ResolveIndex()
	if err != nil {
		t.Fatal(err)
	}
	if opts.Token != "env-token" {
		t.Errorf("token = %q, want the environment's", opts.Token)
	}
}

// A context's token must never be sent to an endpoint named on the command
// line: that host is not the context's daemon.
func TestResolveIndexFlagEndpointIgnoresContextCredentials(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "env-token")
	t.Setenv("CTX_INDEX_TOKEN", "ctx-token")
	f := &ConnectionFlags{ConfigFile: writeIndexConfig(t, ""), IndexEndpoint: "other:9500", IndexInsecure: true}

	ep, opts, err := f.ResolveIndex()
	if err != nil {
		t.Fatal(err)
	}
	if ep != "other:9500" || opts != (endpointauth.ClientOptions{Plaintext: true}) {
		t.Errorf("got %q %+v, want other:9500 with only the flags' plaintext setting", ep, opts)
	}
}

func TestBuildRemoteIndexRefusesTokenOverPlaintext(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "")
	t.Setenv("CTX_INDEX_TOKEN", "ctx-token")
	f := &ConnectionFlags{ConfigFile: writeIndexConfig(t, "    index-insecure: true\n")}

	c, err := f.BuildRemoteIndex()
	if c != nil {
		c.Close() //nolint:errcheck,gosec // test
	}
	if !errors.Is(err, endpointauth.ErrTokenOverPlaintext) {
		t.Errorf("err = %v, want ErrTokenOverPlaintext", err)
	}
}

func TestResolveIndexBrokenConfigIsLocalOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(":\n\t- not yaml"), 0o600); err != nil {
		t.Fatal(err)
	}
	ep, _, err := (&ConnectionFlags{ConfigFile: path}).ResolveIndex()
	if ep != "" || err != nil {
		t.Errorf("got %q, %v; want no endpoint and no error", ep, err)
	}
}
