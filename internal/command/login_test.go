// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

// deviceIDP is an OIDC provider offering only the device flow, whose token
// endpoint approves the first poll.
func deviceIDP(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var logins atomic.Int32
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                        srv.URL,
			"token_endpoint":                srv.URL + "/token",
			"device_authorization_endpoint": srv.URL + "/device",
		})
	})
	mux.HandleFunc("POST /device", func(w http.ResponseWriter, _ *http.Request) {
		logins.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code": "dc", "user_code": "ABCD-EFGH",
			"verification_uri": srv.URL + "/activate", "expires_in": 60, "interval": 1,
		})
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "at", "refresh_token": "rt", "token_type": "Bearer", "expires_in": 3600,
		})
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &logins
}

// homeDirs points every user directory at a temp dir, so the default token
// store never touches the real one.
func homeDirs(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("AppData", filepath.Join(home, "AppData"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	return home
}

func TestLoginCommand_DeviceFlowCachesThenLogsOut(t *testing.T) {
	home := homeDirs(t)
	idp, logins := deviceIDP(t)
	cfg := writeCfg(t, `current-context: sso
contexts:
  sso:
    brokers: sso:9092
    auth: oauthbearer
    oauth-issuer: `+idp.URL+`
    oauth-client-id: k4a
    oauth-scopes: kafka
`)

	var out bytes.Buffer
	cmd := &LoginCommand{Out: &out}
	if code := cmd.Run([]string{"--config", cfg}); code != 0 {
		t.Fatalf("login = %d: %s", code, out.String())
	}
	if !strings.Contains(out.String(), "ABCD-EFGH") || !strings.Contains(out.String(), "Logged in.") {
		t.Fatalf("login printed %q", out.String())
	}
	dir := kafka.DefaultTokenStore().Dir
	if !strings.HasPrefix(dir, home) {
		t.Fatalf("token store %q escaped the test's home %q", dir, home)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("token cache = %v, %v; want one entry", entries, err)
	}

	out.Reset()
	if code := cmd.Run([]string{"--config", cfg}); code != 0 || !strings.Contains(out.String(), "Already logged in") {
		t.Fatalf("second login = %d, %q; want the cached token", code, out.String())
	}
	if n := logins.Load(); n != 1 {
		t.Fatalf("device logins = %d, want 1", n)
	}

	if code := cmd.Run([]string{"--config", cfg, "--logout"}); code != 0 {
		t.Fatalf("logout = %d", code)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("logout left %d cached tokens", len(entries))
	}
}

func TestLoginCommand_RefusesNonInteractiveAuth(t *testing.T) {
	homeDirs(t)
	cfg := writeCfg(t, `current-context: cc
contexts:
  cc:
    brokers: cc:9092
    auth: oauthbearer
    oauth-token-url: https://idp.example/token
    oauth-client-id: k4a
    oauth-client-secret: s
`)
	if code := (&LoginCommand{Out: &bytes.Buffer{}}).Run([]string{"--config", cfg}); code == 0 {
		t.Fatal("login succeeded for a client-credentials context")
	}
}

// A context's interactive-flow, extension and authzid fields reach the
// connection, and a flag replaces each one on its own.
func TestResolve_OIDCContextFieldsAndOverlay(t *testing.T) {
	t.Parallel()

	cfg := writeCfg(t, `current-context: sso
contexts:
  sso:
    brokers: sso:9092
    auth: oauthbearer
    oauth-issuer: https://idp.example
    oauth-client-id: k4a
    oauth-flow: browser
    oauth-redirect-port: "8765"
    oauth-authzid: ctx-principal
    oauth-extensions: logicalCluster=lkc-ctx
`)
	f := &ConnectionFlags{ConfigFile: cfg}
	f.OAuth.Extensions = "logicalCluster=lkc-flag,identityPoolId=pool-1"
	_, got, err := f.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	want := kafka.OAuthConfig{
		Issuer: "https://idp.example", ClientID: "k4a", Flow: kafka.FlowBrowser, RedirectPort: "8765",
		AuthzID: "ctx-principal", Extensions: "logicalCluster=lkc-flag,identityPoolId=pool-1",
	}
	if got.OAuth != want {
		t.Fatalf("oauth = %+v\nwant    %+v", got.OAuth, want)
	}
	if !got.OAuth.Interactive() {
		t.Fatal("an issuer context is not interactive")
	}
}
