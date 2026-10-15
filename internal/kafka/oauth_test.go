// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	testClientID     = "k4a-client"
	testClientSecret = "s3cr3t-value" //nolint:gosec // test fixture
)

// tokenServer is a client-credentials endpoint that issues a numbered token
// per request with the given lifetime, and checks the request is a proper
// client-credentials grant carrying the configured client and scopes.
func tokenServer(t *testing.T, expiresIn, status int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse form: %v", err)
		}
		if got := r.PostForm.Get("grant_type"); got != "client_credentials" {
			t.Errorf("grant_type = %q", got)
		}
		if got := r.PostForm.Get("scope"); got != "kafka read" {
			t.Errorf("scope = %q, want %q", got, "kafka read")
		}
		id, secret, ok := r.BasicAuth()
		if !ok {
			id, secret = r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
		}
		if id != testClientID || secret != testClientSecret {
			t.Errorf("client credentials = %q/%q", id, secret)
		}
		if status != http.StatusOK {
			http.Error(w, `{"error":"invalid_client"}`, status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": fmt.Sprintf("token-%d", n),
			"token_type":   "Bearer",
			"expires_in":   expiresIn,
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func clientCredentials(url string) OAuthConfig {
	return OAuthConfig{
		TokenURL:     url,
		ClientID:     testClientID,
		ClientSecret: testClientSecret,
		Scopes:       "kafka, read",
	}
}

func TestClientCredentials_CachesUntilExpiry(t *testing.T) {
	t.Parallel()

	srv, hits := tokenServer(t, 3600, http.StatusOK)
	src, err := newTokenSource(clientCredentials(srv.URL))
	if err != nil {
		t.Fatalf("newTokenSource: %v", err)
	}
	for range 3 {
		tok, err := src(t.Context())
		if err != nil {
			t.Fatalf("token: %v", err)
		}
		if tok != "token-1" {
			t.Fatalf("token = %q, want the cached token-1", tok)
		}
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("token endpoint hit %d times, want 1 (a long-lived token is cached)", n)
	}
}

// A token inside oauth2's expiry margin is treated as expired, so each
// handshake fetches a new one: this is the refresh path.
func TestClientCredentials_RefreshesExpiringToken(t *testing.T) {
	t.Parallel()

	srv, hits := tokenServer(t, 1, http.StatusOK)
	src, err := newTokenSource(clientCredentials(srv.URL))
	if err != nil {
		t.Fatalf("newTokenSource: %v", err)
	}
	first, err := src(t.Context())
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	second, err := src(t.Context())
	if err != nil {
		t.Fatalf("second token: %v", err)
	}
	if first == second || hits.Load() != 2 {
		t.Fatalf("tokens %q, %q after %d fetches; want a fresh token per fetch", first, second, hits.Load())
	}
}

func TestClientCredentials_ErrorDoesNotLeakSecret(t *testing.T) {
	t.Parallel()

	srv, _ := tokenServer(t, 3600, http.StatusUnauthorized)
	src, err := newTokenSource(clientCredentials(srv.URL + "/token?api_key=hunter2"))
	if err != nil {
		t.Fatalf("newTokenSource: %v", err)
	}
	_, err = src(t.Context())
	if err == nil {
		t.Fatal("expected an error from a 401 token endpoint")
	}
	for _, secret := range []string{testClientSecret, "hunter2"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

// The mechanism presents the source's token in the RFC 7628 initial response.
func TestOAuthMechanism_PresentsToken(t *testing.T) {
	t.Parallel()

	srv, _ := tokenServer(t, 3600, http.StatusOK)
	mech, err := oauthMechanism(clientCredentials(srv.URL))
	if err != nil {
		t.Fatalf("oauthMechanism: %v", err)
	}
	_, init, err := mech.Authenticate(context.Background(), "broker:9092")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if !strings.Contains(string(init), "auth=Bearer token-1\x01") {
		t.Fatalf("initial response %q does not carry the fetched token", init)
	}
}

func TestTokenFile_ReReadEachHandshake(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := newTokenSource(OAuthConfig{TokenFile: path})
	if err != nil {
		t.Fatalf("newTokenSource: %v", err)
	}
	if tok, err := src(t.Context()); err != nil || tok != "one" {
		t.Fatalf("first read = %q, %v", tok, err)
	}
	if err := os.WriteFile(path, []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if tok, err := src(t.Context()); err != nil || tok != "two" {
		t.Fatalf("after rotation = %q, %v; want the rotated token", tok, err)
	}
	if err := os.WriteFile(path, []byte("  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := src(t.Context()); err == nil {
		t.Fatal("expected an error for an empty token file")
	}
}

func TestNewTokenSource_Validation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  OAuthConfig
		ok   bool
	}{
		{"static", OAuthConfig{Token: "t"}, true},
		{"none", OAuthConfig{}, false},
		{"two sources", OAuthConfig{Token: "t", TokenFile: "/f"}, false},
		{"url without client", OAuthConfig{TokenURL: "https://idp.example/token"}, false},
		{"https", clientCredentials("https://idp.example/token"), true},
		{"http loopback", clientCredentials("http://127.0.0.1:8080/token"), true},
		{"http localhost", clientCredentials("http://localhost/token"), true},
		{"http remote", clientCredentials("http://idp.example/token"), false},
		{"userinfo", clientCredentials("https://u:p@idp.example/token"), false},
		{"no host", clientCredentials("idp.example/token"), false},
	} {
		_, err := newTokenSource(tc.cfg)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
		if err != nil && strings.Contains(err.Error(), ":p@") {
			t.Errorf("%s: error leaks URL credentials: %v", tc.name, err)
		}
	}
}
