// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// memStore is a TokenStore that never touches the disk.
type memStore struct {
	m  map[string]*oauth2.Token
	mu sync.Mutex
}

func newMemStore() *memStore { return &memStore{m: map[string]*oauth2.Token{}} }

func (s *memStore) Load(key string) (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.m[key]; ok {
		c := *t
		return &c, nil
	}
	return nil, nil //nolint:nilnil // not cached
}

func (s *memStore) Save(key string, tok *oauth2.Token) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := *tok
	s.m[key] = &c
	return nil
}

func (s *memStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}

func (s *memStore) only(t *testing.T) *oauth2.Token {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.m) != 1 {
		t.Fatalf("store holds %d tokens, want 1", len(s.m))
	}
	for _, tok := range s.m {
		return tok
	}
	return nil
}

// fakeIDP is an OIDC provider: discovery, the device and authorization
// endpoints, and a token endpoint for the device, code and refresh grants.
type fakeIDP struct {
	srv *httptest.Server
	// authError, when set, is returned from the authorize endpoint.
	authError string
	challenge string
	// refreshStatus, when set, fails every refresh grant with it.
	refreshStatus int
	mu            sync.Mutex
	issued        atomic.Int32
	refreshes     atomic.Int32
	devices       atomic.Int32
}

const (
	testDeviceCode = "dev-code-1"
	testUserCode   = "WDJB-MJHT"
	testAuthCode   = "auth-code-1"
)

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	idp := &fakeIDP{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		base := idp.srv.URL
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                        base,
			"authorization_endpoint":        base + "/authorize",
			"token_endpoint":                base + "/token",
			"device_authorization_endpoint": base + "/device",
		})
	})
	mux.HandleFunc("POST /device", func(w http.ResponseWriter, r *http.Request) {
		idp.devices.Add(1)
		_ = r.ParseForm()
		if r.PostForm.Get("client_id") != testClientID {
			t.Errorf("device client_id = %q", r.PostForm.Get("client_id"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":      testDeviceCode,
			"user_code":        testUserCode,
			"verification_uri": idp.srv.URL + "/activate\x1b[2J",
			"expires_in":       60,
			"interval":         1,
		})
	})
	mux.HandleFunc("GET /authorize", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("code_challenge_method") != "S256" || q.Get("code_challenge") == "" {
			t.Errorf("authorize request without a PKCE S256 challenge: %v", q)
		}
		idp.mu.Lock()
		idp.challenge = q.Get("code_challenge")
		idp.mu.Unlock()
		back := url.Values{"state": {q.Get("state")}}
		if idp.authError != "" {
			back.Set("error", idp.authError)
		} else {
			back.Set("code", testAuthCode)
		}
		http.Redirect(w, r, q.Get("redirect_uri")+"?"+back.Encode(), http.StatusFound)
	})
	mux.HandleFunc("POST /token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f := r.PostForm
		switch f.Get("grant_type") {
		case "urn:ietf:params:oauth:grant-type:device_code":
			if f.Get("device_code") != testDeviceCode {
				t.Errorf("device_code = %q", f.Get("device_code"))
			}
		case "authorization_code":
			idp.mu.Lock()
			challenge := idp.challenge
			idp.mu.Unlock()
			sum := sha256.Sum256([]byte(f.Get("code_verifier")))
			if f.Get("code") != testAuthCode || base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
				http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
				return
			}
		case "refresh_token":
			idp.refreshes.Add(1)
			if idp.refreshStatus != 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(idp.refreshStatus)
				_, _ = io.WriteString(w, `{"error":"invalid_grant"}`)
				return
			}
		default:
			t.Errorf("unexpected grant %q", f.Get("grant_type"))
		}
		n := idp.issued.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  fmt.Sprintf("access-%d", n),
			"refresh_token": fmt.Sprintf("refresh-%d", n),
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	})
	idp.srv = httptest.NewServer(mux)
	t.Cleanup(idp.srv.Close)
	return idp
}

func (idp *fakeIDP) config(flow string, store TokenStore) OAuthConfig {
	return OAuthConfig{Issuer: idp.srv.URL, ClientID: testClientID, Flow: flow, Scopes: "openid,kafka", Store: store}
}

// followBrowser stands in for the browser: it follows the authorization URL
// through the IdP's redirect to k4a's loopback listener.
func followBrowser(t *testing.T) func(string) error {
	t.Helper()
	return func(u string) error {
		resp, err := http.Get(u) //nolint:gosec,noctx // the test's own IdP and listener
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}
}

func handshakeToken(t *testing.T, cfg OAuthConfig) (string, error) {
	t.Helper()
	mech, err := oauthMechanism(cfg)
	if err != nil {
		t.Fatalf("oauthMechanism: %v", err)
	}
	_, init, err := mech.Authenticate(t.Context(), "broker:9092")
	if err != nil {
		return "", err
	}
	return string(init), nil
}

func TestDeviceLogin_CachesTokenForHandshake(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	store := newMemStore()
	cfg := idp.config(FlowDevice, store)

	if _, err := handshakeToken(t, cfg); !errors.Is(err, ErrOAuthLoginRequired) {
		t.Fatalf("handshake before login: err = %v, want ErrOAuthLoginRequired", err)
	}

	var out bytes.Buffer
	prompted, err := OAuthLogin(t.Context(), cfg, LoginOptions{Out: &out})
	if err != nil || !prompted {
		t.Fatalf("OAuthLogin = %v, %v; want a login", prompted, err)
	}
	if !strings.Contains(out.String(), testUserCode) || !strings.Contains(out.String(), "/activate") {
		t.Fatalf("instructions %q lack the code or the URL", out.String())
	}
	if strings.Contains(out.String(), "\x1b") {
		t.Fatalf("instructions %q pass an IdP escape sequence to the terminal", out.String())
	}
	if tok := store.only(t); tok.AccessToken != "access-1" || tok.RefreshToken != "refresh-1" {
		t.Fatalf("cached token = %+v", tok)
	}

	// A later process: the cached token is used without another login.
	init, err := handshakeToken(t, cfg)
	if err != nil || !strings.Contains(init, "auth=Bearer access-1\x01") {
		t.Fatalf("handshake = %q, %v; want the cached token", init, err)
	}
	if prompted, err := OAuthLogin(t.Context(), cfg, LoginOptions{Out: io.Discard}); err != nil || prompted {
		t.Fatalf("second OAuthLogin = %v, %v; want the cache, not a login", prompted, err)
	}
	if n := idp.devices.Load(); n != 1 {
		t.Fatalf("device endpoint hit %d times, want 1", n)
	}
}

func TestOIDC_RefreshesExpiredToken(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	store := newMemStore()
	cfg := idp.config(FlowDevice, store)
	key := tokenCacheKey(cfg)
	_ = store.Save(key, &oauth2.Token{
		AccessToken: "stale", RefreshToken: "refresh-0", Expiry: time.Now().Add(-time.Minute),
	})

	init, err := handshakeToken(t, cfg)
	if err != nil || !strings.Contains(init, "auth=Bearer access-1\x01") {
		t.Fatalf("handshake = %q, %v; want the refreshed token", init, err)
	}
	if idp.refreshes.Load() != 1 {
		t.Fatalf("refreshes = %d, want 1", idp.refreshes.Load())
	}
	// The rotated refresh token is what the next process must use.
	if tok := store.only(t); tok.RefreshToken != "refresh-1" {
		t.Fatalf("cached refresh token = %q, want the rotated one", tok.RefreshToken)
	}
}

func TestOIDC_RefusedRefreshMeansLogin(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	idp.refreshStatus = http.StatusBadRequest
	store := newMemStore()
	cfg := idp.config(FlowDevice, store)
	_ = store.Save(tokenCacheKey(cfg), &oauth2.Token{
		AccessToken: "stale", RefreshToken: "revoked", Expiry: time.Now().Add(-time.Minute),
	})

	if _, err := handshakeToken(t, cfg); !errors.Is(err, ErrOAuthLoginRequired) {
		t.Fatalf("err = %v, want ErrOAuthLoginRequired", err)
	}
	if tok, _ := store.Load(tokenCacheKey(cfg)); tok != nil {
		t.Fatalf("dead refresh token still cached: %+v", tok)
	}
}

func TestBrowserLogin_PKCE(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	store := newMemStore()
	cfg := idp.config(FlowBrowser, store)

	var opened string
	open := func(u string) error {
		opened = u
		// A request without the login's state must not end the wait.
		redirect, _ := url.Parse(u)
		cb := redirect.Query().Get("redirect_uri") + "?code=forged&state=wrong"
		resp, err := http.Get(cb) //nolint:gosec,noctx // the test's own listener
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("forged callback status = %d, want 400", resp.StatusCode)
		}
		return followBrowser(t)(u)
	}
	var out bytes.Buffer
	if _, err := OAuthLogin(t.Context(), cfg, LoginOptions{Out: &out, OpenBrowser: open}); err != nil {
		t.Fatalf("OAuthLogin: %v", err)
	}
	if !strings.Contains(opened, "redirect_uri=http%3A%2F%2F127.0.0.1%3A") {
		t.Fatalf("authorization URL %q does not redirect to a loopback listener", opened)
	}
	if !strings.Contains(out.String(), opened) {
		t.Fatalf("the authorization URL is not printed for a browser that does not open: %q", out.String())
	}
	if tok := store.only(t); tok.AccessToken != "access-1" {
		t.Fatalf("cached token = %+v", tok)
	}
}

func TestBrowserLogin_IdPRefusal(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	idp.authError = "access_denied"
	cfg := idp.config(FlowBrowser, newMemStore())
	_, err := OAuthLogin(t.Context(), cfg, LoginOptions{Out: io.Discard, OpenBrowser: followBrowser(t)})
	if err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("err = %v, want the IdP's refusal", err)
	}
}

func TestOIDC_ForceLogsInAgainAndLogoutForgets(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	store := newMemStore()
	cfg := idp.config(FlowBrowser, store)
	opts := LoginOptions{Out: io.Discard, OpenBrowser: followBrowser(t)}
	if _, err := OAuthLogin(t.Context(), cfg, opts); err != nil {
		t.Fatal(err)
	}
	opts.Force = true
	if prompted, err := OAuthLogin(t.Context(), cfg, opts); err != nil || !prompted {
		t.Fatalf("forced login = %v, %v", prompted, err)
	}
	if tok := store.only(t); tok.AccessToken != "access-2" {
		t.Fatalf("cached token = %q after a forced login, want access-2", tok.AccessToken)
	}
	if err := OAuthLogout(cfg); err != nil {
		t.Fatal(err)
	}
	if tok, _ := store.Load(tokenCacheKey(cfg)); tok != nil {
		t.Fatalf("logout left %+v cached", tok)
	}
}

func TestDiscovery_RefusesAnotherIssuer(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer": "https://elsewhere.example", "token_endpoint": "https://elsewhere.example/token",
		})
	}))
	t.Cleanup(srv.Close)
	cfg := OAuthConfig{Issuer: srv.URL, ClientID: testClientID, Store: newMemStore()}
	_, err := OAuthLogin(t.Context(), cfg, LoginOptions{Out: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "different issuer") {
		t.Fatalf("err = %v, want an issuer mismatch", err)
	}
}

func TestCheckOAuthSource_Flows(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  OAuthConfig
		ok   bool
	}{
		{"issuer defaults to device", OAuthConfig{Issuer: "https://idp.example", ClientID: "c"}, true},
		{"issuer browser", OAuthConfig{Issuer: "https://idp.example", ClientID: "c", Flow: FlowBrowser}, true},
		{"issuer and token url", OAuthConfig{Issuer: "https://idp.example", TokenURL: "https://idp.example/t"}, false},
		{"device without issuer", OAuthConfig{TokenURL: "https://idp.example/t", Flow: FlowDevice}, false},
		{"client credentials", OAuthConfig{TokenURL: "https://idp.example/t", Flow: FlowClientCredentials}, true},
		{"client credentials on issuer", OAuthConfig{Issuer: "https://idp.example", Flow: FlowClientCredentials}, false},
		{"flow on a static token", OAuthConfig{Token: "t", Flow: FlowDevice}, false},
		{"unknown flow", OAuthConfig{Issuer: "https://idp.example", Flow: "implicit"}, false},
	} {
		err := checkOAuthSource(tc.cfg)
		if (err == nil) != tc.ok {
			t.Errorf("%s: err = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
	for _, tc := range []struct {
		name string
		cfg  OAuthConfig
	}{
		{"no client id", OAuthConfig{Issuer: "https://idp.example"}},
		{"http issuer", OAuthConfig{Issuer: "http://idp.example", ClientID: "c"}},
		{"bad redirect port", OAuthConfig{Issuer: "https://idp.example", ClientID: "c", RedirectPort: "70000"}},
	} {
		if _, err := newTokenSource(tc.cfg); err == nil {
			t.Errorf("%s: newTokenSource accepted it", tc.name)
		}
	}
}

func TestOAuthMechanism_ExtensionsAndAuthzID(t *testing.T) {
	t.Parallel()

	init, err := handshakeToken(t, OAuthConfig{
		Token:      "tok",
		AuthzID:    "svc,a=b",
		Extensions: " logicalCluster=lkc-1 , identityPoolId=pool-2,",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "n,a=svc=2Ca=3Db,\x01auth=Bearer tok\x01identityPoolId=pool-2\x01logicalCluster=lkc-1\x01\x01"
	if init != want {
		t.Fatalf("initial response = %q, want %q", init, want)
	}
}

func TestParseExtensions_RefusesWhatBreaksFraming(t *testing.T) {
	t.Parallel()

	for _, s := range []string{
		"auth=Bearer x",      // the token's own key
		"AUTH=x",             // case-insensitively
		"bad-key=x",          // keys are letters only
		"k",                  // no value
		"k=a\x01auth=Bearer", // a separator in a value
		"k=1,k=2",            // twice
	} {
		if _, err := parseExtensions(s); err == nil {
			t.Errorf("parseExtensions(%q) accepted it", s)
		}
	}
	if _, err := saslName("a\x01b"); err == nil {
		t.Error("saslName accepted a control character")
	}
	if _, err := oauthMechanism(OAuthConfig{Token: "t", Extensions: "auth=x"}); err == nil {
		t.Error("oauthMechanism accepted a reserved extension")
	}
}

func TestFileTokenStore(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "k4a", "oauth")
	s := FileTokenStore{Dir: dir}
	key := tokenCacheKey(OAuthConfig{Issuer: "https://idp.example", ClientID: "c"})

	if tok, err := s.Load(key); err != nil || tok != nil {
		t.Fatalf("empty Load = %+v, %v", tok, err)
	}
	exp := time.Now().Add(time.Hour).Round(time.Second)
	if err := s.Save(key, &oauth2.Token{AccessToken: "a", RefreshToken: "r", Expiry: exp}); err != nil {
		t.Fatal(err)
	}
	tok, err := s.Load(key)
	if err != nil || tok.AccessToken != "a" || tok.RefreshToken != "r" || !tok.Expiry.Equal(exp) {
		t.Fatalf("Load = %+v, %v", tok, err)
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(filepath.Join(dir, key+".json"))
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("token file mode = %v, %v; want 0600", fi.Mode().Perm(), err)
		}
		di, err := os.Stat(dir)
		if err != nil || di.Mode().Perm() != 0o700 {
			t.Fatalf("token dir mode = %v, %v; want 0700", di.Mode().Perm(), err)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("cache dir holds %d entries, want only the token (no temp files)", len(entries))
	}
	if err := s.Delete(key); err != nil {
		t.Fatal(err)
	}
	if tok, _ := s.Load(key); tok != nil {
		t.Fatalf("Load after Delete = %+v", tok)
	}
	if err := s.Save("../escape", &oauth2.Token{AccessToken: "a"}); err == nil {
		t.Fatal("Save accepted a key that is not hex")
	}
}

func TestDefaultTokenStore_UnderUserConfigDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("AppData", filepath.Join(home, "AppData"))

	dir := DefaultTokenStore().Dir
	if !strings.HasPrefix(dir, home) || !strings.HasSuffix(dir, filepath.Join("k4a", "oauth")) {
		t.Fatalf("default token dir = %q, want k4a/oauth under %q", dir, home)
	}
	if strings.Contains(dir, ".k4a") {
		t.Fatalf("default token dir %q is under ~/.k4a, which state export copies", dir)
	}
}
