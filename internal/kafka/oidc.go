// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

// ErrOAuthLoginRequired is what a handshake returns when an interactive OIDC
// context has no usable cached token. A handshake never prompts: the TUI owns
// the screen by then, and a device login outlasts any dial timeout.
var ErrOAuthLoginRequired = errors.New("OIDC login required: run `k4a login` with the same context or flags")

// browserLoginTimeout bounds the wait for the browser to come back to the
// loopback redirect. The device flow is bounded by the IdP's own expiry.
const browserLoginTimeout = 5 * time.Minute

// maxDiscoveryBytes caps the discovery document read from the issuer.
const maxDiscoveryBytes = 1 << 20

// LoginOptions configures an interactive OIDC login.
type LoginOptions struct {
	// Out receives the instructions for the person logging in.
	Out io.Writer
	// OpenBrowser opens a URL for the browser flow; nil uses the system's
	// opener. Its failure is not fatal: the URL is printed as well.
	OpenBrowser func(url string) error
	// Force logs in even when a usable token is cached.
	Force bool
}

// OAuthLogin makes sure an interactive OIDC context has a usable token: a
// cached one, one refreshed from the cached refresh token, or, failing both,
// one from a device-code or browser login run on opts.Out. It reports
// whether a person had to log in.
func OAuthLogin(ctx context.Context, cfg OAuthConfig, opts LoginOptions) (bool, error) {
	if !cfg.Interactive() {
		return false, errors.New("login applies only to an OIDC issuer (--oauth-issuer) with the device or browser flow")
	}
	if err := checkOAuthSource(cfg); err != nil {
		return false, err
	}
	s, err := newOIDCSource(cfg)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !opts.Force {
		cached, cerr := s.current(ctx)
		if cerr != nil {
			return false, cerr
		}
		if cached != nil {
			return false, nil
		}
	}
	tok, err := s.login(ctx, opts)
	if err != nil {
		return true, err
	}
	s.tok = tok
	if err := s.store.Save(s.key, tok); err != nil {
		return true, fmt.Errorf("caching the OIDC token: %w", err)
	}
	return true, nil
}

// OAuthLogout forgets the cached token of an interactive OIDC context.
func OAuthLogout(cfg OAuthConfig) error {
	if !cfg.Interactive() {
		return errors.New("logout applies only to an OIDC issuer (--oauth-issuer)")
	}
	s, err := newOIDCSource(cfg)
	if err != nil {
		return err
	}
	return s.store.Delete(s.key)
}

// oidcSource serves a token obtained by an interactive login: from memory,
// from the store, or refreshed with the stored refresh token.
type oidcSource struct {
	store  TokenStore
	client *http.Client
	oc     *oauth2.Config // set by discovery
	tok    *oauth2.Token
	md     *oidcMetadata
	cfg    OAuthConfig
	key    string
	mu     sync.Mutex
}

func newOIDCSource(cfg OAuthConfig) (*oidcSource, error) {
	if cfg.ClientID == "" {
		return nil, errors.New("an OIDC login requires a client ID (--oauth-client-id)")
	}
	if err := checkEndpointURL("OIDC issuer", cfg.Issuer); err != nil {
		return nil, err
	}
	if cfg.RedirectPort != "" {
		if p, err := strconv.Atoi(cfg.RedirectPort); err != nil || p < 1 || p > 65535 {
			return nil, fmt.Errorf("OAuth redirect port %q must be a number from 1 to 65535", cfg.RedirectPort)
		}
	}
	store := cfg.Store
	if store == nil {
		store = DefaultTokenStore()
	}
	return &oidcSource{
		store:  store,
		client: &http.Client{Timeout: tokenFetchTimeout},
		cfg:    cfg,
		key:    tokenCacheKey(cfg),
	}, nil
}

// tokenCacheKey identifies a login: the same issuer, client and scopes share
// a token whichever context or flow asked for it.
func tokenCacheKey(cfg OAuthConfig) string {
	scopes := splitScopes(cfg.Scopes)
	slices.Sort(scopes)
	sum := sha256.Sum256([]byte(strings.Join([]string{
		strings.TrimSuffix(cfg.Issuer, "/"), cfg.ClientID, strings.Join(scopes, " "),
	}, "\n")))
	return hex.EncodeToString(sum[:16])
}

func (s *oidcSource) token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok, err := s.current(ctx)
	if err != nil {
		return "", err
	}
	if tok == nil {
		return "", fmt.Errorf("%w (issuer %s)", ErrOAuthLoginRequired, redactURL(s.cfg.Issuer))
	}
	return tok.AccessToken, nil
}

// current returns a valid token, refreshing it if it can, or nil when a
// person has to log in. Callers hold s.mu.
func (s *oidcSource) current(ctx context.Context) (*oauth2.Token, error) {
	if s.tok.Valid() {
		return s.tok, nil
	}
	if s.tok == nil {
		stored, err := s.store.Load(s.key)
		if err != nil {
			return nil, err
		}
		s.tok = stored
	}
	if s.tok.Valid() {
		return s.tok, nil
	}
	if s.tok == nil || s.tok.RefreshToken == "" {
		return nil, nil //nolint:nilnil // nil token, nil error means "log in"
	}
	oc, err := s.config(ctx)
	if err != nil {
		return nil, err
	}
	fresh, err := oc.TokenSource(s.httpContext(ctx), s.tok).Token()
	if err != nil {
		var re *oauth2.RetrieveError
		if errors.As(err, &re) {
			// The IdP refused the refresh token (expired, revoked, rotated
			// by another process): it is dead, so forget it and log in.
			s.tok = nil
			s.store.Delete(s.key) //nolint:errcheck,gosec // best effort: a login overwrites it anyway
			return nil, nil       //nolint:nilnil // as above
		}
		return nil, fmt.Errorf("refreshing the OIDC token at %s: %w", redactURL(oc.Endpoint.TokenURL), err)
	}
	s.tok = fresh
	// A failed save costs the next process a login, not this connection.
	s.store.Save(s.key, fresh) //nolint:errcheck,gosec // as above
	return fresh, nil
}

func (s *oidcSource) httpContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, oauth2.HTTPClient, s.client)
}

// oidcMetadata is the part of an OIDC discovery document k4a uses.
type oidcMetadata struct {
	Issuer                      string `json:"issuer"`
	AuthorizationEndpoint       string `json:"authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
}

// config discovers the issuer's endpoints once and builds the OAuth client.
func (s *oidcSource) config(ctx context.Context) (*oauth2.Config, error) {
	if s.oc != nil {
		return s.oc, nil
	}
	md, err := discover(s.httpContext(ctx), s.client, s.cfg.Issuer)
	if err != nil {
		return nil, err
	}
	style := oauth2.AuthStyleAutoDetect
	if s.cfg.ClientSecret == "" {
		style = oauth2.AuthStyleInParams // a public client: no Basic header to try first
	}
	s.md = md
	s.oc = &oauth2.Config{
		ClientID:     s.cfg.ClientID,
		ClientSecret: s.cfg.ClientSecret,
		Scopes:       splitScopes(s.cfg.Scopes),
		Endpoint: oauth2.Endpoint{
			AuthURL:       md.AuthorizationEndpoint,
			TokenURL:      md.TokenEndpoint,
			DeviceAuthURL: md.DeviceAuthorizationEndpoint,
			AuthStyle:     style,
		},
	}
	return s.oc, nil
}

// discover reads the issuer's OpenID Provider metadata and checks that it
// names the issuer asked for and that its token endpoint is safe to send a
// refresh token to.
func discover(ctx context.Context, client *http.Client, issuer string) (*oidcMetadata, error) {
	issuer = strings.TrimSuffix(issuer, "/")
	u := issuer + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery at %s: %w", redactURL(issuer), err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery at %s: %w", redactURL(issuer), err)
	}
	defer resp.Body.Close() //nolint:errcheck // read-only body
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OIDC discovery at %s: %s", redactURL(issuer), resp.Status)
	}
	var md oidcMetadata
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxDiscoveryBytes)).Decode(&md); err != nil {
		return nil, fmt.Errorf("OIDC discovery at %s: %w", redactURL(issuer), err)
	}
	if strings.TrimSuffix(md.Issuer, "/") != issuer {
		return nil, fmt.Errorf("OIDC discovery at %s names a different issuer", redactURL(issuer))
	}
	if md.TokenEndpoint == "" {
		return nil, fmt.Errorf("OIDC issuer %s advertises no token endpoint", redactURL(issuer))
	}
	if err := checkEndpointURL("OIDC token endpoint", md.TokenEndpoint); err != nil {
		return nil, err
	}
	return &md, nil
}

// login runs the configured interactive flow. Callers hold s.mu.
func (s *oidcSource) login(ctx context.Context, opts LoginOptions) (*oauth2.Token, error) {
	oc, err := s.config(ctx)
	if err != nil {
		return nil, err
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	if s.cfg.Flow == FlowBrowser {
		if err := checkEndpointURL("OIDC authorization endpoint", s.md.AuthorizationEndpoint); err != nil {
			return nil, err
		}
		open := opts.OpenBrowser
		if open == nil {
			open = openBrowser
		}
		return browserLogin(s.httpContext(ctx), *oc, s.cfg.RedirectPort, out, open)
	}
	if s.md.DeviceAuthorizationEndpoint == "" {
		return nil, fmt.Errorf(
			"OIDC issuer %s does not offer the device flow; use --oauth-flow browser", redactURL(s.cfg.Issuer),
		)
	}
	if err := checkEndpointURL("OIDC device authorization endpoint", s.md.DeviceAuthorizationEndpoint); err != nil {
		return nil, err
	}
	return deviceLogin(s.httpContext(ctx), oc, out)
}

// deviceLogin runs the RFC 8628 device authorization grant: the person opens
// a URL on any device and types the code, while k4a polls for the token.
func deviceLogin(ctx context.Context, oc *oauth2.Config, out io.Writer) (*oauth2.Token, error) {
	da, err := oc.DeviceAuth(ctx)
	if err != nil {
		return nil, fmt.Errorf("starting the device login at %s: %w", redactURL(oc.Endpoint.DeviceAuthURL), err)
	}
	// These strings come from the IdP and go to a terminal.
	say(out, "To sign in, open %s and enter the code %s\n",
		printable(da.VerificationURI), printable(da.UserCode))
	if da.VerificationURIComplete != "" {
		say(out, "  (or open %s)\n", printable(da.VerificationURIComplete))
	}
	say(out, "Waiting for the sign-in to finish...\n")
	tok, err := oc.DeviceAccessToken(ctx, da)
	if err != nil {
		return nil, fmt.Errorf("device login: %w", err)
	}
	return tok, nil
}

type callbackResult struct {
	err  error
	code string
}

// browserLogin runs the authorization-code grant with PKCE (RFC 7636) and a
// loopback redirect (RFC 8252): k4a listens on 127.0.0.1, opens the browser at
// the IdP, and exchanges the code the browser brings back.
func browserLogin(
	ctx context.Context, oc oauth2.Config, port string, out io.Writer, open func(string) error,
) (*oauth2.Token, error) {
	if port == "" {
		port = "0"
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		return nil, fmt.Errorf("listening for the browser login's redirect: %w", err)
	}
	defer ln.Close() //nolint:errcheck // srv.Close closes it too
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return nil, errors.New("listening for the browser login's redirect: not a TCP address")
	}
	oc.RedirectURL = fmt.Sprintf("http://127.0.0.1:%d/callback", addr.Port)

	state, err := randomString()
	if err != nil {
		return nil, err
	}
	verifier := oauth2.GenerateVerifier()
	authURL := oc.AuthCodeURL(state, oauth2.S256ChallengeOption(verifier))

	results := make(chan callbackResult, 1)
	srv := &http.Server{Handler: callbackHandler(state, results), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)  //nolint:errcheck // returns ErrServerClosed when the login ends
	defer srv.Close() //nolint:errcheck // best-effort shutdown

	say(out, "Opening your browser to sign in. If it does not open, visit:\n  %s\n", authURL)
	if openErr := open(authURL); openErr != nil {
		say(out, "(could not open a browser: %v)\n", openErr)
	}

	wait, cancel := context.WithTimeout(ctx, browserLoginTimeout)
	defer cancel()
	var res callbackResult
	select {
	case <-wait.Done():
		return nil, fmt.Errorf("waiting for the browser login: %w", wait.Err())
	case res = <-results:
	}
	if res.err != nil {
		return nil, res.err
	}
	tok, err := oc.Exchange(ctx, res.code, oauth2.VerifierOption(verifier))
	if err != nil {
		return nil, fmt.Errorf("exchanging the browser login's code at %s: %w", redactURL(oc.Endpoint.TokenURL), err)
	}
	return tok, nil
}

// callbackHandler accepts the IdP's redirect. A request without the right
// state is answered and ignored, so a stray or forged request cannot end the
// wait; the first one with it is the result.
func callbackHandler(state string, results chan<- callbackResult) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /callback", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("state") != state {
			http.Error(w, "unexpected login state", http.StatusBadRequest)
			return
		}
		var res callbackResult
		switch {
		case q.Get("error") != "":
			res.err = fmt.Errorf("browser login refused: %s %s",
				printable(q.Get("error")), printable(q.Get("error_description")))
		case q.Get("code") == "":
			res.err = errors.New("browser login returned no code")
		default:
			res.code = q.Get("code")
		}
		select {
		case results <- res:
		default:
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		msg := "Signed in to k4a. You can close this tab."
		if res.err != nil {
			msg = "k4a sign-in failed: " + res.err.Error()
		}
		say(w, "<!doctype html><title>k4a</title><p>%s</p>\n", html.EscapeString(msg))
	})
	return mux
}

// say writes login instructions or the callback page; a failed write is not
// worth failing the login over.
func say(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, format, a...) //nolint:errcheck // best-effort output
}

func randomString() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating login state: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// printable drops control characters from IdP-supplied text before it reaches
// a terminal, where an escape sequence could rewrite what the person sees.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return -1
		}
		return r
	}, s)
}

// openBrowser opens url with the platform's opener without waiting for it.
// The opener hands off to the browser and exits, so it is not tied to the
// login's context: canceling the login must not kill a browser it started.
func openBrowser(url string) error {
	name, args := "xdg-open", []string{url}
	switch runtime.GOOS {
	case "darwin":
		name = "open"
	case "windows":
		name, args = "rundll32", []string{"url.dll,FileProtocolHandler", url}
	}
	cmd := exec.CommandContext(
		context.Background(),
		name,
		args...,
	) //nolint:gosec // url is the authorization URL k4a built
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait() //nolint:errcheck // reap the opener; its exit status says nothing about the login
	return nil
}
