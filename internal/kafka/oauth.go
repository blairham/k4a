// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// OAuthConfig says where an OAUTHBEARER token comes from. Exactly one source
// is used: a static Token, a TokenFile re-read on every handshake (so a
// sidecar that rotates it is picked up without a restart), or the OAuth 2.0
// client-credentials grant against TokenURL, whose token is cached in memory
// and fetched again shortly before it expires. None of these values, nor the
// token itself, is ever written to disk or logged by k4a.
type OAuthConfig struct {
	Token        string
	TokenFile    string
	TokenURL     string
	ClientID     string
	ClientSecret string
	// Scopes is a comma- or space-separated list sent with the
	// client-credentials request.
	Scopes string
}

// IsSet reports whether any OAuth source field is filled in, which is what
// makes a --brokers connection without --auth mean oauthbearer.
func (o OAuthConfig) IsSet() bool {
	return o.Token != "" || o.TokenFile != "" || o.TokenURL != "" || o.ClientID != "" || o.ClientSecret != ""
}

// tokenFetchTimeout bounds one client-credentials request when the handshake's
// own context carries no deadline.
const tokenFetchTimeout = 30 * time.Second

// oauthMechanism builds the OAUTHBEARER mechanism for cfg. The token is
// resolved lazily, on each handshake, so an unreachable identity provider is
// a connection error rather than a startup one, and franz-go's
// re-authentication on session expiry gets a fresh token.
func oauthMechanism(cfg OAuthConfig) (sasl.Mechanism, error) {
	src, err := newTokenSource(cfg)
	if err != nil {
		return nil, err
	}
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		tok, err := src(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: tok}, nil
	}), nil
}

// tokenSource returns the bearer token to present on one handshake.
type tokenSource func(ctx context.Context) (string, error)

func newTokenSource(cfg OAuthConfig) (tokenSource, error) {
	sources := 0
	for _, set := range []bool{cfg.Token != "", cfg.TokenFile != "", cfg.TokenURL != ""} {
		if set {
			sources++
		}
	}
	switch {
	case sources == 0:
		return nil, errors.New(
			"oauthbearer auth requires a token (--oauth-token), a token file (--oauth-token-file) or a token URL with a client ID and secret (--oauth-token-url)",
		)
	case sources > 1:
		return nil, errors.New("oauthbearer auth takes exactly one of a token, a token file or a token URL")
	}

	switch {
	case cfg.Token != "":
		tok := cfg.Token
		return func(context.Context) (string, error) { return tok, nil }, nil
	case cfg.TokenFile != "":
		return fileTokenSource(cfg.TokenFile), nil
	default:
		return newClientCredentialsSource(cfg)
	}
}

// fileTokenSource reads the token from path on every handshake.
func fileTokenSource(path string) tokenSource {
	return func(context.Context) (string, error) {
		b, err := os.ReadFile(path) //nolint:gosec // the user named this file as their token
		if err != nil {
			return "", fmt.Errorf("reading OAuth token file: %w", err)
		}
		tok := strings.TrimSpace(string(b))
		if tok == "" {
			return "", fmt.Errorf("OAuth token file %s is empty", path)
		}
		return tok, nil
	}
}

// clientCredentialsSource caches the client-credentials token and fetches a
// new one once it is within oauth2's expiry margin of expiring.
type clientCredentialsSource struct {
	cfg    clientcredentials.Config
	client *http.Client
	tok    *oauth2.Token
	mu     sync.Mutex
}

func newClientCredentialsSource(cfg OAuthConfig) (tokenSource, error) {
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, errors.New("oauthbearer with a token URL requires a client ID and client secret")
	}
	if err := checkTokenURL(cfg.TokenURL); err != nil {
		return nil, err
	}
	s := &clientCredentialsSource{
		cfg: clientcredentials.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			TokenURL:     cfg.TokenURL,
			Scopes:       splitScopes(cfg.Scopes),
		},
		client: &http.Client{Timeout: tokenFetchTimeout},
	}
	return s.token, nil
}

func (s *clientCredentialsSource) token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tok.Valid() {
		return s.tok.AccessToken, nil
	}
	tok, err := s.cfg.Token(context.WithValue(ctx, oauth2.HTTPClient, s.client))
	if err != nil {
		return "", fmt.Errorf("fetching OAuth token from %s: %w", redactURL(s.cfg.TokenURL), err)
	}
	s.tok = tok
	return tok.AccessToken, nil
}

// checkTokenURL refuses to send the client secret anywhere but over HTTPS,
// with an exception for a loopback identity provider (a local dev stack).
func checkTokenURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid OAuth token URL %q", redactURL(raw))
	}
	if u.User != nil {
		return errors.New("OAuth token URL must not embed credentials; use the client ID and secret")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopback(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("OAuth token URL %s must use https (http is allowed only for localhost)", redactURL(raw))
	default:
		return fmt.Errorf("OAuth token URL %s must use https", redactURL(raw))
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// redactURL drops any userinfo and query from a URL before it goes into an
// error message, since either could carry a secret.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable URL)"
	}
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func splitScopes(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
}
