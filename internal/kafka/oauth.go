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
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// OAuthConfig says where an OAUTHBEARER token comes from. Exactly one source
// is used: a static Token, a TokenFile re-read on every handshake (so a
// sidecar that rotates it is picked up without a restart), the OAuth 2.0
// client-credentials grant against TokenURL, whose token is cached in memory
// and fetched again shortly before it expires, or an interactive OIDC login
// (device code or browser) against Issuer, whose token is cached in Store and
// refreshed with its refresh token. Nothing here, nor any token, is logged.
type OAuthConfig struct {
	// Store caches the interactive flows' tokens; nil means
	// DefaultTokenStore. Tests inject one so they never touch the user's.
	Store        TokenStore
	Token        string
	TokenFile    string
	TokenURL     string
	ClientID     string
	ClientSecret string
	// Scopes is a comma- or space-separated list sent with the token request.
	Scopes string
	// AuthzID is the SASL authorization identity: act as this principal
	// rather than the token's own (RFC 7628's gs2 "a=").
	AuthzID string
	// Extensions are RFC 7628 SASL extensions, "key=value,key=value", that
	// some brokers require beside the token (Confluent Cloud's
	// logicalCluster and identityPoolId).
	Extensions string
	// Flow names the grant: client-credentials (TokenURL), or device or
	// browser (Issuer). Empty infers it from the source, device for Issuer.
	Flow string
	// Issuer is the OIDC issuer the interactive flows discover their
	// endpoints from (/.well-known/openid-configuration).
	Issuer string
	// RedirectPort fixes the browser flow's loopback port for an identity
	// provider that matches redirect URIs exactly; empty picks a free one.
	RedirectPort string
}

// IsSet reports whether any OAuth source field is filled in, which is what
// makes a --brokers connection without --auth mean oauthbearer.
func (o OAuthConfig) IsSet() bool {
	return o.Token != "" || o.TokenFile != "" || o.TokenURL != "" || o.ClientID != "" || o.ClientSecret != "" ||
		o.Issuer != "" || o.Flow != ""
}

// Interactive reports whether the token comes from an interactive OIDC login,
// which needs a person at a terminal the first time and a cached token after.
func (o OAuthConfig) Interactive() bool { return o.Issuer != "" }

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
	ext, err := parseExtensions(cfg.Extensions)
	if err != nil {
		return nil, err
	}
	zid, err := saslName(cfg.AuthzID)
	if err != nil {
		return nil, err
	}
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		tok, err := src(ctx)
		if err != nil {
			return oauth.Auth{}, err
		}
		// franz-go writes the token between \x01 separators verbatim; one
		// inside it would end the field early and inject the rest.
		if strings.ContainsAny(tok, "\x00\x01") {
			return oauth.Auth{}, errors.New("OAuth token contains a control character and cannot be sent")
		}
		return oauth.Auth{Token: tok, Zid: zid, Extensions: ext}, nil
	}), nil
}

// tokenSource returns the bearer token to present on one handshake.
type tokenSource func(ctx context.Context) (string, error)

func newTokenSource(cfg OAuthConfig) (tokenSource, error) {
	if err := checkOAuthSource(cfg); err != nil {
		return nil, err
	}
	switch {
	case cfg.Token != "":
		tok := cfg.Token
		return func(context.Context) (string, error) { return tok, nil }, nil
	case cfg.TokenFile != "":
		return fileTokenSource(cfg.TokenFile), nil
	case cfg.Issuer != "":
		s, err := newOIDCSource(cfg)
		if err != nil {
			return nil, err
		}
		return s.token, nil
	default:
		return newClientCredentialsSource(cfg)
	}
}

// OAuth grant names for OAuthConfig.Flow.
const (
	FlowClientCredentials = "client-credentials"
	FlowDevice            = "device"
	FlowBrowser           = "browser"
)

// checkOAuthSource enforces exactly one token source, and a flow that fits it.
func checkOAuthSource(cfg OAuthConfig) error {
	sources := 0
	for _, set := range []bool{cfg.Token != "", cfg.TokenFile != "", cfg.TokenURL != "", cfg.Issuer != ""} {
		if set {
			sources++
		}
	}
	switch {
	case sources == 0:
		return errors.New(
			"oauthbearer auth requires a token (--oauth-token), a token file (--oauth-token-file), a token URL with a client ID and secret (--oauth-token-url) or an OIDC issuer to log in to (--oauth-issuer)",
		)
	case sources > 1:
		return errors.New("oauthbearer auth takes exactly one of a token, a token file, a token URL or an OIDC issuer")
	}
	switch cfg.Flow {
	case "":
		return nil
	case FlowClientCredentials:
		if cfg.TokenURL == "" {
			return errors.New("the client-credentials flow takes a token URL (--oauth-token-url)")
		}
	case FlowDevice, FlowBrowser:
		if cfg.Issuer == "" {
			return fmt.Errorf("the %s flow discovers its endpoints from an OIDC issuer (--oauth-issuer)", cfg.Flow)
		}
	default:
		return fmt.Errorf("unknown OAuth flow %q: use client-credentials, device or browser", cfg.Flow)
	}
	return nil
}

// parseExtensions parses "key=value,key=value" into RFC 7628 SASL
// extensions. franz-go writes them into the initial response as given, so
// anything that could break its framing is refused here: keys are letters
// only and never "auth" (the token's own key), values are printable.
func parseExtensions(s string) (map[string]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil //nolint:nilnil // no extensions is a nil map
	}
	ext := map[string]string{}
	for item := range strings.SplitSeq(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		k, v, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("OAuth extension %q is not key=value", item)
		}
		if !isExtensionKey(k) {
			return nil, fmt.Errorf("OAuth extension key %q must be letters only", k)
		}
		if strings.EqualFold(k, "auth") {
			return nil, errors.New(`OAuth extension key "auth" is reserved for the token`)
		}
		if _, dup := ext[k]; dup {
			return nil, fmt.Errorf("OAuth extension %q is given twice", k)
		}
		for _, r := range v {
			if (r < 0x21 || r > 0x7e) && r != ' ' && r != '\t' {
				return nil, fmt.Errorf("OAuth extension %q has a value with a non-printable character", k)
			}
		}
		ext[k] = v
	}
	return ext, nil
}

func isExtensionKey(k string) bool {
	if k == "" {
		return false
	}
	for _, r := range k {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return false
		}
	}
	return true
}

// saslName encodes an authorization ID for the GS2 header (RFC 5801):
// "=" and "," are escaped, since franz-go writes it as given and a bare comma
// would end the header.
func saslName(s string) (string, error) {
	if !utf8.ValidString(s) || strings.ContainsAny(s, "\x00\x01") {
		return "", errors.New("OAuth authorization ID must be UTF-8 without control characters")
	}
	return strings.NewReplacer("=", "=3D", ",", "=2C").Replace(s), nil
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
	if err := checkEndpointURL("OAuth token URL", cfg.TokenURL); err != nil {
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

// checkEndpointURL refuses to send a client secret, a code or a refresh token
// anywhere but over HTTPS, with an exception for a loopback identity provider
// (a local dev stack). label names the URL in errors ("OAuth token URL").
func checkEndpointURL(label, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("invalid %s %q", label, redactURL(raw))
	}
	if u.User != nil {
		return fmt.Errorf("%s must not embed credentials; use the client ID and secret", label)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopback(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("%s %s must use https (http is allowed only for localhost)", label, redactURL(raw))
	default:
		return fmt.Errorf("%s %s must use https", label, redactURL(raw))
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
