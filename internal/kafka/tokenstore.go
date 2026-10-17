// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/oauth2"

	"github.com/blairham/k4a/internal/secrets"
)

// TokenStore caches the tokens of interactive OIDC logins, keyed by issuer,
// client and scopes. Load returns nil for a key it does not hold.
type TokenStore interface {
	Load(key string) (*oauth2.Token, error)
	Save(key string, tok *oauth2.Token) error
	Delete(key string) error
}

// FileTokenStore keeps one JSON file per login in Dir, owner-only (0600 in a
// 0700 directory). It is the default when no keychain is injected, and
// KeychainTokenStore's fallback.
type FileTokenStore struct {
	Dir string
}

// DefaultTokenStore is a FileTokenStore under the user config directory
// (os.UserConfigDir()/k4a/oauth), deliberately not ~/.k4a: that tree is
// exported by `k4a state export` and holds nothing secret.
func DefaultTokenStore() FileTokenStore {
	dir, err := os.UserConfigDir()
	if err != nil {
		return FileTokenStore{}
	}
	return FileTokenStore{Dir: filepath.Join(dir, "k4a", "oauth")}
}

type storedToken struct {
	Expiry       time.Time `json:"expiry,omitzero"`
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type,omitempty"`
	RefreshToken string    `json:"refresh_token,omitempty"`
}

func encodeToken(tok *oauth2.Token) ([]byte, error) {
	return json.Marshal(storedToken{ //nolint:gosec // writing the token is this cache's purpose
		AccessToken:  tok.AccessToken,
		TokenType:    tok.TokenType,
		RefreshToken: tok.RefreshToken,
		Expiry:       tok.Expiry,
	})
}

// decodeToken returns nil for a corrupt or empty entry: the next login
// replaces it.
func decodeToken(b []byte) *oauth2.Token {
	var st storedToken
	if err := json.Unmarshal(b, &st); err != nil || (st.AccessToken == "" && st.RefreshToken == "") {
		return nil
	}
	return &oauth2.Token{
		AccessToken:  st.AccessToken,
		TokenType:    st.TokenType,
		RefreshToken: st.RefreshToken,
		Expiry:       st.Expiry,
	}
}

func checkKey(key string) error {
	if key == "" {
		return errors.New("empty token cache key")
	}
	for _, r := range key {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return fmt.Errorf("invalid token cache key %q", key)
		}
	}
	return nil
}

func (s FileTokenStore) path(key string) (string, error) {
	if s.Dir == "" {
		return "", errors.New("no user config directory to cache the OIDC token in")
	}
	if err := checkKey(key); err != nil {
		return "", err
	}
	return filepath.Join(s.Dir, key+".json"), nil
}

// Load reads a cached token. A missing or unreadable cache entry is no token:
// the cost is a login, which overwrites it.
func (s FileTokenStore) Load(key string) (*oauth2.Token, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, nil //nolint:nilnil,nilerr // no cache directory means nothing cached
	}
	b, err := os.ReadFile(p) //nolint:gosec // p is Dir plus a hex key
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil //nolint:nilnil // not cached
		}
		return nil, fmt.Errorf("reading the cached OIDC token: %w", err)
	}
	return decodeToken(b), nil
}

// Save writes tok atomically (a temp file renamed over the entry), so a
// concurrent reader sees the old token or the new one, never half of either.
func (s FileTokenStore) Save(key string, tok *oauth2.Token) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	b, err := encodeToken(tok)
	if err != nil {
		return err
	}
	if mkErr := os.MkdirAll(s.Dir, 0o700); mkErr != nil {
		return fmt.Errorf("creating the OIDC token cache: %w", mkErr)
	}
	f, err := os.CreateTemp(s.Dir, ".token-*") // created 0600
	if err != nil {
		return fmt.Errorf("writing the OIDC token cache: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) //nolint:errcheck // gone after the rename; this cleans up a failure
	if _, werr := f.Write(b); werr != nil {
		f.Close() //nolint:errcheck,gosec // the write error is the one to report
		return fmt.Errorf("writing the OIDC token cache: %w", werr)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("writing the OIDC token cache: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("writing the OIDC token cache: %w", err)
	}
	return nil
}

// Delete forgets a cached token; a missing one is not an error.
func (s FileTokenStore) Delete(key string) error {
	p, err := s.path(key)
	if err != nil {
		return nil //nolint:nilerr // nothing can be cached without a directory
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing the cached OIDC token: %w", err)
	}
	return nil
}

// KeychainTokenStore keeps each login's token in the OS keychain, as account
// "oauth/<key>" under the k4a service, and uses Fallback when the keychain
// cannot take it: no Secret Service on a headless Linux host, or a token
// larger than the macOS (~3 KB) or Windows (2.5 KB) entry limit.
type KeychainTokenStore struct {
	Keychain secrets.Store
	Fallback TokenStore
}

func keychainAccount(key string) string { return "oauth/" + key }

// Load prefers the keychain and reads the fallback when the keychain has no
// entry or cannot be reached, which is where such a token would have gone.
func (s KeychainTokenStore) Load(key string) (*oauth2.Token, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	if v, err := s.Keychain.Get(keychainAccount(key)); err == nil {
		if tok := decodeToken([]byte(v)); tok != nil {
			return tok, nil
		}
	}
	return s.Fallback.Load(key)
}

// Save writes to the keychain and then drops any fallback copy, so a stale
// file cannot outlive a token the keychain replaced; if the keychain refuses,
// the token goes to the fallback instead.
func (s KeychainTokenStore) Save(key string, tok *oauth2.Token) error {
	if err := checkKey(key); err != nil {
		return err
	}
	b, err := encodeToken(tok)
	if err != nil {
		return err
	}
	if s.Keychain.Set(keychainAccount(key), string(b)) == nil {
		return s.Fallback.Delete(key)
	}
	return s.Fallback.Save(key, tok)
}

// Delete removes both copies. A keychain error counts only when the entry is
// provably still there, so logging out on a host without a keychain works.
func (s KeychainTokenStore) Delete(key string) error {
	if err := checkKey(key); err != nil {
		return err
	}
	var errs []error
	acct := keychainAccount(key)
	if kerr := s.Keychain.Delete(acct); kerr != nil && !errors.Is(kerr, secrets.ErrNotFound) {
		if _, gerr := s.Keychain.Get(acct); gerr == nil {
			errs = append(errs, kerr)
		}
	}
	if ferr := s.Fallback.Delete(key); ferr != nil {
		errs = append(errs, ferr)
	}
	return errors.Join(errs...)
}
