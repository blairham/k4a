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
)

// TokenStore caches the tokens of interactive OIDC logins, keyed by issuer,
// client and scopes. Load returns nil for a key it does not hold.
type TokenStore interface {
	Load(key string) (*oauth2.Token, error)
	Save(key string, tok *oauth2.Token) error
	Delete(key string) error
}

// FileTokenStore keeps one JSON file per login in Dir, owner-only (0600 in a
// 0700 directory). It is the default until a platform keychain store exists.
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

func (s FileTokenStore) path(key string) (string, error) {
	if s.Dir == "" {
		return "", errors.New("no user config directory to cache the OIDC token in")
	}
	for _, r := range key {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", fmt.Errorf("invalid token cache key %q", key)
		}
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
	var st storedToken
	if err := json.Unmarshal(b, &st); err != nil || (st.AccessToken == "" && st.RefreshToken == "") {
		return nil, nil //nolint:nilnil,nilerr // a corrupt entry is replaced by the next login
	}
	return &oauth2.Token{
		AccessToken:  st.AccessToken,
		TokenType:    st.TokenType,
		RefreshToken: st.RefreshToken,
		Expiry:       st.Expiry,
	}, nil
}

// Save writes tok atomically (a temp file renamed over the entry), so a
// concurrent reader sees the old token or the new one, never half of either.
func (s FileTokenStore) Save(key string, tok *oauth2.Token) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	b, err := json.Marshal(storedToken{ //nolint:gosec // writing the token is this cache's purpose
		AccessToken:  tok.AccessToken,
		TokenType:    tok.TokenType,
		RefreshToken: tok.RefreshToken,
		Expiry:       tok.Expiry,
	})
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
