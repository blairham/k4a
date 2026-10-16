// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/secrets"
)

// refusingStore fails the test on any use: a context without keychain
// references, or whose secrets are already supplied, must not read it.
type refusingStore struct{ t *testing.T }

func (s refusingStore) Get(a string) (string, error) {
	s.t.Errorf("keychain read %q", a)
	return "", nil
}
func (s refusingStore) Set(a, _ string) error { s.t.Errorf("keychain write %q", a); return nil }
func (s refusingStore) Delete(a string) error { s.t.Errorf("keychain delete %q", a); return nil }

func TestApplyKeychainFillsOnlyWhatIsMissing(t *testing.T) {
	t.Parallel()

	store := secrets.NewMemory()
	_ = store.Set("prod/password", "from-keychain")
	_ = store.Set("prod/oauth-client-secret", "secret-from-keychain")
	ctx := Context{
		Auth:     "scram",
		Username: "alice",
		Keychain: Keychain{Password: "prod/password", OAuthClientSecret: "prod/oauth-client-secret"},
	}

	got, err := ctx.ApplyKeychain(ctx.ToAuthConfig(), store)
	if err != nil {
		t.Fatal(err)
	}
	if got.Password != "from-keychain" || got.OAuth.ClientSecret != "secret-from-keychain" {
		t.Fatalf("auth = %+v, want both secrets from the keychain", got)
	}

	// A flag (or env, or a literal in the file) already supplied it: the
	// keychain is not read at all.
	auth := ctx.ToAuthConfig()
	auth.Password = "from-flag"
	auth.OAuth.ClientSecret = "from-flag"
	got, err = ctx.ApplyKeychain(auth, refusingStore{t})
	if err != nil || got.Password != "from-flag" {
		t.Fatalf("auth = %+v, %v; want the flag's password", got, err)
	}
}

func TestApplyKeychainWithoutReferencesNeverReads(t *testing.T) {
	t.Parallel()

	ctx := Context{Auth: "scram", Username: "alice"}
	got, err := ctx.ApplyKeychain(ctx.ToAuthConfig(), refusingStore{t})
	if err != nil || got.Password != "" {
		t.Fatalf("auth = %+v, %v", got, err)
	}
	if _, err := ctx.ApplyKeychain(ctx.ToAuthConfig(), nil); err != nil {
		t.Fatalf("nil store without references: %v", err)
	}
}

func TestApplyKeychainMissingEntry(t *testing.T) {
	t.Parallel()

	ctx := Context{Auth: "scram", Username: "alice", Keychain: Keychain{Password: "prod/password"}}
	_, err := ctx.ApplyKeychain(ctx.ToAuthConfig(), secrets.NewMemory())
	if !errors.Is(err, secrets.ErrNotFound) || !strings.Contains(err.Error(), "set-credential") {
		t.Fatalf("err = %v, want ErrNotFound naming set-credential", err)
	}
	if _, err := ctx.ApplyKeychain(ctx.ToAuthConfig(), nil); err == nil {
		t.Fatal("a reference with no keychain available must fail, not connect without a password")
	}
}

func TestKeychainRefRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	ctx := Context{Auth: "scram", Username: "alice", Password: "literal"}
	if err := ctx.SetKeychainRef("password", "prod/password"); err != nil {
		t.Fatal(err)
	}
	if ctx.Password != "" {
		t.Fatalf("SetKeychainRef left the literal password in the context")
	}
	if err := ctx.SetKeychainRef("username", "x"); !errors.Is(err, ErrUnknownKeychainField) {
		t.Fatalf("username: err = %v", err)
	}
	if err := Save(path, &Config{Contexts: map[string]Context{"prod": ctx}}); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.Contexts["prod"]
	if got.Keychain.Password != "prod/password" {
		t.Fatalf("reloaded keychain = %+v", got.Keychain)
	}
	if account, _ := got.ClearKeychainRef("password"); account != "prod/password" || got.Keychain != (Keychain{}) {
		t.Fatalf("ClearKeychainRef = %q, keychain now %+v", account, got.Keychain)
	}
}

// An empty keychain block is not written, so configs that never use it are
// unchanged by a Save.
func TestEmptyKeychainOmitted(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := Save(path, &Config{Contexts: map[string]Context{"dev": {Auth: "plaintext"}}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "keychain") {
		t.Fatalf("empty keychain block written:\n%s", data)
	}
}
