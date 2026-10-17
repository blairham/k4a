// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package secrets keeps context credentials in the OS keychain (macOS
// Keychain, the Secret Service on Linux, Windows Credential Manager) so they
// need not sit in ~/.k4a/config.yaml or the environment. Callers take a
// Store rather than reaching for the keychain directly, so tests run against
// Memory and never touch the real one.
package secrets

import (
	"errors"
	"fmt"
	"sync"

	"github.com/zalando/go-keyring"
)

// Service is the keychain service every k4a entry is stored under; the
// account names the entry within it.
const Service = "k4a"

// ErrNotFound means the keychain holds no entry for the account.
var ErrNotFound = errors.New("no such keychain entry")

// Store reads and writes secrets by account name under Service.
type Store interface {
	Get(account string) (string, error)
	Set(account, secret string) error
	Delete(account string) error
}

// Account is the account name `k4a set-credential` stores a context's field
// under, e.g. "production/password".
func Account(context, field string) string {
	return context + "/" + field
}

// Keyring returns the Store backed by the OS keychain.
func Keyring() Store { return osKeyring{} }

type osKeyring struct{}

func (osKeyring) Get(account string) (string, error) {
	s, err := keyring.Get(Service, account)
	return s, wrap(account, err)
}

func (osKeyring) Set(account, secret string) error {
	return wrap(account, keyring.Set(Service, account, secret))
}

func (osKeyring) Delete(account string) error {
	return wrap(account, keyring.Delete(Service, account))
}

func wrap(account string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, keyring.ErrNotFound):
		return fmt.Errorf("%w: %s/%s", ErrNotFound, Service, account)
	default:
		return fmt.Errorf("keychain %s/%s: %w", Service, account, err)
	}
}

// Memory is an in-process Store for tests. The zero value is not usable;
// call NewMemory.
type Memory struct {
	m  map[string]string
	mu sync.Mutex
}

// NewMemory returns an empty in-memory Store.
func NewMemory() *Memory { return &Memory{m: map[string]string{}} }

// Get returns the secret for account, or ErrNotFound.
func (s *Memory) Get(account string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[account]
	if !ok {
		return "", fmt.Errorf("%w: %s/%s", ErrNotFound, Service, account)
	}
	return v, nil
}

// Set stores secret under account.
func (s *Memory) Set(account, secret string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[account] = secret
	return nil
}

// Delete removes account, or returns ErrNotFound.
func (s *Memory) Delete(account string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[account]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrNotFound, Service, account)
	}
	delete(s.m, account)
	return nil
}
