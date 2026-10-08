// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/config"
)

// The regression these cover: a config holding three working contexts but no
// current-context made bare `k4a` exit 1 with "create a config at <path>" —
// advice to create the file that was sitting right there, fully populated.

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const populatedNoCurrent = `contexts:
  production-us:
    brokers: prod:9098
    auth: iam
  staging-us:
    brokers: stg:9098
    auth: iam
`

// The recoverable case must reach the caller as a bare sentinel, because the
// caller's response is to open the picker, not to print advice.
func TestResolveConnectionReturnsBareSentinelWhenNothingIsCurrent(t *testing.T) {
	_, _, _, err := resolveConnection(UIFlags{ConfigFile: writeCfg(t, populatedNoCurrent)})
	if !errors.Is(err, errPickContext) {
		t.Fatalf("err = %v, want errPickContext", err)
	}
	if strings.Contains(err.Error(), "create a config") {
		t.Errorf("recoverable signal was wrapped in advice: %v", err)
	}
}

// An empty config is the one case where "create a config" is the right advice.
func TestResolveConnectionStillAdvisesCreatingAnEmptyConfig(t *testing.T) {
	_, _, _, err := resolveConnection(UIFlags{ConfigFile: writeCfg(t, "contexts: {}\n")})
	if err == nil {
		t.Fatal("expected an error")
	}
	// An empty config raises ErrNoCurrentContext too, but it is NOT the
	// picker case: there is nothing to pick. It must stay fatal and advise.
	if errors.Is(err, errPickContext) {
		t.Error("an empty config opened the picker; there is nothing in it to pick")
	}
	if !strings.Contains(err.Error(), "create a config") {
		t.Errorf("want the create-a-config advice for an empty config, got: %v", err)
	}
}

// Naming a context that isn't there stays fatal, and the message lists what is.
func TestResolveConnectionOnAMissingNameListsWhatExists(t *testing.T) {
	_, _, _, err := resolveConnection(UIFlags{
		ConfigFile: writeCfg(t, populatedNoCurrent),
		Context:    "nope",
	})
	if !errors.Is(err, config.ErrContextNotFound) {
		t.Fatalf("err = %v, want ErrContextNotFound", err)
	}
	msg := err.Error()
	if strings.Contains(msg, "create a config") {
		t.Errorf("wrong advice for a populated config: %v", err)
	}
	for _, want := range []string{"use-context", "production-us", "staging-us"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message missing %q: %v", want, err)
		}
	}
}
