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
	"github.com/blairham/k4a/internal/secrets"
)

const credCfg = `current-context: prod
contexts:
  prod:
    brokers: prod:9092
    auth: scram
    username: alice
    password: plaintext-in-file
`

func writeCredCfg(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(credCfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// stubTerminal makes stdin look interactive and answers each password prompt
// in turn, recording the prompts.
func stubTerminal(t *testing.T, answers ...string) *[]string {
	t.Helper()
	var prompts []string
	oldI, oldR := interactiveStdin, readPassword
	interactiveStdin = func() bool { return true }
	readPassword = func(p string) (string, error) {
		prompts = append(prompts, p)
		if len(answers) == 0 {
			return "", errors.New("no more answers")
		}
		a := answers[0]
		answers = answers[1:]
		return a, nil
	}
	t.Cleanup(func() { interactiveStdin, readPassword = oldI, oldR })
	return &prompts
}

// set-credential moves the password out of the file into the keychain, and
// a connection then reads it from there; delete-credential undoes both.
func TestSetAndDeleteCredential(t *testing.T) {
	path := writeCredCfg(t)

	set := &SetCredentialCommand{in: strings.NewReader("s3cret\n")}
	if code := set.Run([]string{"--config", path, "prod"}); code != 0 {
		t.Fatalf("set-credential exit %d", code)
	}
	if got, err := testStore.Get("prod/password"); err != nil || got != "s3cret" {
		t.Fatalf("keychain prod/password = %q, %v", got, err)
	}
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "plaintext-in-file") || !strings.Contains(string(data), "password: prod/password") {
		t.Fatalf("config after set-credential:\n%s", data)
	}

	f := ConnectionFlags{ConfigFile: path}
	_, auth, err := f.Resolve()
	if err != nil || auth.Password != "s3cret" {
		t.Fatalf("Resolve = %+v, %v; want the keychain password", auth, err)
	}

	// A flag still wins, and then the keychain is not consulted.
	f.Password = "from-flag"
	if _, auth, _ = f.Resolve(); auth.Password != "from-flag" {
		t.Fatalf("flag password lost: %+v", auth)
	}

	if code := (&DeleteCredentialCommand{}).Run([]string{"--config", path, "prod"}); code != 0 {
		t.Fatalf("delete-credential exit %d", code)
	}
	if _, err := testStore.Get("prod/password"); !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("entry survived delete-credential: %v", err)
	}
	cfg, _ := config.Load(path)
	if cfg.Contexts["prod"].Keychain != (config.Keychain{}) {
		t.Fatalf("reference survived delete-credential: %+v", cfg.Contexts["prod"].Keychain)
	}
	// Deleting again is not an error: the goal state already holds.
	if code := (&DeleteCredentialCommand{}).Run([]string{"--config", path, "prod"}); code != 0 {
		t.Fatalf("second delete-credential exit %d", code)
	}
}

func TestSetCredentialRefusals(t *testing.T) {
	path := writeCredCfg(t)
	for name, tc := range map[string]struct {
		in   string
		args []string
	}{
		"unknown context": {"x\n", []string{"--config", path, "nope"}},
		"no context":      {"x\n", []string{"--config", path}},
		"unknown field":   {"x\n", []string{"--config", path, "--field", "username", "prod"}},
		"empty secret":    {"\n", []string{"--config", path, "prod"}},
	} {
		if code := (&SetCredentialCommand{in: strings.NewReader(tc.in)}).Run(tc.args); code == 0 {
			t.Errorf("%s: set-credential succeeded", name)
		}
	}
	data, _ := os.ReadFile(path)
	if string(data) != credCfg {
		t.Fatalf("a refused set-credential rewrote the config:\n%s", data)
	}
}

func TestSetCredentialOnTerminalConfirms(t *testing.T) {
	path := writeCredCfg(t)

	stubTerminal(t, "one", "two")
	if code := (&SetCredentialCommand{}).Run([]string{"--config", path, "--field", "oauth-token", "prod"}); code == 0 {
		t.Fatal("mismatched entries were stored")
	}

	prompts := stubTerminal(t, "tok", "tok")
	if code := (&SetCredentialCommand{}).Run([]string{"--config", path, "--field", "oauth-token", "prod"}); code != 0 {
		t.Fatal("set-credential failed")
	}
	if len(*prompts) != 2 {
		t.Fatalf("prompts = %q, want the secret asked twice", *prompts)
	}
	if got, _ := testStore.Get("prod/oauth-token"); got != "tok" {
		t.Fatalf("keychain prod/oauth-token = %q", got)
	}
	_ = testStore.Delete("prod/oauth-token")
}

// A context with a username and no password asks on a terminal, and leaves a
// pipe alone so a script fails with the usual error instead of hanging.
func TestResolvePromptsForMissingPassword(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := strings.Replace(credCfg, "    password: plaintext-in-file\n", "", 1)
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	f := ConnectionFlags{ConfigFile: path}

	if _, auth, err := f.Resolve(); err != nil || auth.Password != "" {
		t.Fatalf("non-terminal Resolve = %+v, %v; want no prompt and no password", auth, err)
	}

	prompts := stubTerminal(t, "typed")
	_, auth, err := f.Resolve()
	if err != nil || auth.Password != "typed" {
		t.Fatalf("Resolve = %+v, %v; want the typed password", auth, err)
	}
	if len(*prompts) != 1 || !strings.Contains((*prompts)[0], "alice") || !strings.Contains((*prompts)[0], "prod") {
		t.Fatalf("prompts = %q", *prompts)
	}

	// --brokers with a username prompts too, through the TUI's resolver.
	prompts = stubTerminal(t, "typed2")
	_, auth, _, err = resolveConnection(UIFlags{Brokers: "h:9092", Username: "bob"})
	if err != nil || auth.Password != "typed2" || len(*prompts) != 1 {
		t.Fatalf("resolveConnection = %+v, %v, prompts %q", auth, err, *prompts)
	}
}
