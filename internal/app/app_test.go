// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/version"
)

// documentedSubcommands reads the subcommand list from AGENTS.md, so the
// table and the document that agents and contributors read cannot drift.
func documentedSubcommands(t *testing.T) []string {
	t.Helper()
	doc, err := os.ReadFile(filepath.Join("..", "..", "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(doc)
	start := strings.Index(s, "### Subcommands")
	if start < 0 {
		t.Fatal("AGENTS.md has no subcommand section to check against")
	}
	// The list is the first paragraph under the heading; later paragraphs
	// are prose that quotes other code spans.
	paras := strings.SplitN(s[start:], "\n\n", 3)
	if len(paras) < 2 {
		t.Fatal("AGENTS.md subcommand section has no list paragraph")
	}
	list := paras[1]
	matches := regexp.MustCompile("`([a-z][a-z-]*)`").FindAllStringSubmatch(list, -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, m[1])
	}
	if len(names) < 10 {
		t.Fatalf("parsed only %d subcommands from AGENTS.md: %v", len(names), names)
	}
	return names
}

// Every documented subcommand resolves to a working command, and every
// command in the table is documented.
func TestSubcommandTableMatchesDocs(t *testing.T) {
	t.Parallel()

	table := commands(version.Info{Version: "v0.0.0-test"})
	documented := documentedSubcommands(t)

	for _, name := range documented {
		factory, ok := table[name]
		if !ok {
			t.Errorf("documented subcommand %q is not in the table", name)
			continue
		}
		cmd, err := factory()
		if err != nil || cmd == nil {
			t.Errorf("%s: factory returned (%v, %v)", name, cmd, err)
			continue
		}
		if cmd.Synopsis() == "" || cmd.Help() == "" {
			t.Errorf("%s: empty Synopsis or Help", name)
		}
	}
	for name := range table {
		if !slices.Contains(documented, name) {
			t.Errorf("subcommand %q is in the table but not in AGENTS.md", name)
		}
	}
}

// runWith runs the CLI as if invoked with args, with HOME on a temp dir so
// nothing reads or writes the real ~/.k4a, and returns the exit status and
// what reached stdout and stderr.
func runWith(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))

	origArgs, origOut, origErr := os.Args, os.Stdout, os.Stderr
	t.Cleanup(func() {
		os.Args, os.Stdout, os.Stderr = origArgs, origOut, origErr
		log.SetOutput(origErr)
	})

	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outC, errC := drain(outR), drain(errR)

	os.Args = append([]string{"k4a"}, args...)
	// The default logger holds the stderr it was created with, so it is
	// redirected explicitly rather than through os.Stderr.
	os.Stdout, os.Stderr = outW, errW
	log.SetOutput(errW)
	code = Run()
	os.Stdout, os.Stderr = origOut, origErr
	log.SetOutput(origErr)
	_ = outW.Close()
	_ = errW.Close()
	return code, <-outC, <-errC
}

func drain(r *os.File) <-chan string {
	c := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		_ = r.Close()
		c <- string(b)
	}()
	return c
}

// A known subcommand runs that command.
func TestRunDispatchesKnownSubcommand(t *testing.T) {
	code, out, _ := runWith(t, "version")
	if code != 0 {
		t.Fatalf("k4a version = %d, want 0", code)
	}
	if !strings.HasPrefix(out, "k4a ") || !strings.Contains(out, "commit:") {
		t.Errorf("k4a version printed %q", out)
	}
}

// An unrecognized first argument is flags for ui: `k4a --context NAME`
// reaches ui's context resolution (and fails there on a missing context)
// rather than the CLI rejecting "--context" as an unknown subcommand.
func TestRunRoutesUnknownFirstArgToUI(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(cfg, []byte("contexts:\n  staging:\n    brokers: 127.0.0.1:1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, stderr := runWith(t, "--config", cfg, "--context", "no-such-context")
	if code != 1 {
		t.Errorf("k4a --context no-such-context = %d, want ui's 1", code)
	}
	if !strings.Contains(stderr, "no-such-context") {
		t.Errorf("stderr %q does not show ui resolving the context", stderr)
	}
}

// --help is not routed to ui: it lists the subcommands.
func TestRunHelpListsSubcommands(t *testing.T) {
	_, _, stderr := runWith(t, "--help")
	for _, name := range []string{"create-topic", "list-acls", "use-context"} {
		if !strings.Contains(stderr, name) {
			t.Errorf("k4a --help does not list %q:\n%s", name, stderr)
		}
	}
}
