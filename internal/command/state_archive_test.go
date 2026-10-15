// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/state"
)

const secretConfig = `current-context: prod
contexts:
  prod:
    brokers: b:9092
    auth: scram
    username: alice
    password: inline-pass
    oauth-client-secret: ${CLIENT_SECRET}
`

func newStateCmd(t *testing.T, root string) (*StateCommand, *bytes.Buffer) {
	t.Helper()
	var out, errOut bytes.Buffer
	return &StateCommand{
		Root:   state.NewRoot(root),
		Out:    &out,
		Err:    &errOut,
		Prompt: Prompt{In: strings.NewReader(""), Out: &out, Interactive: func() bool { return false }},
	}, &errOut
}

// archivedConfig imports the archive into a scratch root and returns the
// config.yaml it carried.
func archivedConfig(t *testing.T, archive string) string {
	t.Helper()
	plan, err := state.InspectArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	scratch := state.NewRoot(t.TempDir())
	if _, aerr := plan.Apply(scratch); aerr != nil {
		t.Fatal(aerr)
	}
	body, err := os.ReadFile(filepath.Join(scratch.Path(), "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestStateExportStripsInlineSecrets(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeConfig(t, root, secretConfig)
	archive := filepath.Join(t.TempDir(), "s.tar.gz")

	cmd, errOut := newStateCmd(t, root)
	if code := cmd.Run([]string{"export", "-o", archive}); code != 0 {
		t.Fatalf("export exit %d: %s", code, errOut)
	}
	info, err := os.Stat(archive)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("archive mode = %o, want 600", perm)
	}
	cfg := archivedConfig(t, archive)
	if strings.Contains(cfg, "inline-pass") {
		t.Errorf("inline password exported:\n%s", cfg)
	}
	if !strings.Contains(cfg, "${CLIENT_SECRET}") || !strings.Contains(cfg, "alice") {
		t.Errorf("$VAR reference or non-secret setting lost:\n%s", cfg)
	}
	if !strings.Contains(errOut.String(), "prod.password") {
		t.Errorf("stripped secret not reported: %q", errOut)
	}
}

func TestStateExportIncludeSecretsWarns(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeConfig(t, root, secretConfig)
	archive := filepath.Join(t.TempDir(), "s.tar.gz")

	cmd, errOut := newStateCmd(t, root)
	if code := cmd.Run([]string{"export", "--include-secrets", "-o", archive}); code != 0 {
		t.Fatalf("export exit %d: %s", code, errOut)
	}
	if !strings.Contains(errOut.String(), "WARNING") || !strings.Contains(errOut.String(), "prod.password") {
		t.Errorf("no loud warning for exported secrets: %q", errOut)
	}
	if cfg := archivedConfig(t, archive); !strings.Contains(cfg, "inline-pass") {
		t.Errorf("--include-secrets dropped the secret:\n%s", cfg)
	}

	dst := t.TempDir()
	imp, impErr := newStateCmd(t, dst)
	if code := imp.Run([]string{"import", archive}); code != 0 {
		t.Fatalf("import exit %d: %s", code, impErr)
	}
	if !strings.Contains(impErr.String(), "WARNING") {
		t.Errorf("import of an archive with secrets did not warn: %q", impErr)
	}
}

func TestStateExportRefusesInsideStateRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cmd, _ := newStateCmd(t, root)
	if code := cmd.Run([]string{"export", "-o", filepath.Join(root, "x.tar.gz")}); code == 0 {
		t.Error("export into the state directory succeeded")
	}
}

func exportSession(t *testing.T, body string) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "state.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "s.tar.gz")
	cmd, errOut := newStateCmd(t, src)
	if code := cmd.Run([]string{"export", "-o", archive}); code != 0 {
		t.Fatalf("export exit %d: %s", code, errOut)
	}
	return archive
}

func TestStateImportAsksBeforeReplacing(t *testing.T) {
	t.Parallel()

	archive := exportSession(t, "imported")
	dst := t.TempDir()
	session := filepath.Join(dst, "state.json")
	if err := os.WriteFile(session, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd, _ := newStateCmd(t, dst)
	if code := cmd.Run([]string{"import", archive}); code == 0 {
		t.Error("import replaced existing state without --yes on a non-terminal")
	}
	if body, _ := os.ReadFile(session); string(body) != "mine" { //nolint:errcheck // compared
		t.Errorf("state replaced without confirmation: %q", body)
	}

	cmd, errOut := newStateCmd(t, dst)
	if code := cmd.Run([]string{"import", "--yes", archive}); code != 0 {
		t.Fatalf("import --yes exit %d: %s", code, errOut)
	}
	if body, _ := os.ReadFile(session); string(body) != "imported" { //nolint:errcheck // compared
		t.Errorf("state after import --yes = %q", body)
	}
}

func TestStateImportRespectsReadonly(t *testing.T) {
	t.Parallel()

	archive := exportSession(t, "imported")
	dst := t.TempDir()
	writeConfig(t, dst, "ui:\n  readonly: true\n")

	cmd, _ := newStateCmd(t, dst)
	if code := cmd.Run([]string{"import", "--yes", archive}); code == 0 {
		t.Error("import ran with ui.readonly set")
	}
	if _, err := os.Stat(filepath.Join(dst, "state.json")); !os.IsNotExist(err) {
		t.Errorf("readonly import wrote state: %v", err)
	}
}
