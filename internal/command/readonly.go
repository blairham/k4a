// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/state"
)

// checkWritable refuses op when ui.readonly is set in the config at cfgPath
// (the default config when empty). Every mutating subcommand calls it before
// doing anything, so the setting means the same thing headless as in the
// TUI. A config that exists but cannot be read refuses too: the guard rail
// must not fall away because the file it lives in is broken.
func checkWritable(cfgPath, op string) error {
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("%s: cannot check ui.readonly: %w", op, err)
	}
	if cfg.UI.Readonly {
		return fmt.Errorf("%s is disabled: ui.readonly is set in %s (k4a config set ui.readonly false)", op, cfgPath)
	}
	return nil
}

// checkWritable is checkWritable against the config these flags select.
func (f *ConnectionFlags) checkWritable(op string) error {
	return checkWritable(f.ConfigFile, op)
}

// checkRootWritable is checkWritable for commands that act on a state root
// rather than a cluster: the config read is the one inside that root.
func checkRootWritable(root *state.Root, op string) error {
	return checkWritable(filepath.Join(root.Path(), "config.yaml"), op)
}

// stdinIsTerminal reports whether stdin is interactive. A prompt on a pipe or
// /dev/null would either hang or read an answer nobody typed.
func stdinIsTerminal() bool {
	fi, err := os.Stdin.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// confirm asks question on out and accepts only y or yes from in. A
// non-interactive caller is refused rather than prompted, and must pass the
// command's --yes instead.
func confirm(in io.Reader, out io.Writer, interactive bool, question string) error {
	if !interactive {
		return errors.New("stdin is not a terminal; pass --yes to confirm")
	}
	fmt.Fprintf(out, "%s [y/N]: ", question) //nolint:errcheck // prompt; a failed write shows up as no answer
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading confirmation: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return nil
	default:
		return errors.New("aborted")
	}
}
