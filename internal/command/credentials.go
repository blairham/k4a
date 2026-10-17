// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"golang.org/x/term"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/secrets"
)

// secretStore is the keychain every command reads and writes. It is a
// variable so tests swap in secrets.Memory (see TestMain) and never reach the
// real keychain.
var secretStore = secrets.Keyring

// interactiveStdin and readPassword are the terminal behind the password
// prompt, swapped out in tests.
var (
	interactiveStdin = stdinIsTerminal
	readPassword     = func(prompt string) (string, error) {
		fmt.Fprint(os.Stderr, prompt) //nolint:errcheck // prompt; a failed write shows up as no answer
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr) //nolint:errcheck // the newline the hidden input swallowed
		return string(b), err
	}
)

// fillSecrets completes auth after flags and env have been laid over the
// context: first from the keychain entries ctx names, then, for a password
// still missing, by asking on the terminal. ctx is nil for a --brokers
// connection, which has only the prompt.
func fillSecrets(ctx *config.Context, who string, auth kafka.AuthConfig) (kafka.AuthConfig, error) {
	if ctx != nil {
		var err error
		if auth, err = ctx.ApplyKeychain(auth, secretStore()); err != nil {
			return auth, err
		}
	}
	auth, err := promptPassword(auth, who)
	return withTokenStore(auth, secretStore()), err
}

// withTokenStore caches an interactive OIDC login's tokens in the keychain
// (the test store under TestMain), or the owner-only file store when the
// keychain cannot take them.
func withTokenStore(auth kafka.AuthConfig, store secrets.Store) kafka.AuthConfig {
	if auth.OAuth.Interactive() && auth.OAuth.Store == nil && store != nil {
		auth.OAuth.Store = kafka.KeychainTokenStore{Keychain: store, Fallback: kafka.DefaultTokenStore()}
	}
	return auth
}

// promptPassword asks for a missing SASL password when stdin is a terminal.
// A pipe or /dev/null is left alone: the connection then fails with the
// usual "requires a username and password", which is the honest answer for
// a script.
func promptPassword(auth kafka.AuthConfig, who string) (kafka.AuthConfig, error) {
	if !auth.NeedsPassword() || !interactiveStdin() {
		return auth, nil
	}
	pw, err := readPassword(fmt.Sprintf("Password for %s (%s): ", auth.Username, who))
	if err != nil {
		return auth, fmt.Errorf("reading password: %w", err)
	}
	auth.Password = pw
	return auth, nil
}

// CredentialFlags are the flags of set-credential and delete-credential.
type CredentialFlags struct {
	ConfigFile string `long:"config" description:"Config file path (default: ~/.k4a/config.yaml)"`
	Field      string `long:"field"  description:"Secret to store: password, oauth-token or oauth-client-secret" default:"password" choice:"password" choice:"oauth-token" choice:"oauth-client-secret"`
}

// parseCredentialArgs parses flags and the single context argument.
func parseCredentialArgs(args []string) (CredentialFlags, string, bool, error) {
	var flags CredentialFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	rest, err := parser.ParseArgs(args)
	if err != nil {
		if goflags.WroteHelp(err) {
			return flags, "", true, nil
		}
		return flags, "", false, err
	}
	if len(rest) != 1 {
		return flags, "", false, errors.New("exactly one context name required")
	}
	if flags.ConfigFile == "" {
		flags.ConfigFile = config.DefaultConfigPath()
	}
	return flags, rest[0], false, nil
}

// loadContext loads the config and the named context from it.
func loadContext(path, name string) (*config.Config, config.Context, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return nil, config.Context{}, err
	}
	ctx, ok := cfg.Contexts[name]
	if !ok {
		return nil, config.Context{}, fmt.Errorf(
			"%w: %q (available: %s)",
			config.ErrContextNotFound,
			name,
			cfg.ContextNames(),
		)
	}
	return cfg, ctx, nil
}

// SetCredentialCommand stores a context secret in the OS keychain.
type SetCredentialCommand struct {
	// in replaces stdin in tests.
	in io.Reader
}

// Help returns the help text.
func (c *SetCredentialCommand) Help() string {
	return `Usage: k4a set-credential [options] CONTEXT

  Store a context's secret in the OS keychain (macOS Keychain, the Secret
  Service on Linux, Windows Credential Manager) and point the context at it.
  The entry is service "k4a", account "CONTEXT/FIELD"; the context gains a
  keychain: reference and loses any literal value of the field, so the
  config file no longer holds the secret.

  On a terminal the secret is typed twice without echo; otherwise it is read
  from stdin (one line), so a password manager can pipe it in.

Options:
      --field=NAME    password (default), oauth-token or oauth-client-secret
      --config=FILE   Config file path (default: ~/.k4a/config.yaml)`
}

// Synopsis returns the one-line description.
func (c *SetCredentialCommand) Synopsis() string {
	return "Store a context secret in the OS keychain"
}

// Run stores the secret.
func (c *SetCredentialCommand) Run(args []string) int {
	if err := c.run(args); err != nil {
		log.Error("set-credential failed", "err", err)
		return 1
	}
	return 0
}

func (c *SetCredentialCommand) run(args []string) error {
	flags, name, help, err := parseCredentialArgs(args)
	if err != nil || help {
		return err
	}
	cfg, ctx, err := loadContext(flags.ConfigFile, name)
	if err != nil {
		return err
	}
	secret, err := c.readSecret(name, flags.Field)
	if err != nil {
		return err
	}
	account := secrets.Account(name, flags.Field)
	if err := ctx.SetKeychainRef(flags.Field, account); err != nil {
		return err
	}
	if err := secretStore().Set(account, secret); err != nil {
		return err
	}
	cfg.Contexts[name] = ctx
	if err := config.Save(flags.ConfigFile, cfg); err != nil {
		return fmt.Errorf("stored in the keychain, but saving the reference failed: %w", err)
	}
	log.Info("stored in the keychain", "context", name, "field", flags.Field, "account", secrets.Service+"/"+account)
	return nil
}

// readSecret reads the secret: typed twice without echo on a terminal, or one
// line from a pipe.
func (c *SetCredentialCommand) readSecret(name, field string) (string, error) {
	if c.in != nil || !interactiveStdin() {
		in := c.in
		if in == nil {
			in = os.Stdin
		}
		line, err := bufio.NewReader(in).ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", fmt.Errorf("reading %s: %w", field, err)
		}
		return nonEmpty(strings.TrimRight(line, "\r\n"), field)
	}
	first, err := readPassword(fmt.Sprintf("%s for %s: ", field, name))
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", field, err)
	}
	again, err := readPassword("again: ")
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", field, err)
	}
	if first != again {
		return "", errors.New("the two entries differ; nothing stored")
	}
	return nonEmpty(first, field)
}

func nonEmpty(s, field string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("empty %s; nothing stored", field)
	}
	return s, nil
}

// DeleteCredentialCommand removes a context secret from the OS keychain.
type DeleteCredentialCommand struct{}

// Help returns the help text.
func (c *DeleteCredentialCommand) Help() string {
	return `Usage: k4a delete-credential [options] CONTEXT

  Remove a context's secret from the OS keychain and drop the context's
  keychain: reference to it. An entry that is already gone is not an error.

Options:
      --field=NAME    password (default), oauth-token or oauth-client-secret
      --config=FILE   Config file path (default: ~/.k4a/config.yaml)`
}

// Synopsis returns the one-line description.
func (c *DeleteCredentialCommand) Synopsis() string {
	return "Remove a context secret from the OS keychain"
}

// Run removes the secret.
func (c *DeleteCredentialCommand) Run(args []string) int {
	if err := c.run(args); err != nil {
		log.Error("delete-credential failed", "err", err)
		return 1
	}
	return 0
}

func (c *DeleteCredentialCommand) run(args []string) error {
	flags, name, help, err := parseCredentialArgs(args)
	if err != nil || help {
		return err
	}
	cfg, ctx, err := loadContext(flags.ConfigFile, name)
	if err != nil {
		return err
	}
	account, err := ctx.ClearKeychainRef(flags.Field)
	if err != nil {
		return err
	}
	if account == "" {
		// A reference written by hand may be gone while the entry
		// set-credential made is not; remove that one.
		account = secrets.Account(name, flags.Field)
	}
	if err := secretStore().Delete(account); err != nil && !errors.Is(err, secrets.ErrNotFound) {
		return err
	}
	cfg.Contexts[name] = ctx
	if err := config.Save(flags.ConfigFile, cfg); err != nil {
		return fmt.Errorf("removed from the keychain, but saving the config failed: %w", err)
	}
	log.Info("removed from the keychain", "context", name, "field", flags.Field, "account", secrets.Service+"/"+account)
	return nil
}
