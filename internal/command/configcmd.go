// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/charmbracelet/log"
	"gopkg.in/yaml.v3"

	"github.com/blairham/k4a/internal/config"
)

// ConfigCommand provides get/set/show access to the top-level k4a
// preferences (UI and index settings). Context management lives in
// `k4a contexts` and `k4a use-context`; this command is for the
// non-context knobs.
type ConfigCommand struct{}

// Help returns the help text.
func (c *ConfigCommand) Help() string {
	return `Usage: k4a config <subcommand> [args]

  Inspect and modify k4a's top-level configuration (~/.k4a/config.yaml).
  Context management lives in "k4a contexts" / "k4a use-context"; this
  command is for the non-context preferences.

Subcommands:
  show               Print the current config (YAML)
  path               Print the config file path
  get <key>          Print one value (e.g., k4a config get index.enabled)
  set <key> <value>  Set one value (e.g., k4a config set index.enabled true)

Supported keys:
  ui.logoless        bool — hide the ASCII logo in the TUI
  ui.readonly        bool — block destructive operations
  index.enabled      bool — enable the local Tier 3 index (populated by
                            the live tail when a topic is open)

Examples:
  k4a config show
  k4a config get index.enabled
  k4a config set index.enabled true
  k4a config set ui.logoless false`
}

// Synopsis returns the one-line description.
func (c *ConfigCommand) Synopsis() string {
	return "Show or change top-level k4a preferences"
}

// Run dispatches on the first arg.
func (c *ConfigCommand) Run(args []string) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		fmt.Println(c.Help())
		return 0
	}

	switch strings.ToLower(args[0]) {
	case "show":
		return runConfigShow()
	case "path":
		fmt.Println(config.DefaultConfigPath())
		return 0
	case "get":
		if len(args) != 2 {
			log.Error("usage: k4a config get <key>")
			return 1
		}
		return runConfigGet(args[1])
	case "set":
		if len(args) != 3 {
			log.Error("usage: k4a config set <key> <value>")
			return 1
		}
		return runConfigSet(args[1], args[2])
	default:
		log.Error("unknown config subcommand", "subcommand", args[0])
		fmt.Println(c.Help())
		return 1
	}
}

func runConfigShow() int {
	cfg, err := config.Load(config.DefaultConfigPath())
	if err != nil {
		log.Error("loading config", "err", err)
		return 1
	}
	body, err := yaml.Marshal(cfg)
	if err != nil {
		log.Error("rendering config", "err", err)
		return 1
	}
	os.Stdout.Write(body) //nolint:errcheck // best-effort stdout write
	return 0
}

func runConfigGet(key string) int {
	cfg, err := config.Load(config.DefaultConfigPath())
	if err != nil {
		log.Error("loading config", "err", err)
		return 1
	}
	val, err := configGet(cfg, key)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	fmt.Println(val)
	return 0
}

func runConfigSet(key, value string) int {
	cfgPath := config.DefaultConfigPath()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("loading config", "err", err)
		return 1
	}
	if err := configSet(cfg, key, value); err != nil {
		log.Error(err.Error())
		return 1
	}
	if err := config.Save(cfgPath, cfg); err != nil {
		log.Error("saving config", "err", err)
		return 1
	}
	fmt.Printf("Set %s = %s\n", key, value)
	return 0
}

// configGet looks up the value at the dotted key. Returns a stringified
// representation suitable for shell consumption.
func configGet(cfg *config.Config, key string) (string, error) {
	switch strings.ToLower(key) {
	case "ui.logoless":
		return fmt.Sprintf("%t", cfg.UI.Logoless), nil
	case "ui.readonly":
		return fmt.Sprintf("%t", cfg.UI.Readonly), nil
	case "index.enabled":
		return fmt.Sprintf("%t", cfg.Index.Enabled), nil
	default:
		return "", fmt.Errorf("unknown config key %q (see `k4a config --help`)", key)
	}
}

// configSet parses value into the right type for key and mutates cfg.
// Returns an error on unknown keys or unparseable values.
func configSet(cfg *config.Config, key, value string) error {
	switch strings.ToLower(key) {
	case "ui.logoless":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("ui.logoless: %w", err)
		}
		cfg.UI.Logoless = b
	case "ui.readonly":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("ui.readonly: %w", err)
		}
		cfg.UI.Readonly = b
	case "index.enabled":
		b, err := parseBool(value)
		if err != nil {
			return fmt.Errorf("index.enabled: %w", err)
		}
		cfg.Index.Enabled = b
	default:
		return fmt.Errorf("unknown config key %q (see `k4a config --help`)", key)
	}
	return nil
}

// parseBool accepts true/false/yes/no/on/off/1/0 (case-insensitive).
// strconv.ParseBool handles most of these; we wrap it for friendlier
// errors.
func parseBool(s string) (bool, error) {
	trimmed := strings.TrimSpace(s)
	switch strings.ToLower(trimmed) {
	case "yes", "on":
		return true, nil
	case "no", "off":
		return false, nil
	}
	b, err := strconv.ParseBool(trimmed)
	if err != nil {
		return false, fmt.Errorf("expected a boolean (true/false), got %q", s)
	}
	return b, nil
}
