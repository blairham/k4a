// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/config"
)

// ContextsFlags defines flags for the contexts command.
type ContextsFlags struct {
	ConfigFile string `long:"config" description:"Config file path" default:""`
}

// ContextsCommand lists available contexts.
type ContextsCommand struct{}

// Help returns the help text.
func (c *ContextsCommand) Help() string {
	return `Usage: k4a contexts [options]

  List configured Kafka contexts from ~/.k4a/config.yaml.

Options:
      --config=FILE   Config file path (default: ~/.k4a/config.yaml)`
}

// Synopsis returns the one-line description.
func (c *ContextsCommand) Synopsis() string {
	return "List configured Kafka contexts"
}

// Run lists contexts.
func (c *ContextsCommand) Run(args []string) int {
	var flags ContextsFlags
	parser := goflags.NewParser(&flags, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	cfgPath := flags.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return 1
	}

	if len(cfg.Contexts) == 0 {
		log.Info("no contexts configured", "config", cfgPath)
		return 0
	}

	for name, ctx := range cfg.Contexts {
		marker := "  "
		if name == cfg.CurrentContext {
			marker = "* "
		}
		source := ctx.Brokers
		if source == "" && ctx.SSMBrokers != "" {
			source = fmt.Sprintf("ssm:%s", ctx.SSMBrokers)
		}
		fmt.Printf("%s%-20s auth=%-10s brokers=%s\n", marker, name, ctx.Auth, source)
	}

	return 0
}

// UseContextCommand switches the current context.
type UseContextCommand struct{}

// Help returns the help text.
func (c *UseContextCommand) Help() string {
	return `Usage: k4a use-context <name>

  Set the current-context in ~/.k4a/config.yaml.`
}

// Synopsis returns the one-line description.
func (c *UseContextCommand) Synopsis() string {
	return "Set the current context"
}

// Run switches the current context.
func (c *UseContextCommand) Run(args []string) int {
	if len(args) == 0 {
		log.Error("context name required")
		return 1
	}

	name := args[0]
	cfgPath := config.DefaultConfigPath()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Error("failed to load config", "err", err)
		return 1
	}

	if _, ok := cfg.Contexts[name]; !ok {
		log.Error("context not found", "name", name)
		return 1
	}

	cfg.CurrentContext = name
	if err := config.Save(cfgPath, cfg); err != nil {
		log.Error("failed to save config", "err", err)
		return 1
	}

	log.Info("switched context", "name", name)
	return 0
}
