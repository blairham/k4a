// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package app wires up the k4a CLI/TUI (subcommand routing over hashicorp/cli).
// It lives in a library package so BOTH the canonical entry point (cmd/k4a) and
// the repo-root main.go (`go run main.go` for local dev) delegate to one
// implementation — there is no second copy of the command table to drift.
package app

import (
	"fmt"
	"os"

	"github.com/hashicorp/cli"

	"github.com/blairham/k4a/internal/command"
	"github.com/blairham/k4a/internal/version"
)

// Run builds the CLI and executes it, returning the process exit status. The
// version/commit/date are resolved dynamically (ldflags override → build info)
// by internal/version, so callers pass nothing.
func Run() int {
	info := version.Get()

	c := cli.NewCLI("k4a", info.Version)
	c.Args = os.Args[1:]

	c.Commands = map[string]cli.CommandFactory{
		"ui": func() (cli.Command, error) {
			return &command.UICommand{Version: info.Version}, nil
		},
		"produce": func() (cli.Command, error) {
			return &command.ProduceCommand{}, nil
		},
		"consume": func() (cli.Command, error) {
			return &command.ConsumeCommand{}, nil
		},
		"verify": func() (cli.Command, error) {
			return &command.VerifyCommand{}, nil
		},
		"create-topic": func() (cli.Command, error) {
			return &command.CreateTopicCmd{}, nil
		},
		"create-acl": func() (cli.Command, error) {
			return &command.CreateACLCmd{}, nil
		},
		"delete-topic": func() (cli.Command, error) {
			return &command.DeleteTopicCmd{}, nil
		},
		"describe-topic": func() (cli.Command, error) {
			return &command.DescribeTopicCmd{}, nil
		},
		"topic-size": func() (cli.Command, error) {
			return &command.TopicSizeCmd{}, nil
		},
		"list-acls": func() (cli.Command, error) {
			return &command.ListACLsCmd{}, nil
		},
		"alter-config": func() (cli.Command, error) {
			return &command.AlterConfigCmd{}, nil
		},
		"alter-replication": func() (cli.Command, error) {
			return &command.AlterReplicationCmd{}, nil
		},
		"contexts": func() (cli.Command, error) {
			return &command.ContextsCommand{}, nil
		},
		"use-context": func() (cli.Command, error) {
			return &command.UseContextCommand{}, nil
		},
		"version": func() (cli.Command, error) {
			return &command.VersionCommand{
				Version: info.Version,
				Commit:  info.Commit,
				Date:    info.Date,
			}, nil
		},
		"upgrade": func() (cli.Command, error) {
			return &command.UpgradeCommand{Version: info.Version}, nil
		},
		"state": func() (cli.Command, error) {
			return &command.StateCommand{}, nil
		},
		"config": func() (cli.Command, error) {
			return &command.ConfigCommand{}, nil
		},
		"search": func() (cli.Command, error) {
			return &command.SearchCommand{}, nil
		},
		"mcp": func() (cli.Command, error) {
			return &command.MCPCommand{Version: info.Version}, nil
		},
	}

	// Default to "ui" when the first arg is not a known subcommand.
	if len(os.Args) > 1 {
		if _, ok := c.Commands[os.Args[1]]; !ok && os.Args[1] != "--help" && os.Args[1] != "-h" {
			c.Args = append([]string{"ui"}, os.Args[1:]...)
		}
	} else {
		c.Args = append([]string{"ui"}, c.Args...)
	}

	exitStatus, err := c.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	return exitStatus
}
