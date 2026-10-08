// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/blairham/k4a/internal/upgrade"
)

// UpgradeCommand implements the "upgrade" CLI command.
type UpgradeCommand struct {
	Version string
}

// Help returns the help text.
func (c *UpgradeCommand) Help() string {
	return `Upgrade k4a to the latest release.

Usage: k4a upgrade [--check]

Options:
  --check    Check for updates without installing`
}

// Synopsis returns the one-line description.
func (c *UpgradeCommand) Synopsis() string {
	return "Upgrade to the latest version"
}

// Run executes the upgrade.
func (c *UpgradeCommand) Run(args []string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if len(args) > 0 && args[0] == "--check" {
		return c.check(ctx)
	}

	if _, err := upgrade.Run(ctx, c.Version, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}

func (c *UpgradeCommand) check(ctx context.Context) int {
	latest, available, err := upgrade.Check(ctx, c.Version)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	if !available {
		fmt.Printf("k4a %s is already the latest version\n", c.Version)
		return 0
	}
	fmt.Printf("Update available: %s -> %s\nRun '%s' to install\n", c.Version, latest, upgrade.Command())
	return 0
}
