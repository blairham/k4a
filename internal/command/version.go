// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import "fmt"

// VersionCommand implements the "version" CLI command.
type VersionCommand struct {
	Version string
	Commit  string
	Date    string
}

// Help returns the help text.
func (c *VersionCommand) Help() string {
	return "Print version information."
}

// Synopsis returns the one-line description.
func (c *VersionCommand) Synopsis() string {
	return "Print version information"
}

// Run prints the version.
func (c *VersionCommand) Run(_ []string) int {
	fmt.Printf("k4a %s (commit: %s, built: %s)\n", c.Version, c.Commit, c.Date)
	return 0
}
