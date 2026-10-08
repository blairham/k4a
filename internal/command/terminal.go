// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import "fmt"

const bgColor = "#000000"

// setTerminalBg sets the terminal background and iTerm2 tab color
// to match the k4a dark theme, like k9s does.
func setTerminalBg() {
	// OSC 11: set terminal background color (works in iTerm2, kitty, alacritty, etc.).
	// iTerm2 automatically matches the tab color to the terminal background.
	fmt.Printf("\033]11;%s\a", bgColor)

	// iTerm2 proprietary: explicitly set the tab background color (RGB 10, 10, 10).
	fmt.Print("\033]6;1;bg;red;brightness;10\a")
	fmt.Print("\033]6;1;bg;green;brightness;10\a")
	fmt.Print("\033]6;1;bg;blue;brightness;10\a")
}

// resetTerminalBg restores the terminal background and iTerm2 tab color
// to their defaults on exit.
func resetTerminalBg() {
	// OSC 111: reset terminal background to default.
	fmt.Print("\033]111\a")

	// iTerm2 proprietary: reset tab color to default.
	fmt.Print("\033]6;1;bg;*;default\a")
}
