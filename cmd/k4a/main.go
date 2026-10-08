// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command k4a is the interactive Kafka TUI/CLI — like k9s, but for Kafka.
// This is the canonical build target (GoReleaser, the Makefile, `go install`).
// The CLI wiring lives in internal/app so the repo-root main.go can share it.
package main

import (
	"os"

	"github.com/blairham/k4a/internal/app"
)

func main() { os.Exit(app.Run()) }
