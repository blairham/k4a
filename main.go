// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command k4a (repo-root shim) exists so `go run main.go` / `go run .` launches
// the CLI from the repo root during local development. The canonical build
// target is ./cmd/k4a; both are thin wrappers over internal/app, so there is a
// single implementation of the command table.
package main

import (
	"os"

	"github.com/blairham/k4a/internal/app"
)

func main() { os.Exit(app.Run()) }
