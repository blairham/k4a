// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"os"
	"testing"

	"github.com/blairham/k4a/internal/secrets"
)

// testStore is what every command in this package's tests reads and writes
// instead of the OS keychain, and no test sees a terminal to prompt on.
var testStore = secrets.NewMemory()

func TestMain(m *testing.M) {
	secretStore = func() secrets.Store { return testStore }
	interactiveStdin = func() bool { return false }
	readPassword = func(string) (string, error) {
		panic("readPassword called without a test stubbing it")
	}
	os.Exit(m.Run())
}
