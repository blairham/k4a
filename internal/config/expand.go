// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"regexp"
)

// envRef matches a value that is, in its entirety, one environment variable
// reference: $NAME or ${NAME}.
var envRef = regexp.MustCompile(`^\$(?:([A-Za-z_][A-Za-z0-9_]*)|\{([A-Za-z_][A-Za-z0-9_]*)\})$`)

// ExpandRef resolves a value that is exactly `$NAME` or `${NAME}` to that
// environment variable (empty when unset) and returns anything else
// unchanged. Running os.ExpandEnv over a password mangled every literal `$`
// in it (`pa$$word` became `pa` plus whatever `$word` held); a whole-value
// reference keeps "name an env var instead of storing the secret" working
// without touching values that merely contain a `$`.
func ExpandRef(s string) string {
	m := envRef.FindStringSubmatch(s)
	if m == nil {
		return s
	}
	return os.Getenv(m[1] + m[2])
}
