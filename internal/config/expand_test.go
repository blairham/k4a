// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import "testing"

func TestExpandRef(t *testing.T) {
	t.Setenv("K4A_TEST_PASS", "s3cret")
	t.Setenv("word", "LEAK")

	tests := []struct{ in, want string }{
		{"${K4A_TEST_PASS}", "s3cret"},
		{"$K4A_TEST_PASS", "s3cret"},
		{"${K4A_UNSET_VAR_99999}", ""},
		{"pa$$word", "pa$$word"},
		{"pa$word", "pa$word"},
		{"x${K4A_TEST_PASS}", "x${K4A_TEST_PASS}"},
		{"$", "$"},
		{"literal", "literal"},
	}
	for _, tt := range tests {
		if got := ExpandRef(tt.in); got != tt.want {
			t.Errorf("ExpandRef(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// Credentials and paths in a context share the rule: a whole-value reference
// resolves, a literal `$` survives. Paths used not to expand at all, while the
// same --cert flag did.
func TestToAuthConfigExpandsOnlyWholeReferences(t *testing.T) {
	t.Setenv("K4A_TEST_USER", "admin")
	t.Setenv("K4A_TEST_CA", "/etc/k4a/ca.pem")
	t.Setenv("word", "LEAK")

	ctx := &Context{
		Username: "${K4A_TEST_USER}",
		Password: "pa$$word", // pragma: allowlist secret
		CAFile:   "$K4A_TEST_CA",
		CertFile: "/certs/$word.pem",
	}
	a := ctx.ToAuthConfig()
	if a.Username != "admin" {
		t.Errorf("Username = %q, want admin", a.Username)
	}
	if a.Password != "pa$$word" { // pragma: allowlist secret
		t.Errorf("Password = %q, want it unchanged", a.Password)
	}
	if a.CAFile != "/etc/k4a/ca.pem" {
		t.Errorf("CAFile = %q, want /etc/k4a/ca.pem", a.CAFile)
	}
	if a.CertFile != "/certs/$word.pem" {
		t.Errorf("CertFile = %q, want it unchanged", a.CertFile)
	}
}
