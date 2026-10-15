// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStripInlineSecrets(t *testing.T) {
	t.Parallel()

	in := `# old password: hunter2
current-context: prod
x-defaults: &creds
  password: anchored-secret
contexts:
  prod:
    brokers: b:9092
    auth: scram
    username: alice
    password: inline-pass
    oauth-client-secret: ${CLIENT_SECRET}
    index-token: tok-123
    schema-registry:
      url: https://sr
      password: $SR_PASS
      token: sr-inline
  dev:
    <<: *creds
    auth: plain
ui:
  readonly: true
`
	out, stripped, err := StripInlineSecrets([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"dev.password", "prod.index-token", "prod.password", "prod.schema-registry.token",
	}
	if !slices.Equal(stripped, want) {
		t.Errorf("stripped = %v, want %v", stripped, want)
	}
	for _, secret := range []string{"inline-pass", "tok-123", "sr-inline", "anchored-secret", "hunter2"} {
		if strings.Contains(string(out), secret) {
			t.Errorf("output still holds %q:\n%s", secret, out)
		}
	}

	var cfg Config
	if err := yaml.Unmarshal(out, &cfg); err != nil {
		t.Fatal(err)
	}
	prod := cfg.Contexts["prod"]
	if prod.OAuth.ClientSecret != "${CLIENT_SECRET}" || prod.SchemaRegistry.Password != "$SR_PASS" {
		t.Errorf("$VAR references were not kept: %+v", prod)
	}
	if prod.Username != "alice" || prod.Brokers != "b:9092" || !cfg.UI.Readonly || cfg.CurrentContext != "prod" {
		t.Errorf("non-secret settings lost: %+v", cfg)
	}
}

func TestIsEnvRef(t *testing.T) {
	t.Parallel()

	for s, want := range map[string]bool{
		"$A": true, "${A_1}": true, "x$A": false, "$A x": false, "pa$$word": false, "": false,
	} {
		if got := IsEnvRef(s); got != want {
			t.Errorf("IsEnvRef(%q) = %v, want %v", s, got, want)
		}
	}
}
