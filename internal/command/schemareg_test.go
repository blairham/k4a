// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"strings"
	"testing"

	goflags "github.com/jessevdk/go-flags"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/schemareg"
)

const registryCfg = `current-context: prod
contexts:
  prod:
    brokers: prod:9096
    auth: plaintext
    schema-registry:
      url: https://sr.prod.example.com
      username: ctx-user
      password: $K4A_TEST_SR_PASSWORD
      ca: /ctx/sr-ca.pem
`

func TestRegistryConfigPrecedence(t *testing.T) {
	t.Setenv("K4A_TEST_SR_PASSWORD", "from-env") // pragma: allowlist secret
	path := writeCfg(t, registryCfg)

	got := registryConfig(SchemaRegistryFlags{}, path, "", false)
	want := schemareg.Config{
		URL: "https://sr.prod.example.com", Username: "ctx-user", Password: "from-env", CAFile: "/ctx/sr-ca.pem",
	}
	if got != want {
		t.Errorf("context only = %+v\nwant %+v", got, want)
	}

	got = registryConfig(SchemaRegistryFlags{URL: "https://flag", Token: "tok"}, path, "prod", false)
	want = schemareg.Config{URL: "https://flag", Token: "tok", CAFile: "/ctx/sr-ca.pem"}
	if got != want {
		t.Errorf("flags over context = %+v\nwant %+v", got, want)
	}

	if got := registryConfig(SchemaRegistryFlags{}, path, "", true); got.IsSet() {
		t.Errorf("a --brokers connection read the context's registry: %+v", got)
	}
}

// The environment fills a flag that was not given, and a given flag wins
// over it: go-flags' env tags, checked here so a renamed tag cannot drop it.
func TestRegistryFlagsFromEnvironment(t *testing.T) {
	t.Setenv("K4A_SCHEMA_REGISTRY_URL", "https://env")
	t.Setenv("K4A_SCHEMA_REGISTRY_TOKEN", "env-token")
	var f ConsumeFlags
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs(
		[]string{"-t", "x", "--schema-registry-url", "https://flag"},
	); err != nil {
		t.Fatal(err)
	}
	if f.SchemaRegistry.URL != "https://flag" || f.SchemaRegistry.Token != "env-token" {
		t.Errorf("flags = %+v", f.SchemaRegistry)
	}
	if f.Decode != "auto" {
		t.Errorf("--decode default = %q", f.Decode)
	}
	if _, err := goflags.NewParser(&f, goflags.None).ParseArgs([]string{"-t", "x", "--decode", "json"}); err == nil {
		t.Error("--decode accepted a value outside auto|raw")
	}
}

func TestConsumeDecoderRawAndWarnings(t *testing.T) {
	t.Setenv("K4A_SCHEMA_REGISTRY_URL", "")
	path := writeCfg(t, registryCfg)
	d, err := newConsumeDecoder(ConsumeFlags{Decode: "raw", ConnectionFlags: ConnectionFlags{ConfigFile: path}})
	if err != nil || d.reg != nil {
		t.Fatalf("--decode raw built a registry: %v, %v", d.reg, err)
	}

	d, err = newConsumeDecoder(ConsumeFlags{
		Decode:          "auto",
		ConnectionFlags: ConnectionFlags{Brokers: "b:9092"},
		SchemaRegistry:  SchemaRegistryFlags{URL: "http://127.0.0.1:1"},
	})
	if err != nil || d.reg == nil {
		t.Fatalf("auto with a URL: %v, %v", d.reg, err)
	}
	var stderr bytes.Buffer
	d.stderr = &stderr
	for range 3 {
		m := kafka.ConsumedMessage{Topic: "t", Value: "raw", RawValue: []byte{0, 0, 0, 0, 1, 'x'}}
		d.decode(context.Background(), &m)
		if m.Value != "raw" || m.ValueDecoding.Reason == "" {
			t.Fatalf("unreachable registry: %+v", m)
		}
	}
	if n := strings.Count(stderr.String(), "warning:"); n != 1 {
		t.Errorf("%d warnings for one reason, want 1:\n%s", n, stderr.String())
	}

	if _, err := newConsumeDecoder(ConsumeFlags{
		Decode:          "auto",
		ConnectionFlags: ConnectionFlags{Brokers: "b:9092"},
		SchemaRegistry:  SchemaRegistryFlags{URL: "http://sr.example.com", Password: "p"},
	}); err == nil || !strings.Contains(err.Error(), "https") {
		t.Errorf("credentials over plain http to a remote registry: err = %v", err)
	}
}
