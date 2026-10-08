// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/smithy-go"

	"github.com/blairham/k4a/internal/kafka"
)

func TestLoadNonexistent(t *testing.T) {
	t.Parallel()

	cfg, err := Load("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("Load nonexistent should return zero config, got error: %v", err)
	}
	if cfg.CurrentContext != "" {
		t.Errorf("expected empty CurrentContext, got %q", cfg.CurrentContext)
	}
	if len(cfg.Contexts) != 0 {
		t.Errorf("expected empty Contexts, got %d", len(cfg.Contexts))
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("{{invalid yaml"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error parsing invalid YAML")
	}
}

func TestLoadValid(t *testing.T) {
	t.Parallel()

	yaml := `current-context: staging
contexts:
  staging:
    brokers: broker1:9092,broker2:9092
    auth: scram
    username: admin
    password: secret
    region: us-east-1
  production:
    ssm-brokers: /prod/brokers
    auth: iam
    region: us-west-2
    profile: Production/Admin
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.CurrentContext != "staging" {
		t.Errorf("CurrentContext = %q, want %q", cfg.CurrentContext, "staging")
	}
	if len(cfg.Contexts) != 2 {
		t.Fatalf("expected 2 contexts, got %d", len(cfg.Contexts))
	}

	stg := cfg.Contexts["staging"]
	if stg.Brokers != "broker1:9092,broker2:9092" {
		t.Errorf("staging brokers = %q", stg.Brokers)
	}
	if stg.Auth != "scram" {
		t.Errorf("staging auth = %q", stg.Auth)
	}
	if stg.Username != "admin" {
		t.Errorf("staging username = %q", stg.Username)
	}

	prod := cfg.Contexts["production"]
	if prod.SSMBrokers != "/prod/brokers" {
		t.Errorf("production ssm-brokers = %q", prod.SSMBrokers)
	}
	if prod.Profile != "Production/Admin" {
		t.Errorf("production profile = %q", prod.Profile)
	}
}

func TestSaveAndLoad(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "config.yaml")

	original := &Config{
		CurrentContext: "dev",
		Contexts: map[string]Context{
			"dev": {
				Brokers:  "localhost:9092",
				Auth:     "plaintext",
				Insecure: true,
			},
		},
	}

	if err := Save(path, original); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if loaded.CurrentContext != original.CurrentContext {
		t.Errorf("CurrentContext = %q, want %q", loaded.CurrentContext, original.CurrentContext)
	}

	dev, ok := loaded.Contexts["dev"]
	if !ok {
		t.Fatal("missing dev context")
	}
	if dev.Brokers != "localhost:9092" {
		t.Errorf("brokers = %q", dev.Brokers)
	}
	if !dev.Insecure {
		t.Error("expected insecure = true")
	}
}

func TestResolve(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		CurrentContext: "staging",
		Contexts: map[string]Context{
			"staging":    {Brokers: "s:9092", Auth: "iam"},
			"production": {Brokers: "p:9092", Auth: "iam"},
		},
	}

	// Resolve by explicit name.
	ctx, err := cfg.Resolve("production")
	if err != nil {
		t.Fatalf("Resolve production: %v", err)
	}
	if ctx.Brokers != "p:9092" {
		t.Errorf("expected production brokers, got %q", ctx.Brokers)
	}

	// Resolve by empty name (uses current-context).
	ctx, err = cfg.Resolve("")
	if err != nil {
		t.Fatalf("Resolve empty: %v", err)
	}
	if ctx.Brokers != "s:9092" {
		t.Errorf("expected staging brokers, got %q", ctx.Brokers)
	}
}

func TestResolveErrors(t *testing.T) {
	t.Parallel()

	// No current-context and no name.
	cfg := &Config{}
	_, err := cfg.Resolve("")
	if err == nil {
		t.Fatal("expected error with no context")
	}

	// Name not found.
	cfg = &Config{
		Contexts: map[string]Context{
			"staging": {},
		},
	}
	_, err = cfg.Resolve("missing")
	if err == nil {
		t.Fatal("expected error for missing context")
	}
}

func TestResolveBrokersDirect(t *testing.T) {
	t.Parallel()

	ctx := &Context{Brokers: "broker:9092"}
	brokers, err := ctx.ResolveBrokers()
	if err != nil {
		t.Fatalf("ResolveBrokers: %v", err)
	}
	if brokers != "broker:9092" {
		t.Errorf("brokers = %q", brokers)
	}
}

func TestResolveBrokersEnvExpansion(t *testing.T) {
	t.Setenv("TEST_KAFKA_BROKERS", "envbroker:9092")

	ctx := &Context{Brokers: "$TEST_KAFKA_BROKERS"}
	brokers, err := ctx.ResolveBrokers()
	if err != nil {
		t.Fatalf("ResolveBrokers: %v", err)
	}
	if brokers != "envbroker:9092" {
		t.Errorf("brokers = %q, want %q", brokers, "envbroker:9092")
	}
}

func TestResolveBrokersNoBrokers(t *testing.T) {
	t.Parallel()

	ctx := &Context{}
	_, err := ctx.ResolveBrokers()
	if err == nil {
		t.Fatal("expected error with no brokers configured")
	}
}

func TestClassifySSMError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		code     string
		wantSubs []string
	}{
		{"ParameterNotFound", []string{"not found", "/some/param"}},
		{"AccessDeniedException", []string{"access denied", "ssm:GetParameter"}},
		// An expired session names the renewal command, not just "refresh".
		{"ExpiredTokenException", []string{"credentials expired", "granted sso login Staging/AdministratorAccess"}},
	}
	for _, tc := range cases {
		err := classifySSMError("/some/param", "Staging/AdministratorAccess",
			&smithy.GenericAPIError{Code: tc.code, Message: "test"})
		if err == nil {
			t.Errorf("%s: got nil err", tc.code)
			continue
		}
		msg := err.Error()
		for _, sub := range tc.wantSubs {
			if !strings.Contains(strings.ToLower(msg), strings.ToLower(sub)) {
				t.Errorf("%s: missing substring %q in %q", tc.code, sub, msg)
			}
		}
	}

	// Unrecognized API errors fall through to the generic wrapper.
	other := classifySSMError("/p", "", &smithy.GenericAPIError{Code: "WeirdNewError", Message: "x"})
	if other == nil || !strings.Contains(other.Error(), "fetching SSM parameter") {
		t.Errorf("expected generic fallback, got %q", other)
	}

	// Non-API errors also fall through.
	plain := classifySSMError("/p", "", errors.New("network unreachable"))
	if plain == nil || !strings.Contains(plain.Error(), "fetching SSM parameter") {
		t.Errorf("expected generic fallback for plain error, got %q", plain)
	}

	// A lapsed SSO session fails before the API call, so it arrives as a
	// credential error rather than an ExpiredTokenException — it must still get
	// the renewal hint instead of the bare generic wrapper.
	creds := classifySSMError("/p", "Production/ReadOnlyAccess",
		errors.New("failed to refresh cached credentials"))
	if creds == nil || !strings.Contains(creds.Error(), "granted sso login Production/ReadOnlyAccess") {
		t.Errorf("expected granted login hint for a credential failure, got %q", creds)
	}
}

func TestToAuthConfig(t *testing.T) {
	t.Parallel()

	ctx := &Context{
		Auth:     "scram",
		Username: "user",
		Password: "pass",
		CertFile: "/path/cert.pem",
		KeyFile:  "/path/key.pem",
		CAFile:   "/path/ca.pem",
		Region:   "us-east-1",
		Profile:  "MyProfile",
		Insecure: true,
	}

	auth := ctx.ToAuthConfig()

	if auth.Method != kafka.AuthSCRAM {
		t.Errorf("Method = %q, want %q", auth.Method, kafka.AuthSCRAM)
	}
	if auth.Username != "user" {
		t.Errorf("Username = %q", auth.Username)
	}
	if auth.Password != "pass" { //nolint:goconst // pragma: allowlist secret
		t.Errorf("Password = %q", auth.Password)
	}
	if auth.CertFile != "/path/cert.pem" {
		t.Errorf("CertFile = %q", auth.CertFile)
	}
	if auth.Region != "us-east-1" {
		t.Errorf("Region = %q", auth.Region)
	}
	if auth.Profile != "MyProfile" {
		t.Errorf("Profile = %q", auth.Profile)
	}
	if !auth.Insecure {
		t.Error("expected Insecure = true")
	}
}

func TestToAuthConfigEnvExpansion(t *testing.T) {
	t.Setenv("TEST_USER", "envuser")
	t.Setenv("TEST_PASS", "envpass")

	ctx := &Context{
		Auth:     "scram",
		Username: "$TEST_USER",
		Password: "$TEST_PASS",
	}

	auth := ctx.ToAuthConfig()
	if auth.Username != "envuser" {
		t.Errorf("Username = %q, want envuser", auth.Username)
	}
	if auth.Password != "envpass" { // pragma: allowlist secret
		t.Errorf("Password = %q, want envpass", auth.Password)
	}
}

// ---------------------------------------------------------------------------
// DefaultConfigDir / DefaultConfigPath
// ---------------------------------------------------------------------------

func TestDefaultConfigDir(t *testing.T) {
	t.Parallel()

	dir := DefaultConfigDir()
	if dir == "" {
		t.Fatal("DefaultConfigDir() returned empty string")
	}
	if !strings.HasSuffix(dir, ".k4a") {
		t.Errorf("DefaultConfigDir() = %q, want suffix %q", dir, ".k4a")
	}
}

func TestDefaultConfigPath(t *testing.T) {
	t.Parallel()

	path := DefaultConfigPath()
	if path == "" {
		t.Fatal("DefaultConfigPath() returned empty string")
	}
	if !strings.HasSuffix(path, "config.yaml") {
		t.Errorf("DefaultConfigPath() = %q, want suffix %q", path, "config.yaml")
	}
	if !strings.Contains(path, ".k4a") {
		t.Errorf("DefaultConfigPath() = %q, want to contain %q", path, ".k4a")
	}
}

// ---------------------------------------------------------------------------
// SaveAndReload — round-trip with multiple contexts
// ---------------------------------------------------------------------------

func TestSaveAndReload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip", "config.yaml")

	original := &Config{
		CurrentContext: "staging",
		Contexts: map[string]Context{
			"staging": {
				Brokers:  "staging-broker:9092",
				Auth:     "scram",
				Username: "admin",
				Password: "s3cret", // pragma: allowlist secret
				Insecure: true,
			},
			"production": {
				Brokers: "prod-broker:9098",
				Auth:    "iam",
				Region:  "us-west-2",
				Profile: "Production/ReadOnly",
			},
		},
	}

	if err := Save(path, original); err != nil {
		t.Fatalf("Save error: %v", err)
	}

	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if loaded.CurrentContext != "staging" {
		t.Errorf("CurrentContext = %q, want %q", loaded.CurrentContext, "staging")
	}
	if len(loaded.Contexts) != 2 {
		t.Fatalf("len(Contexts) = %d, want 2", len(loaded.Contexts))
	}

	stg, ok := loaded.Contexts["staging"]
	if !ok {
		t.Fatal("missing staging context after reload")
	}
	if stg.Brokers != "staging-broker:9092" {
		t.Errorf("staging Brokers = %q", stg.Brokers)
	}
	if stg.Auth != "scram" {
		t.Errorf("staging Auth = %q", stg.Auth)
	}
	if stg.Username != "admin" {
		t.Errorf("staging Username = %q", stg.Username)
	}
	if !stg.Insecure {
		t.Error("staging Insecure = false, want true")
	}

	prod, ok := loaded.Contexts["production"]
	if !ok {
		t.Fatal("missing production context after reload")
	}
	if prod.Auth != "iam" {
		t.Errorf("production Auth = %q", prod.Auth)
	}
	if prod.Region != "us-west-2" {
		t.Errorf("production Region = %q", prod.Region)
	}
	if prod.Profile != "Production/ReadOnly" {
		t.Errorf("production Profile = %q", prod.Profile)
	}
}

// ---------------------------------------------------------------------------
// ResolveBrokers — direct brokers string
// ---------------------------------------------------------------------------

func TestResolveBrokersDirectBrokers(t *testing.T) {
	t.Parallel()

	ctx := &Context{Brokers: "broker-a:9092,broker-b:9092"}
	brokers, err := ctx.ResolveBrokers()
	if err != nil {
		t.Fatalf("ResolveBrokers() error = %v", err)
	}
	if brokers != "broker-a:9092,broker-b:9092" {
		t.Errorf("brokers = %q, want %q", brokers, "broker-a:9092,broker-b:9092")
	}
}
