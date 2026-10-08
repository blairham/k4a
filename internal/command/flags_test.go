// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

func TestConnectionFlagsAuthConfig(t *testing.T) {
	t.Parallel()

	flags := &ConnectionFlags{
		Auth:     "mtls",
		Username: "admin",
		Password: "secret",
		CertFile: "/path/to/cert.pem",
		KeyFile:  "/path/to/key.pem",
		CAFile:   "/path/to/ca.pem",
		Region:   "eu-west-1",
		Profile:  "Production/ReadOnly",
		Insecure: true,
	}

	cfg := flags.AuthConfig()

	tests := []struct {
		name string
		got  string
		want string
	}{
		{"Method", string(cfg.Method), "mtls"},
		{"Username", cfg.Username, "admin"},
		{"Password", cfg.Password, "secret"},
		{"CertFile", cfg.CertFile, "/path/to/cert.pem"},
		{"KeyFile", cfg.KeyFile, "/path/to/key.pem"},
		{"CAFile", cfg.CAFile, "/path/to/ca.pem"},
		{"Region", cfg.Region, "eu-west-1"},
		{"Profile", cfg.Profile, "Production/ReadOnly"},
	}

	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("AuthConfig().%s = %q, want %q", tt.name, tt.got, tt.want)
		}
	}

	if !cfg.Insecure {
		t.Error("AuthConfig().Insecure = false, want true")
	}
}

func TestConnectionFlagsResolve_DirectBrokers(t *testing.T) {
	t.Parallel()

	flags := &ConnectionFlags{
		Brokers:  "localhost:9092",
		Auth:     "scram",
		Username: "user",
		Password: "pass",
	}

	brokers, auth, err := flags.Resolve()
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	if brokers != "localhost:9092" {
		t.Errorf("brokers = %q, want %q", brokers, "localhost:9092")
	}

	if auth.Method != kafka.AuthMethod("scram") {
		t.Errorf("auth.Method = %q, want %q", auth.Method, "scram")
	}

	if auth.Username != "user" {
		t.Errorf("auth.Username = %q, want %q", auth.Username, "user")
	}

	if auth.Password != "pass" { // pragma: allowlist secret
		t.Errorf("auth.Password = %q, want %q", auth.Password, "pass") // pragma: allowlist secret
	}
}

func TestConnectionFlagsResolve_DirectBrokers_DefaultAuth(t *testing.T) {
	t.Parallel()

	flags := &ConnectionFlags{
		Brokers: "localhost:9092",
	}

	brokers, auth, err := flags.Resolve()
	if err != nil {
		t.Fatalf("Resolve() error: %v", err)
	}

	if brokers != "localhost:9092" {
		t.Errorf("brokers = %q, want %q", brokers, "localhost:9092")
	}

	if auth.Method != kafka.AuthMethod("scram") {
		t.Errorf("auth.Method = %q, want %q (default)", auth.Method, "scram")
	}
}

func TestConnectionFlagsResolve_NoBrokersNoConfig(t *testing.T) {
	t.Parallel()

	flags := &ConnectionFlags{
		ConfigFile: "/nonexistent/path/k4a-test-config.yaml",
	}

	_, _, err := flags.Resolve()
	if err == nil {
		t.Fatal("Resolve() expected error when no brokers and no config, got nil")
	}
}

func TestBuildClient_NoBrokersNoConfig(t *testing.T) {
	t.Parallel()

	flags := &ConnectionFlags{
		ConfigFile: "/nonexistent/path/k4a-test-config.yaml",
	}

	_, err := flags.BuildClient()
	if err == nil {
		t.Fatal("BuildClient() expected error when no brokers and no config, got nil")
	}
}
