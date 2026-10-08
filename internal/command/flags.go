// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
)

// ConnectionFlags holds common connection flags shared across headless CLI commands.
// Connection can be specified via config file or direct flags; when both are present,
// flags override config values.
type ConnectionFlags struct {
	CertFile      string `long:"cert"           env:"KAFKA_TLS_CERT"     description:"Client certificate PEM file (mTLS)"`
	IndexEndpoint string `long:"index-endpoint" env:"K4A_INDEX_ENDPOINT" description:"Shared k4a-index daemon (host:port); overrides the context"`
	Brokers       string `long:"brokers"        env:"KAFKA_BROKERS"      description:"Bootstrap servers (comma-separated)"                                  short:"b"`
	Auth          string `long:"auth"           env:"KAFKA_AUTH"         description:"Auth method: plaintext, tls, scram, mtls, iam"                        short:"a"`
	Username      string `long:"username"       env:"KAFKA_USERNAME"     description:"SASL/SCRAM username"`
	Password      string `long:"password"       env:"KAFKA_PASSWORD"     description:"SASL/SCRAM password"`
	CAFile        string `long:"ca"             env:"KAFKA_TLS_CA"       description:"CA certificate PEM file"`
	Context       string `long:"context"                                 description:"Named context from ~/.k4a/config.yaml"`
	ConfigFile    string `long:"config"                                  description:"Config file path (default: ~/.k4a/config.yaml)"`
	Region        string `long:"region"         env:"AWS_REGION"         description:"AWS region (IAM auth)"                                                short:"r" default:"us-east-1"`
	Profile       string `long:"profile"        env:"AWS_PROFILE"        description:"AWS profile (IAM auth)"                                               short:"p"`
	KeyFile       string `long:"key"            env:"KAFKA_TLS_KEY"      description:"Client private key PEM file (mTLS)"`
	Insecure      bool   `long:"insecure"                                description:"Skip TLS certificate verification"                                    short:"k"`
	IndexInsecure bool   `long:"index-insecure"                          description:"Dial the shared index over plaintext (a local/port-forwarded daemon)"`
}

// ResolveIndexEndpoint returns the shared-index endpoint and whether to dial it
// plaintext. Priority: --index-endpoint flag, else the resolved context's
// index-endpoint. An empty result means local-only search. Config errors are
// swallowed (returned as "no endpoint") — a missing/broken config must never
// turn a search into a hard error over an optional accelerator.
func (f *ConnectionFlags) ResolveIndexEndpoint() (endpoint string, plaintext bool) {
	if f.IndexEndpoint != "" {
		return f.IndexEndpoint, f.IndexInsecure
	}
	if f.Brokers != "" {
		return "", false // direct connection, no context to read an endpoint from
	}
	cfgPath := f.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", false
	}
	ctx, err := cfg.Resolve(f.Context)
	if err != nil {
		return "", false
	}
	return ctx.IndexEndpoint, ctx.IndexInsecure || f.IndexInsecure
}

// BuildRemoteIndex dials the shared index if one is configured. Returns
// (nil, nil) when no endpoint is set (local-only search). Callers must Close
// the returned client when non-nil.
func (f *ConnectionFlags) BuildRemoteIndex() (*remoteindex.Client, error) {
	endpoint, plaintext := f.ResolveIndexEndpoint()
	if endpoint == "" {
		return nil, nil //nolint:nilnil // (nil client, nil error) is the "no shared index configured" signal
	}
	return remoteindex.Dial(endpoint, plaintext)
}

// AuthConfig converts connection flags to a kafka.AuthConfig.
func (f *ConnectionFlags) AuthConfig() kafka.AuthConfig {
	return kafka.AuthConfig{
		Method:   kafka.AuthMethod(f.Auth),
		Username: f.Username,
		Password: f.Password,
		CertFile: f.CertFile,
		KeyFile:  f.KeyFile,
		CAFile:   f.CAFile,
		Region:   f.Region,
		Profile:  f.Profile,
		Insecure: f.Insecure,
	}
}

// Resolve resolves connection details from either CLI flags or the config file.
// If --brokers is set, flags are used directly. Otherwise, the config file is loaded
// and the specified (or current) context is resolved.
func (f *ConnectionFlags) Resolve() (string, kafka.AuthConfig, error) {
	if f.Brokers != "" {
		auth := f.Auth
		if auth == "" {
			auth = "scram"
		}
		return f.Brokers, kafka.AuthConfig{
			Method:   kafka.AuthMethod(auth),
			Username: f.Username,
			Password: f.Password,
			CertFile: f.CertFile,
			KeyFile:  f.KeyFile,
			CAFile:   f.CAFile,
			Region:   f.Region,
			Profile:  f.Profile,
			Insecure: f.Insecure,
		}, nil
	}

	cfgPath := f.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", kafka.AuthConfig{}, err
	}

	ctx, err := cfg.Resolve(f.Context)
	if err != nil {
		return "", kafka.AuthConfig{}, fmt.Errorf(
			"%w\n\nUse --brokers to connect directly, or create a config at %s", err, cfgPath,
		)
	}

	brokers, err := ctx.ResolveBrokers()
	if err != nil {
		return "", kafka.AuthConfig{}, err
	}

	authCfg := ctx.ToAuthConfig()

	// Allow flags to override config values.
	if f.Auth != "" {
		authCfg.Method = kafka.AuthMethod(f.Auth)
	}
	if f.Profile != "" {
		authCfg.Profile = f.Profile
	}
	if f.Region != "" && f.Region != "us-east-1" {
		authCfg.Region = f.Region
	}

	return brokers, authCfg, nil
}

// BuildClient resolves connection details and builds a Kafka client.
func (f *ConnectionFlags) BuildClient() (*kafka.Client, error) {
	brokers, authCfg, err := f.Resolve()
	if err != nil {
		return nil, fmt.Errorf("connection failed: %w", err)
	}

	client, err := kafka.NewClient(authCfg, brokers)
	if err != nil {
		return nil, fmt.Errorf("client setup failed: %w", err)
	}
	return client, nil
}
