// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"

	"github.com/blairham/k4a/internal/awscreds"
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
	Region        string `long:"region"         env:"AWS_REGION"         description:"AWS region (IAM auth)"                                                short:"r"`
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
		return f.Brokers, directAuthConfig(f.AuthConfig()), nil
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

	return brokers, overlayAuth(ctx.ToAuthConfig(), f.AuthConfig()), nil
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

// directAuthConfig fills the defaults for a --brokers connection, which has no
// context to inherit them from.
func directAuthConfig(auth kafka.AuthConfig) kafka.AuthConfig {
	if auth.Method == "" {
		auth.Method = inferAuthMethod(auth)
	}
	if auth.Region == "" {
		auth.Region = awscreds.DefaultRegion
	}
	return auth
}

// overlayAuth applies every connection flag the user set over a context's
// values, so "flags override config" holds for all of them rather than only
// the few that used to be copied across. An empty string means "not set":
// the region flag deliberately has no default, so an explicit
// --region us-east-1 is distinguishable from no flag at all. --insecure can
// only turn verification off; a context that skips it cannot be re-enabled
// from the command line.
func overlayAuth(base, flags kafka.AuthConfig) kafka.AuthConfig {
	for _, f := range []struct{ dst, src *string }{
		{&base.Username, &flags.Username},
		{&base.Password, &flags.Password},
		{&base.CertFile, &flags.CertFile},
		{&base.KeyFile, &flags.KeyFile},
		{&base.CAFile, &flags.CAFile},
		{&base.Region, &flags.Region},
		{&base.Profile, &flags.Profile},
	} {
		if *f.src != "" {
			*f.dst = *f.src
		}
	}
	if flags.Method != "" {
		base.Method = flags.Method
	}
	if flags.Insecure {
		base.Insecure = true
	}
	return base
}

// inferAuthMethod picks the method a --brokers connection without --auth means
// from what was supplied. Defaulting to scram made a bare `k4a -b
// localhost:9092` fail with "scram auth requires a username and password";
// a local broker is the common case for no credentials at all. Credentials
// still mean scram, which keeps `-b host --username u --password p` working
// as before, and a client certificate means mtls rather than being ignored.
func inferAuthMethod(auth kafka.AuthConfig) kafka.AuthMethod {
	switch {
	case auth.Username != "" || auth.Password != "":
		return kafka.AuthSCRAM
	case auth.CertFile != "" || auth.KeyFile != "":
		return kafka.AuthMTLS
	default:
		return kafka.AuthPlaintext
	}
}
