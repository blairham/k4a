// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"fmt"
	"os"

	"github.com/blairham/k4a/internal/awscreds"
	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
)

// ConnectionFlags holds common connection flags shared across headless CLI commands.
// Connection can be specified via config file or direct flags; when both are present,
// flags override config values.
type ConnectionFlags struct {
	CertFile      string `long:"cert"             env:"KAFKA_TLS_CERT"       description:"Client certificate PEM file (mTLS)"`
	IndexEndpoint string `long:"index-endpoint"   env:"K4A_INDEX_ENDPOINT"   description:"Shared k4a-index daemon (host:port); overrides the context"`
	Brokers       string `long:"brokers"          env:"KAFKA_BROKERS"        description:"Bootstrap servers (comma-separated)"                                                                     short:"b"`
	Auth          string `long:"auth"             env:"KAFKA_AUTH"           description:"Auth method: plaintext, tls, scram, scram-sha-256, plain, oauthbearer, mtls, iam"                        short:"a"`
	Username      string `long:"username"         env:"KAFKA_USERNAME"       description:"SASL username (scram, scram-sha-256, plain)"`
	Password      string `long:"password"         env:"KAFKA_PASSWORD"       description:"SASL password (scram, scram-sha-256, plain)"`
	CAFile        string `long:"ca"               env:"KAFKA_TLS_CA"         description:"CA certificate PEM file"`
	Context       string `long:"context"                                     description:"Named context from ~/.k4a/config.yaml"`
	ConfigFile    string `long:"config"                                      description:"Config file path (default: ~/.k4a/config.yaml)"`
	Region        string `long:"region"           env:"AWS_REGION"           description:"AWS region (IAM auth)"                                                                                   short:"r"`
	Profile       string `long:"profile"          env:"AWS_PROFILE"          description:"AWS profile (IAM auth)"                                                                                  short:"p"`
	KeyFile       string `long:"key"              env:"KAFKA_TLS_KEY"        description:"Client private key PEM file (mTLS)"`
	IndexCA       string `long:"index-ca"         env:"K4A_INDEX_CA"         description:"CA PEM file that verifies the shared index's TLS certificate (added to the system roots)"`
	IndexCert     string `long:"index-cert"       env:"K4A_INDEX_CERT"       description:"Client certificate PEM file for a shared index that requires mTLS"`
	IndexKey      string `long:"index-key"        env:"K4A_INDEX_KEY"        description:"Client private key PEM file for --index-cert"`
	IndexToken    string `long:"index-token-file" env:"K4A_INDEX_TOKEN_FILE" description:"File holding the shared index's bearer token (or set K4A_INDEX_TOKEN); never sent over plaintext"`
	Insecure      bool   `long:"insecure"                                    description:"Skip TLS certificate verification"                                                                       short:"k"`
	NoTLS         bool   `long:"no-tls"                                      description:"Run a SASL method without TLS (SASL_PLAINTEXT); credentials cross the network in cleartext"`
	IndexInsecure bool   `long:"index-insecure"                              description:"Dial the shared index over plaintext (a local/port-forwarded daemon); refused with a token or TLS files"`

	OAuth OAuthFlags
}

// OAuthFlags configures OAUTHBEARER: a static token, a token file, or the
// client-credentials grant. Shared by the TUI and the headless commands.
type OAuthFlags struct {
	Token        string `long:"oauth-token"         env:"KAFKA_OAUTH_TOKEN"         description:"OAUTHBEARER static bearer token"`
	TokenFile    string `long:"oauth-token-file"    env:"KAFKA_OAUTH_TOKEN_FILE"    description:"OAUTHBEARER token file, re-read on every connection"`
	TokenURL     string `long:"oauth-token-url"     env:"KAFKA_OAUTH_TOKEN_URL"     description:"OAUTHBEARER client-credentials token endpoint (https)"`
	ClientID     string `long:"oauth-client-id"     env:"KAFKA_OAUTH_CLIENT_ID"     description:"OAUTHBEARER client ID"`
	ClientSecret string `long:"oauth-client-secret" env:"KAFKA_OAUTH_CLIENT_SECRET" description:"OAUTHBEARER client secret"`
	Scopes       string `long:"oauth-scopes"        env:"KAFKA_OAUTH_SCOPES"        description:"OAUTHBEARER scopes (comma-separated)"`
}

func (o OAuthFlags) config() kafka.OAuthConfig {
	return kafka.OAuthConfig{
		Token:        o.Token,
		TokenFile:    o.TokenFile,
		TokenURL:     o.TokenURL,
		ClientID:     o.ClientID,
		ClientSecret: o.ClientSecret,
		Scopes:       o.Scopes,
	}
}

// ResolveIndex returns the shared-index endpoint and what to present to it.
// Priority: --index-endpoint, else the resolved context's index-endpoint. An
// empty endpoint means local-only search. Config errors are swallowed
// (returned as "no endpoint") — a missing/broken config must never turn a
// search into a hard error over an optional accelerator.
//
// A flag endpoint takes only the flags' credentials, never a context's: the
// context's token belongs to the context's daemon, not to whatever host was
// named on the command line, and K4A_INDEX_TOKEN is treated as the context's. A context endpoint takes the context's values
// with any flag that was set laid over them.
func (f *ConnectionFlags) ResolveIndex() (string, endpointauth.ClientOptions, error) {
	if f.IndexEndpoint != "" {
		// Not K4A_INDEX_TOKEN either: the docs suggest a context reference it
		// as index-token: $K4A_INDEX_TOKEN, which would make it the context's
		// credential sent to an ad hoc host. Only an explicit file counts here.
		token, err := endpointauth.LoadToken(f.IndexToken, "")
		if err != nil {
			return "", endpointauth.ClientOptions{}, err
		}
		return f.IndexEndpoint, endpointauth.ClientOptions{
			CAFile:    f.IndexCA,
			CertFile:  f.IndexCert,
			KeyFile:   f.IndexKey,
			Token:     token,
			Plaintext: f.IndexInsecure,
		}, nil
	}
	if f.Brokers != "" {
		return "", endpointauth.ClientOptions{}, nil // direct connection, no context to read an endpoint from
	}
	cfgPath := f.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", endpointauth.ClientOptions{}, nil //nolint:nilerr // a broken config means local-only search
	}
	ctx, err := cfg.Resolve(f.Context)
	if err != nil || ctx.IndexEndpoint == "" {
		return "", endpointauth.ClientOptions{}, nil //nolint:nilerr // as above
	}
	opts := remoteindex.ContextOptions(ctx)
	for _, o := range []struct{ dst, src *string }{
		{&opts.CAFile, &f.IndexCA},
		{&opts.CertFile, &f.IndexCert},
		{&opts.KeyFile, &f.IndexKey},
	} {
		if *o.src != "" {
			*o.dst = *o.src
		}
	}
	if f.IndexToken != "" {
		if opts.Token, err = endpointauth.LoadToken(f.IndexToken, ""); err != nil {
			return "", endpointauth.ClientOptions{}, err
		}
	}
	opts.Plaintext = opts.Plaintext || f.IndexInsecure
	return ctx.IndexEndpoint, opts, nil
}

// BuildRemoteIndex dials the shared index if one is configured. Returns
// (nil, nil) when no endpoint is set (local-only search). Callers must Close
// the returned client when non-nil.
func (f *ConnectionFlags) BuildRemoteIndex() (*remoteindex.Client, error) {
	endpoint, opts, err := f.ResolveIndex()
	if err != nil {
		return nil, err
	}
	if endpoint == "" {
		return nil, nil //nolint:nilnil // (nil client, nil error) is the "no shared index configured" signal
	}
	return remoteindex.Dial(endpoint, opts)
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
		OAuth:    f.OAuth.config(),
		Insecure: f.Insecure,
		NoTLS:    f.NoTLS,
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

	warnCleartext(authCfg)
	client, err := kafka.NewClient(authCfg, brokers)
	if err != nil {
		return nil, fmt.Errorf("client setup failed: %w", err)
	}
	return client, nil
}

// warnCleartext tells the user, on stderr so it never mixes with a command's
// output, that --no-tls is sending SASL credentials unencrypted.
func warnCleartext(auth kafka.AuthConfig) {
	if w := kafka.CleartextWarning(auth); w != "" {
		fmt.Fprintln(os.Stderr, "warning: "+w)
	}
}

// directAuthConfig fills the defaults for a --brokers connection, which has no
// context to inherit them from.
func directAuthConfig(auth kafka.AuthConfig) kafka.AuthConfig {
	if auth.Method == "" {
		auth.Method = kafka.InferAuthMethod(auth)
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
// from the command line, and --no-tls likewise only opts out of TLS.
func overlayAuth(base, flags kafka.AuthConfig) kafka.AuthConfig {
	for _, f := range []struct{ dst, src *string }{
		{&base.Username, &flags.Username},
		{&base.Password, &flags.Password},
		{&base.CertFile, &flags.CertFile},
		{&base.KeyFile, &flags.KeyFile},
		{&base.CAFile, &flags.CAFile},
		{&base.Region, &flags.Region},
		{&base.Profile, &flags.Profile},
		{&base.OAuth.Token, &flags.OAuth.Token},
		{&base.OAuth.TokenFile, &flags.OAuth.TokenFile},
		{&base.OAuth.TokenURL, &flags.OAuth.TokenURL},
		{&base.OAuth.ClientID, &flags.OAuth.ClientID},
		{&base.OAuth.ClientSecret, &flags.OAuth.ClientSecret},
		{&base.OAuth.Scopes, &flags.OAuth.Scopes},
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
	if flags.NoTLS {
		base.NoTLS = true
	}
	return base
}
