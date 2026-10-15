// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package kafka provides the Kafka client layer for k4a.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/blairham/k4a/internal/awscreds"
)

// AuthMethod defines the supported authentication methods.
type AuthMethod string

// Supported authentication methods. `scram` predates the other SCRAM variant
// and keeps meaning SCRAM-SHA-512, so existing contexts and scripts are
// unchanged; `scram-sha-512` is accepted as its explicit spelling.
const (
	AuthPlaintext   AuthMethod = "plaintext"
	AuthTLS         AuthMethod = "tls"
	AuthSCRAM       AuthMethod = "scram"
	AuthSCRAMSHA512 AuthMethod = "scram-sha-512"
	AuthSCRAMSHA256 AuthMethod = "scram-sha-256"
	AuthPlain       AuthMethod = "plain"
	AuthOAuthBearer AuthMethod = "oauthbearer"
	AuthMTLS        AuthMethod = "mtls"
	AuthIAM         AuthMethod = "iam"
)

// methodsForErrors lists the accepted --auth values for an unknown-method error.
const methodsForErrors = "plaintext, tls, scram (SHA-512), scram-sha-256, plain, oauthbearer, mtls or iam"

// clientID is stamped on every connection so brokers can attribute k4a traffic.
const clientID = "k4a"

// AuthConfig holds the authentication parameters.
type AuthConfig struct {
	Method   AuthMethod
	Username string
	Password string
	CertFile string
	KeyFile  string
	CAFile   string
	Region   string
	Profile  string
	OAuth    OAuthConfig
	Insecure bool
	// NoTLS runs a SASL method over a plaintext connection (SASL_PLAINTEXT).
	// It is an explicit opt-in, never a fallback: a SASL method without it
	// always dials TLS, and the non-SASL TLS methods reject it.
	NoTLS bool
}

// IsSASL reports whether m authenticates with a SASL mechanism that has a
// meaningful plaintext (no-TLS) form. MSK IAM is SASL too, but MSK only
// accepts it over TLS, so it is not one of them.
func (m AuthMethod) IsSASL() bool {
	switch m {
	case AuthSCRAM, AuthSCRAMSHA512, AuthSCRAMSHA256, AuthPlain, AuthOAuthBearer:
		return true
	default:
		return false
	}
}

// CleartextWarning returns the warning to show before connecting when cfg
// sends SASL credentials over a connection without TLS, or "" when it does
// not. The kafka package does not print it itself: a headless command writes
// it to stderr, the daemon logs it, and the TUI must not scribble on its own
// screen.
func CleartextWarning(cfg AuthConfig) string {
	if !cfg.NoTLS || !cfg.Method.IsSASL() {
		return ""
	}
	const tail = "; use --no-tls only against a local or otherwise trusted network"
	switch cfg.Method {
	case AuthPlain:
		return "SASL/PLAIN without TLS: the username and password are sent in cleartext and all traffic is unencrypted" + tail
	case AuthOAuthBearer:
		return "OAUTHBEARER without TLS: the bearer token is sent in cleartext, where anyone on the path can replay it, and all traffic is unencrypted" + tail
	default:
		return "SCRAM without TLS: the password itself is not sent, but the handshake can be captured and guessed offline, and all traffic is unencrypted" + tail
	}
}

// InferAuthMethod picks the method a --brokers connection without --auth means
// from what was supplied. Defaulting to scram made a bare `k4a -b
// localhost:9092` fail with "scram auth requires a username and password";
// a local broker is the common case for no credentials at all. Credentials
// still mean scram, which keeps `-b host --username u --password p` working
// as before, OAuth settings mean oauthbearer, and a client certificate means
// mtls rather than being ignored. The CLI, the TUI and k4a-index all call it,
// so a bare --brokers means the same thing on every surface.
func InferAuthMethod(auth AuthConfig) AuthMethod {
	switch {
	case auth.Username != "" || auth.Password != "":
		return AuthSCRAM
	case auth.OAuth.IsSet():
		return AuthOAuthBearer
	case auth.CertFile != "" || auth.KeyFile != "":
		return AuthMTLS
	default:
		return AuthPlaintext
	}
}

// NewClientOpts returns the franz-go client options needed to connect to the
// given broker list with the configured auth method.
func NewClientOpts(cfg AuthConfig, brokers string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(splitBrokers(brokers)...),
		kgo.ClientID(clientID),
	}

	if cfg.NoTLS && !cfg.Method.IsSASL() && cfg.Method != AuthPlaintext && cfg.Method != "" {
		return nil, fmt.Errorf(
			"no-tls applies only to the SASL methods (scram, scram-sha-256, plain, oauthbearer); %s always uses TLS",
			cfg.Method,
		)
	}

	switch cfg.Method {
	case AuthPlaintext, "":
		return opts, nil

	case AuthTLS:
		tlsCfg, err := newTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		return append(opts, kgo.DialTLSConfig(tlsCfg)), nil

	case AuthSCRAM, AuthSCRAMSHA512, AuthSCRAMSHA256, AuthPlain, AuthOAuthBearer:
		mech, err := saslMechanism(cfg)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
		if cfg.NoTLS {
			return opts, nil
		}
		tlsCfg, err := newTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		return append(opts, kgo.DialTLSConfig(tlsCfg)), nil

	case AuthMTLS:
		tlsCfg, err := newMTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		return append(opts, kgo.DialTLSConfig(tlsCfg)), nil

	case AuthIAM:
		tlsCfg, err := newTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		return append(opts, kgo.DialTLSConfig(tlsCfg), kgo.SASL(iamMechanism(cfg))), nil

	default:
		return nil, fmt.Errorf("unknown auth method %q (want %s)", cfg.Method, methodsForErrors)
	}
}

// saslMechanism builds the SASL mechanism for one of the username/password or
// token methods. The transport (TLS or not) is the caller's business.
func saslMechanism(cfg AuthConfig) (sasl.Mechanism, error) {
	switch cfg.Method {
	case AuthOAuthBearer:
		return oauthMechanism(cfg.OAuth)
	case AuthPlain:
		if cfg.Username == "" || cfg.Password == "" {
			return nil, errors.New("plain auth requires a username and password")
		}
		return plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism(), nil
	case AuthSCRAMSHA256:
		if cfg.Username == "" || cfg.Password == "" {
			return nil, errors.New("scram-sha-256 auth requires a username and password")
		}
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism(), nil
	default: // AuthSCRAM, AuthSCRAMSHA512
		if cfg.Username == "" || cfg.Password == "" {
			return nil, errors.New("scram auth requires a username and password")
		}
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism(), nil
	}
}

// errMTLSInsecure is returned for mtls with Insecure set. Presenting a client
// certificate to a broker that was never verified authenticates you to
// whoever answers, and mtls already requires the CA bundle that fixes the
// verification failure, so the flag is refused rather than silently dropped.
var errMTLSInsecure = errors.New(
	"mtls auth does not honor --insecure (insecure: true on a context): " +
		"it would present the client certificate to an unverified broker; fix the CA bundle given to --ca instead",
)

// newMTLSConfig is newTLSConfig plus the client certificate mTLS presents.
func newMTLSConfig(cfg AuthConfig) (*tls.Config, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" || cfg.CAFile == "" {
		return nil, errors.New("mtls auth requires a cert file, key file and CA file")
	}
	if cfg.Insecure {
		return nil, errMTLSInsecure
	}
	tlsCfg, err := newTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("loading client certificate: %w", err)
	}
	tlsCfg.Certificates = []tls.Certificate{cert}
	return tlsCfg, nil
}

// iamMechanism signs the MSK IAM handshake with credentials resolved from
// cfg's profile. The AWS config is loaded lazily, on the first handshake, so a
// lapsed SSO session surfaces as a connection error rather than at startup.
func iamMechanism(cfg AuthConfig) sasl.Mechanism {
	return saslaws.ManagedStreamingIAM(func(ctx context.Context) (saslaws.Auth, error) {
		awsCfg, err := awscreds.Load(ctx, cfg.Profile, cfg.Region)
		if err != nil {
			return saslaws.Auth{}, err
		}
		creds, err := awsCfg.Credentials.Retrieve(ctx)
		if err != nil {
			return saslaws.Auth{}, fmt.Errorf("retrieving AWS credentials: %w", err)
		}
		return saslaws.Auth{
			AccessKey:    creds.AccessKeyID,
			SecretKey:    creds.SecretAccessKey,
			SessionToken: creds.SessionToken,
			UserAgent:    clientID,
		}, nil
	})
}

// newTLSConfig returns a TLS config trusting the system roots, or only
// cfg.CAFile when set.
func newTLSConfig(cfg AuthConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.Insecure, //nolint:gosec // explicit opt-in via --insecure
	}
	if cfg.CAFile == "" {
		return tlsCfg, nil
	}
	pem, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file %s contains no PEM certificates", cfg.CAFile)
	}
	tlsCfg.RootCAs = pool
	return tlsCfg, nil
}

func splitBrokers(brokers string) []string {
	var out []string
	for b := range strings.SplitSeq(brokers, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
