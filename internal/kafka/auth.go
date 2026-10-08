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
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/blairham/k4a/internal/awscreds"
)

// AuthMethod defines the supported authentication methods.
type AuthMethod string

// Supported authentication methods.
const (
	AuthPlaintext AuthMethod = "plaintext"
	AuthTLS       AuthMethod = "tls"
	AuthSCRAM     AuthMethod = "scram"
	AuthMTLS      AuthMethod = "mtls"
	AuthIAM       AuthMethod = "iam"
)

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
	Insecure bool
}

// NewClientOpts returns the franz-go client options needed to connect to the
// given broker list with the configured auth method.
func NewClientOpts(cfg AuthConfig, brokers string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(splitBrokers(brokers)...),
		kgo.ClientID(clientID),
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

	case AuthSCRAM:
		if cfg.Username == "" || cfg.Password == "" {
			return nil, errors.New("scram auth requires a username and password")
		}
		tlsCfg, err := newTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		mech := scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism()
		return append(opts, kgo.DialTLSConfig(tlsCfg), kgo.SASL(mech)), nil

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
		return nil, fmt.Errorf("unknown auth method %q (want plaintext, tls, scram, mtls or iam)", cfg.Method)
	}
}

// newMTLSConfig is newTLSConfig plus the client certificate mTLS presents.
func newMTLSConfig(cfg AuthConfig) (*tls.Config, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" || cfg.CAFile == "" {
		return nil, errors.New("mtls auth requires a cert file, key file and CA file")
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

// newTLSConfig returns a TLS config trusting the system roots, plus cfg.CAFile
// when set.
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
