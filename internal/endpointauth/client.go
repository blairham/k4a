// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ErrTokenOverPlaintext refuses to send a bearer token where anyone on the
// network path could read it — the same rule that keeps SCRAM off plaintext.
var ErrTokenOverPlaintext = errors.New("refusing to send a bearer token over plaintext: drop the insecure/plaintext " +
	"setting and dial the endpoint over TLS")

// ClientOptions is what a client presents to a k4a endpoint.
type ClientOptions struct {
	// CAFile adds a CA to the system roots for verifying the server.
	CAFile string
	// CertFile and KeyFile are a client certificate for mTLS. Both or neither.
	CertFile string
	KeyFile  string
	// Token is the bearer token sent on every RPC.
	Token string
	// Plaintext dials without TLS (a port-forwarded or in-cluster listener
	// that serves h2c). It cannot be combined with any credential.
	Plaintext bool
}

// TLSConfig builds the client TLS config, or nil for plaintext. It refuses
// combinations that would silently drop a credential the caller set.
func (o ClientOptions) TLSConfig() (*tls.Config, error) {
	if o.Plaintext {
		switch {
		case o.Token != "":
			return nil, ErrTokenOverPlaintext
		case o.CAFile != "" || o.CertFile != "" || o.KeyFile != "":
			return nil, errors.New("TLS files were given for a plaintext connection: drop one or the other")
		}
		return nil, nil //nolint:nilnil // nil config is "dial plaintext"
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CAFile != "" {
		pool, err := loadPool(o.CAFile, true)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if o.CertFile != "" || o.KeyFile != "" {
		if o.CertFile == "" || o.KeyFile == "" {
			return nil, errors.New("a client certificate needs both the certificate and the key")
		}
		pair, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

// GRPCDialOptions returns the transport and per-RPC credentials for o.
func (o ClientOptions) GRPCDialOptions() ([]grpc.DialOption, error) {
	cfg, err := o.TLSConfig()
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if cfg != nil {
		creds = credentials.NewTLS(cfg)
	}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(creds)}
	if o.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearer(o.Token)))
	}
	return opts, nil
}

// bearer sends the token as an Authorization header. RequireTransportSecurity
// is a second guard behind TLSConfig's refusal: gRPC itself will not attach it
// to a plaintext connection.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

func (bearer) RequireTransportSecurity() bool { return true }
