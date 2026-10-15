// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package endpointauth is the transport authentication shared by the k4a-index
// gRPC listener and the k4a-mcp HTTP listener, and by every client that dials
// them: server TLS with an optional client CA (mTLS), and an optional shared
// bearer token.
//
// It authenticates a caller as "someone holding the deployment's credential";
// it does not authorize per caller. Every authenticated caller still reads
// everything the daemon follows (SECURITY.md).
package endpointauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ServerOptions configures a listener. The zero value is plaintext with no
// token, which is what every install had before these options existed.
type ServerOptions struct {
	// CertFile and KeyFile are the server's certificate and key (PEM). Both or
	// neither.
	CertFile string
	KeyFile  string
	// ClientCAFile, when set, requires a client certificate signed by this CA
	// on every authenticated request (mTLS). Needs CertFile.
	ClientCAFile string
	// Token is the bearer token callers must present; empty disables the check.
	Token string
}

// TLSEnabled reports whether the listener serves TLS.
func (o ServerOptions) TLSEnabled() bool { return o.CertFile != "" }

// Authenticated reports whether any caller credential is required.
func (o ServerOptions) Authenticated() bool { return o.Token != "" || o.ClientCAFile != "" }

// ServerTLS builds the listener's TLS config, or nil for plaintext.
//
// A client CA sets VerifyClientCertIfGiven rather than RequireAndVerify: the
// handshake still verifies any certificate offered, and the Guard refuses an
// unauthenticated request that arrives without one. Health probes, which
// cannot present a certificate (the kubelet's never does), stay reachable.
//
// The certificate is reloaded when its file changes, so a rotated Secret
// (cert-manager renews before expiry) is picked up without a restart. A
// rotated client CA needs a restart.
func ServerTLS(o ServerOptions) (*tls.Config, error) {
	if o.CertFile == "" && o.KeyFile == "" {
		if o.ClientCAFile != "" {
			return nil, errors.New("a client CA needs a server certificate and key: mTLS runs over TLS")
		}
		return nil, nil //nolint:nilnil // nil config is "serve plaintext"
	}
	if o.CertFile == "" || o.KeyFile == "" {
		return nil, errors.New("server TLS needs both a certificate and a key")
	}
	r := &certReloader{certFile: o.CertFile, keyFile: o.KeyFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:     tls.VersionTLS12,
		GetCertificate: r.get,
		NextProtos:     []string{"h2", "http/1.1"},
	}
	if o.ClientCAFile != "" {
		pool, err := loadPool(o.ClientCAFile, false)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return cfg, nil
}

// certReloader serves the current certificate, re-reading the pair when the
// certificate file's modification time changes. Checked at most once a second
// so a handshake storm does not become a stat storm.
type certReloader struct {
	checked  time.Time
	mod      time.Time
	cert     *tls.Certificate
	certFile string
	keyFile  string
	mu       sync.Mutex
}

func (r *certReloader) load() error {
	st, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("server certificate: %w", err)
	}
	pair, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("server certificate: %w", err)
	}
	r.cert, r.mod = &pair, st.ModTime()
	return nil
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if now := time.Now(); now.Sub(r.checked) >= time.Second {
		r.checked = now
		if st, err := os.Stat(r.certFile); err == nil && !st.ModTime().Equal(r.mod) {
			// A half-written pair fails to parse; keep serving the old one and
			// try again on a later handshake.
			_ = r.load() //nolint:errcheck // the previous certificate stays in use
		}
	}
	return r.cert, nil
}

// loadPool reads PEM certificates from file, on top of the system roots when
// withSystem is set (how the Kafka client treats --ca).
func loadPool(file string, withSystem bool) (*x509.CertPool, error) {
	pem, err := os.ReadFile(file) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if withSystem {
		if sys, err := x509.SystemCertPool(); err == nil {
			pool = sys
		}
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA file %s holds no PEM certificate", file)
	}
	return pool, nil
}

// LoadToken returns the bearer token from file, else literal. A file is read
// whole and trimmed of surrounding whitespace, so a Secret written with a
// trailing newline works. Naming both, or a file that holds nothing, is an
// error: an empty token would silently turn authentication off.
func LoadToken(file, literal string) (string, error) {
	if file == "" {
		return strings.TrimSpace(literal), nil
	}
	if literal != "" {
		return "", errors.New("set the token in a file or the environment, not both")
	}
	b, err := os.ReadFile(file) //nolint:gosec // operator-supplied path
	if err != nil {
		return "", fmt.Errorf("token file: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("token file %s is empty", file)
	}
	return tok, nil
}

// tokenMatches compares a presented Authorization header with the expected
// token in constant time. Both sides are hashed first so the comparison does
// not leak the token's length either.
func tokenMatches(header, want string) bool {
	got, ok := strings.CutPrefix(header, "Bearer ")
	if !ok {
		// RFC 6750 makes the scheme case-insensitive.
		if len(header) > 7 && strings.EqualFold(header[:7], "bearer ") {
			got, ok = header[7:], true
		}
	}
	g := sha256.Sum256([]byte(strings.TrimSpace(got)))
	w := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(g[:], w[:]) == 1 && ok
}
