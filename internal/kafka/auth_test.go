// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const testBrokers = "test:9092"

func TestNewClientOpts_Plaintext(t *testing.T) {
	t.Parallel()

	opts, err := NewClientOpts(AuthConfig{Method: AuthPlaintext}, testBrokers)
	if err != nil {
		t.Fatalf("NewClientOpts plaintext: %v", err)
	}
	if len(opts) == 0 {
		t.Fatal("expected at least SeedBrokers + ClientID opts")
	}
}

func TestNewClientOpts_TLS(t *testing.T) {
	t.Parallel()

	if _, err := NewClientOpts(AuthConfig{Method: AuthTLS}, testBrokers); err != nil {
		t.Fatalf("NewClientOpts TLS: %v", err)
	}
}

func TestNewClientOpts_SCRAMMissingCredentials(t *testing.T) {
	t.Parallel()

	if _, err := NewClientOpts(AuthConfig{Method: AuthSCRAM}, testBrokers); err == nil {
		t.Fatal("expected error for SCRAM without credentials")
	}
	if _, err := NewClientOpts(AuthConfig{Method: AuthSCRAM, Username: "user"}, testBrokers); err == nil {
		t.Fatal("expected error for SCRAM without password")
	}
}

func TestNewClientOpts_SCRAMValid(t *testing.T) {
	t.Parallel()

	_, err := NewClientOpts(AuthConfig{
		Method:   AuthSCRAM,
		Username: "admin",
		Password: "secret",
	}, testBrokers)
	if err != nil {
		t.Fatalf("NewClientOpts SCRAM: %v", err)
	}
}

func TestNewClientOpts_SCRAMWithCA(t *testing.T) {
	t.Parallel()

	caFile := writeSelfSignedCA(t)

	_, err := NewClientOpts(AuthConfig{
		Method:   AuthSCRAM,
		Username: "admin",
		Password: "secret",
		CAFile:   caFile,
	}, testBrokers)
	if err != nil {
		t.Fatalf("NewClientOpts SCRAM with CA: %v", err)
	}
}

func TestNewClientOpts_SCRAMBadCA(t *testing.T) {
	t.Parallel()

	badCA := filepath.Join(t.TempDir(), "bad-ca.pem")
	if err := os.WriteFile(badCA, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := NewClientOpts(AuthConfig{
		Method:   AuthSCRAM,
		Username: "admin",
		Password: "secret",
		CAFile:   badCA,
	}, testBrokers); err == nil {
		t.Fatal("expected error for invalid CA file")
	}
}

func TestNewClientOpts_MTLSMissingFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  AuthConfig
	}{
		{"no cert", AuthConfig{Method: AuthMTLS, KeyFile: "k", CAFile: "c"}},
		{"no key", AuthConfig{Method: AuthMTLS, CertFile: "c", CAFile: "c"}},
		{"no ca", AuthConfig{Method: AuthMTLS, CertFile: "c", KeyFile: "k"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewClientOpts(tt.cfg, testBrokers); err == nil {
				t.Fatal("expected error for missing mTLS files")
			}
		})
	}
}

func TestNewClientOpts_MTLSValid(t *testing.T) {
	t.Parallel()

	certFile, keyFile, caFile := writeTestCerts(t)

	_, err := NewClientOpts(AuthConfig{
		Method:   AuthMTLS,
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   caFile,
	}, testBrokers)
	if err != nil {
		t.Fatalf("NewClientOpts mTLS: %v", err)
	}
}

func TestNewClientOpts_UnknownMethod(t *testing.T) {
	t.Parallel()

	if _, err := NewClientOpts(AuthConfig{Method: "unknown"}, testBrokers); err == nil {
		t.Fatal("expected error for unknown auth method")
	}
}

// writeSelfSignedCA generates a self-signed CA certificate and returns its file path.
func writeSelfSignedCA(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	f, err := os.Create(caFile)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatal(err)
	}

	return caFile
}

// writeTestCerts generates a self-signed CA + client cert/key pair for testing.
func writeTestCerts(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()

	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	caFile = filepath.Join(dir, "ca.pem")
	writePEM(t, caFile, "CERTIFICATE", caDER)

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Test Client"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	certFile = filepath.Join(dir, "client.pem")
	writePEM(t, certFile, "CERTIFICATE", clientDER)

	keyDER, err := x509.MarshalECPrivateKey(clientKey)
	if err != nil {
		t.Fatal(err)
	}

	keyFile = filepath.Join(dir, "client-key.pem")
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)

	return certFile, keyFile, caFile
}

func writePEM(t *testing.T, path, pemType string, data []byte) {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if err := pem.Encode(f, &pem.Block{Type: pemType, Bytes: data}); err != nil {
		t.Fatal(err)
	}
}
