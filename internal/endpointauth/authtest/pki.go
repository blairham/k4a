// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package authtest writes a throwaway PKI for tests of TLS listeners and
// clients: one CA, a server certificate for localhost / 127.0.0.1, and a
// client certificate, all as PEM files under t.TempDir().
package authtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// PKI is the set of PEM file paths a test hands to the options under test.
type PKI struct {
	ca         *x509.Certificate
	caKey      *ecdsa.PrivateKey
	dir        string
	CA         string
	ServerCert string
	ServerKey  string
	// ClientCert's subject common name is "k4a test client".
	ClientCert string
	ClientKey  string
}

// Client issues another client certificate from the same CA, with subject
// common name "k4a test <name>" and the DNS SAN "<name>.clients.test".
func (p PKI) Client(t *testing.T, name string) (certFile, keyFile string) {
	t.Helper()
	return leaf(t, p.dir, name, p.ca, p.caKey, x509.ExtKeyUsageClientAuth)
}

// New writes a fresh CA plus a server and a client certificate it signed.
func New(t *testing.T) PKI {
	t.Helper()
	dir := t.TempDir()

	caKey := newKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "k4a test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}

	p := PKI{ca: ca, caKey: caKey, dir: dir, CA: writePEM(t, dir, "ca.crt", "CERTIFICATE", caDER)}
	p.ServerCert, p.ServerKey = leaf(t, dir, "server", ca, caKey, x509.ExtKeyUsageServerAuth)
	p.ClientCert, p.ClientKey = leaf(t, dir, "client", ca, caKey, x509.ExtKeyUsageClientAuth)
	return p
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func leaf(
	t *testing.T, dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, usage x509.ExtKeyUsage,
) (certFile, keyFile string) {
	t.Helper()
	key := newKey(t)
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "k4a test " + name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     []string{"localhost", name + ".clients.test"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return writePEM(t, dir, name+".crt", "CERTIFICATE", der), writePEM(t, dir, name+".key", "EC PRIVATE KEY", keyDER)
}

func writePEM(t *testing.T, dir, name, typ string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
