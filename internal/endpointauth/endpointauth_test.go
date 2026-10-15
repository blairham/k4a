// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

const testToken = "s3cret-token"

// serveGRPC starts a guarded gRPC server whose Index service is unimplemented:
// an RPC that gets past the guard answers Unimplemented, one that does not
// answers Unauthenticated, so the code alone says which side of the guard
// it reached.
func serveGRPC(t *testing.T, o ServerOptions) string {
	t.Helper()
	cfg, err := ServerTLS(o)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGuard(o)
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(g.UnaryInterceptor),
		grpc.ChainStreamInterceptor(g.StreamInterceptor),
	}
	if cfg != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(cfg)))
	}
	srv := grpc.NewServer(opts...)
	indexv1.RegisterIndexServer(srv, indexv1.UnimplementedIndexServer{})
	healthpb.RegisterHealthServer(srv, health.NewServer())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// call makes one unary and one streaming RPC and returns their codes.
func call(t *testing.T, addr string, o ClientOptions) (unary, stream codes.Code) {
	t.Helper()
	dial, err := o.GRPCDialOptions()
	if err != nil {
		t.Fatalf("dial options: %v", err)
	}
	conn, err := grpc.NewClient(addr, dial...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := indexv1.NewIndexClient(conn)
	_, err = c.Coverage(ctx, &indexv1.CoverageRequest{Topic: "t"})
	unary = status.Code(err)
	s, err := c.Search(ctx, &indexv1.SearchRequest{Topic: "t"})
	if err == nil {
		_, err = s.Recv()
	}
	return unary, status.Code(err)
}

func healthCode(t *testing.T, addr string, o ClientOptions) codes.Code {
	t.Helper()
	dial, err := o.GRPCDialOptions()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(addr, dial...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return status.Code(err)
}

func TestGRPCTokenOverTLS(t *testing.T) {
	pki := authtest.New(t)
	addr := serveGRPC(t, ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, Token: testToken})

	cases := []struct {
		name string
		opts ClientOptions
		want codes.Code
	}{
		{"right token", ClientOptions{CAFile: pki.CA, Token: testToken}, codes.Unimplemented},
		{"no token", ClientOptions{CAFile: pki.CA}, codes.Unauthenticated},
		{"wrong token", ClientOptions{CAFile: pki.CA, Token: "nope"}, codes.Unauthenticated},
		{"token prefix", ClientOptions{CAFile: pki.CA, Token: testToken[:4]}, codes.Unauthenticated},
		// Without the CA the server cannot be verified: the client never
		// gets as far as sending the token.
		{"unverified server", ClientOptions{Token: testToken}, codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unary, stream := call(t, addr, tc.opts)
			if unary != tc.want || stream != tc.want {
				t.Errorf("unary=%v stream=%v, want %v for both", unary, stream, tc.want)
			}
		})
	}

	t.Run("health needs no token", func(t *testing.T) {
		if got := healthCode(t, addr, ClientOptions{CAFile: pki.CA}); got != codes.OK {
			t.Errorf("health check = %v, want OK", got)
		}
	})
}

// A token on a plaintext listener (TLS terminated in front of it) is still
// checked; this is the in-cluster shape behind a TLS load balancer.
func TestGRPCTokenOnPlaintextListener(t *testing.T) {
	addr := serveGRPC(t, ServerOptions{Token: testToken})
	unary, _ := call(t, addr, ClientOptions{Plaintext: true})
	if unary != codes.Unauthenticated {
		t.Errorf("no token on a token-guarded listener = %v, want Unauthenticated", unary)
	}
}

func TestGRPCMutualTLS(t *testing.T) {
	pki := authtest.New(t)
	addr := serveGRPC(t, ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, ClientCAFile: pki.CA})

	if u, s := call(
		t,
		addr,
		ClientOptions{CAFile: pki.CA, CertFile: pki.ClientCert, KeyFile: pki.ClientKey},
	); u != codes.Unimplemented ||
		s != codes.Unimplemented {
		t.Errorf("with a client certificate: unary=%v stream=%v, want Unimplemented", u, s)
	}
	if u, s := call(t, addr, ClientOptions{CAFile: pki.CA}); u != codes.Unauthenticated || s != codes.Unauthenticated {
		t.Errorf("without a client certificate: unary=%v stream=%v, want Unauthenticated", u, s)
	}
	// A certificate from another CA fails the handshake itself.
	other := authtest.New(t)
	if u, _ := call(
		t,
		addr,
		ClientOptions{CAFile: pki.CA, CertFile: other.ClientCert, KeyFile: other.ClientKey},
	); u == codes.Unimplemented {
		t.Error("a client certificate from an unknown CA got past the guard")
	}
	if got := healthCode(t, addr, ClientOptions{CAFile: pki.CA}); got != codes.OK {
		t.Errorf("health check without a client certificate = %v, want OK", got)
	}
}

func TestUnauthenticatedDefaultAdmitsEverything(t *testing.T) {
	addr := serveGRPC(t, ServerOptions{})
	if u, s := call(t, addr, ClientOptions{Plaintext: true}); u != codes.Unimplemented || s != codes.Unimplemented {
		t.Errorf("zero options: unary=%v stream=%v, want Unimplemented (admitted)", u, s)
	}
}

func TestClientRefusesTokenOverPlaintext(t *testing.T) {
	_, err := ClientOptions{Plaintext: true, Token: testToken}.GRPCDialOptions()
	if !errors.Is(err, ErrTokenOverPlaintext) {
		t.Errorf("err = %v, want ErrTokenOverPlaintext", err)
	}
	if _, err := (ClientOptions{Plaintext: true, CAFile: "ca.crt"}).GRPCDialOptions(); err == nil {
		t.Error("plaintext with a CA file was accepted")
	}
	if _, err := (ClientOptions{CertFile: "c.crt"}).GRPCDialOptions(); err == nil {
		t.Error("a client certificate without a key was accepted")
	}
}

func TestServerTLSRefusesHalfConfigs(t *testing.T) {
	pki := authtest.New(t)
	for name, o := range map[string]ServerOptions{
		"cert without key":      {CertFile: pki.ServerCert},
		"key without cert":      {KeyFile: pki.ServerKey},
		"client CA without TLS": {ClientCAFile: pki.CA},
		"missing cert file":     {CertFile: filepath.Join(t.TempDir(), "x"), KeyFile: pki.ServerKey},
	} {
		if _, err := ServerTLS(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestHTTPMiddleware(t *testing.T) {
	pki := authtest.New(t)
	o := ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, ClientCAFile: pki.CA, Token: testToken}
	cfg, err := ServerTLS(o)
	if err != nil {
		t.Fatal(err)
	}
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	ts := httptest.NewUnstartedServer(NewGuard(o).Middleware(ok, "/health"))
	// StartTLS would install httptest's own certificate ahead of ours.
	ts.Listener = tls.NewListener(ts.Listener, cfg)
	ts.Start()
	t.Cleanup(ts.Close)
	base := strings.Replace(ts.URL, "http://", "https://", 1)

	client := func(withCert bool) *http.Client {
		co := ClientOptions{CAFile: pki.CA}
		if withCert {
			co.CertFile, co.KeyFile = pki.ClientCert, pki.ClientKey
		}
		tc, err := co.TLSConfig()
		if err != nil {
			t.Fatal(err)
		}
		return &http.Client{Transport: &http.Transport{TLSClientConfig: tc}, Timeout: 5 * time.Second}
	}
	get := func(c *http.Client, path, authz string) int {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		if authz != "" {
			req.Header.Set("Authorization", authz)
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close() //nolint:errcheck,gosec // test
		return resp.StatusCode
	}

	cases := []struct {
		name  string
		path  string
		authz string
		cert  bool
		want  int
	}{
		{"cert and token", "/mcp", "Bearer " + testToken, true, http.StatusTeapot},
		{"scheme is case-insensitive", "/mcp", "bearer " + testToken, true, http.StatusTeapot},
		{"no token", "/mcp", "", true, http.StatusUnauthorized},
		{"wrong token", "/mcp", "Bearer x", true, http.StatusUnauthorized},
		{"token without scheme", "/mcp", testToken, true, http.StatusUnauthorized},
		{"no client cert", "/mcp", "Bearer " + testToken, false, http.StatusUnauthorized},
		{"health is open", "/health", "", false, http.StatusTeapot},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := get(client(tc.cert), tc.path, tc.authz); got != tc.want {
				t.Errorf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLoadToken(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("  abc\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got, err := LoadToken(file, ""); err != nil || got != "abc" {
		t.Errorf("file: %q, %v; want abc", got, err)
	}
	if got, err := LoadToken("", " lit "); err != nil || got != "lit" {
		t.Errorf("literal: %q, %v; want lit", got, err)
	}
	if got, err := LoadToken("", ""); err != nil || got != "" {
		t.Errorf("neither: %q, %v; want empty, nil", got, err)
	}
	if _, err := LoadToken(file, "lit"); err == nil {
		t.Error("both a file and a literal were accepted")
	}
	if _, err := LoadToken(empty, ""); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty file: %v, want an error", err)
	}
	if _, err := LoadToken(filepath.Join(dir, "missing"), ""); err == nil {
		t.Error("a missing file was accepted")
	}
}

// A renewed certificate is served without a restart.
func TestServerCertificateReloads(t *testing.T) {
	first := authtest.New(t)
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	copyFile(t, first.ServerCert, certFile)
	copyFile(t, first.ServerKey, keyFile)

	cfg, err := ServerTLS(ServerOptions{CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	before, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}

	second := authtest.New(t)
	copyFile(t, second.ServerCert, certFile)
	copyFile(t, second.ServerKey, keyFile)
	later := time.Now().Add(time.Minute)
	if err = os.Chtimes(certFile, later, later); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond) // past the one-second stat throttle

	after, err := cfg.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(after.Certificate[0]) == string(before.Certificate[0]) {
		t.Error("the replaced certificate was not reloaded")
	}
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from) //nolint:gosec // test path
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
