// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

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

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

func writeToken(t *testing.T, tok string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(tok+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// serveGuardedIndex is a k4a-index stand-in behind TLS and a token.
func serveGuardedIndex(t *testing.T, pki authtest.PKI, token string) string {
	t.Helper()
	o := endpointauth.ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, Token: token}
	cfg, err := endpointauth.ServerTLS(o)
	if err != nil {
		t.Fatal(err)
	}
	g := endpointauth.NewGuard(o)
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(cfg)),
		grpc.ChainUnaryInterceptor(g.UnaryInterceptor), grpc.ChainStreamInterceptor(g.StreamInterceptor))
	indexv1.RegisterIndexServer(srv, &fakeIndex{following: true, matches: 1})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// bearerRT adds a bearer token to every request.
type bearerRT struct {
	next  http.RoundTripper
	token string
}

func (b bearerRT) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if b.token != "" {
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return b.next.RoundTrip(r)
}

// TestAuthenticatedEndToEnd drives the real flag handling both ways: an MCP
// client with the listener's token reaches a tool, which reaches a k4a-index
// that itself demands TLS and a different token.
func TestAuthenticatedEndToEnd(t *testing.T) {
	t.Setenv("K4A_MCP_TOKEN", "")
	t.Setenv("K4A_MCP_INDEX_TOKEN", "")
	pki := authtest.New(t)
	f := flags{
		Path: "/mcp", IndexAddr: serveGuardedIndex(t, pki, "index-tok"),
		TLSCert: pki.ServerCert, TLSKey: pki.ServerKey, TokenFile: writeToken(t, "mcp-tok"),
		IndexCA: pki.CA, IndexTokenFile: writeToken(t, "index-tok"),
	}

	listener, err := f.listenerOptions()
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := endpointauth.ServerTLS(listener)
	if err != nil {
		t.Fatal(err)
	}
	indexOpts, err := f.indexOptions()
	if err != nil {
		t.Fatal(err)
	}
	dial, err := indexOpts.GRPCDialOptions()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(f.IndexAddr, dial...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // test cleanup

	srv := newMCPServer(indexv1.NewIndexClient(conn), "test", endpointauth.Authorizer{})
	ts := httptest.NewUnstartedServer(newHTTPHandler(srv, f.Path, endpointauth.NewGuard(listener)))
	ts.Listener = tls.NewListener(ts.Listener, tlsCfg)
	ts.Start()
	t.Cleanup(ts.Close)
	base := strings.Replace(ts.URL, "http://", "https://", 1)
	clientTLS, err := endpointauth.ClientOptions{CAFile: pki.CA}.TLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: clientTLS}
	t.Cleanup(transport.CloseIdleConnections)

	connect := func(token string) (*mcp.ClientSession, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx,
			&mcp.StreamableClientTransport{
				Endpoint:             base + "/mcp",
				HTTPClient:           &http.Client{Transport: bearerRT{next: transport, token: token}},
				MaxRetries:           -1,
				DisableStandaloneSSE: true,
			}, nil)
	}

	t.Run("token reaches the index", func(t *testing.T) {
		session, err := connect("mcp-tok")
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer session.Close() //nolint:errcheck // test cleanup
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "coverage", Arguments: map[string]any{"topic": "t"}})
		if err != nil || res.IsError {
			t.Fatalf("coverage through both guards: err=%v result=%+v", err, res)
		}
	})

	t.Run("no token is refused", func(t *testing.T) {
		if s, err := connect(""); err == nil {
			s.Close() //nolint:errcheck,gosec // test cleanup
			t.Fatal("an MCP session opened without the listener's token")
		}
	})

	t.Run("health needs no token", func(t *testing.T) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/health", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := (&http.Client{Transport: transport}).Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close() //nolint:errcheck,gosec // test
		if resp.StatusCode != http.StatusOK {
			t.Errorf("/health = %d, want 200", resp.StatusCode)
		}
	})
}

func TestIndexOptions(t *testing.T) {
	t.Setenv("K4A_MCP_INDEX_TOKEN", "")
	o, err := (&flags{}).indexOptions()
	if err != nil || !o.Plaintext {
		t.Errorf("default = %+v, %v; want plaintext (the in-cluster hop an upgrade keeps)", o, err)
	}
	o, err = (&flags{IndexCA: "ca.crt"}).indexOptions()
	if err != nil || o.Plaintext {
		t.Errorf("--index-ca = %+v, %v; want TLS", o, err)
	}

	t.Setenv("K4A_MCP_INDEX_TOKEN", "tok")
	o, err = (&flags{}).indexOptions()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.GRPCDialOptions(); !errors.Is(err, endpointauth.ErrTokenOverPlaintext) {
		t.Errorf("token without --index-tls: %v, want ErrTokenOverPlaintext", err)
	}
}
