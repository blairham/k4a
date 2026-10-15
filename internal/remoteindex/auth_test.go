// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package remoteindex

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
)

// serveGuarded serves fake behind TLS and a bearer token, the way a daemon
// started with --tls-cert/--tls-key/--token-file does.
func serveGuarded(t *testing.T, pki authtest.PKI, token string, fake indexv1.IndexServer) string {
	t.Helper()
	o := endpointauth.ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, Token: token}
	cfg, err := endpointauth.ServerTLS(o)
	if err != nil {
		t.Fatal(err)
	}
	g := endpointauth.NewGuard(o)
	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(cfg)),
		grpc.ChainUnaryInterceptor(g.UnaryInterceptor),
		grpc.ChainStreamInterceptor(g.StreamInterceptor),
	)
	indexv1.RegisterIndexServer(srv, fake)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// With the right CA and token the search is served by the daemon; with a
// wrong token the daemon refuses and the client falls back to its own scan,
// which is the "daemon is never a hard dependency" rule holding for auth
// failures too.
func TestDialPresentsTLSAndToken(t *testing.T) {
	pki := authtest.New(t)
	addr := serveGuarded(t, pki, "tok", &fakeIndex{following: true, matches: 3})

	for _, tc := range []struct {
		name       string
		token      string
		wantRemote bool
	}{
		{"right token", "tok", true},
		{"wrong token", "bad", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Dial(addr, endpointauth.ClientOptions{CAFile: pki.CA, Token: tc.token})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() }) //nolint:errcheck // test cleanup

			var scanned atomic.Bool
			m, p, e := c.Wrap(recordingNext(&scanned))(context.Background(), "t", coveringParams())
			n, last := drain(t, m, p, e)
			if tc.wantRemote {
				if scanned.Load() || n != 3 || last.Source != kafka.SourceSharedIndex {
					t.Errorf("scanned=%v n=%d source=%v; want 3 matches from the shared index", scanned.Load(), n, last.Source)
				}
				return
			}
			if !scanned.Load() {
				t.Error("a refused daemon did not fall back to the local scan")
			}
		})
	}
}

func TestDialRefusesTokenOverPlaintext(t *testing.T) {
	if _, err := Dial("x:1", endpointauth.ClientOptions{Plaintext: true, Token: "tok"}); err == nil {
		t.Error("Dial accepted a token over plaintext")
	}
}
