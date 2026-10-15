// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

// TestListenerFlagsGuardTheServer runs the daemon's own flag validation and
// server construction, so a flag that parses but never reaches the listener
// fails here rather than in a deployment.
func TestListenerFlagsGuardTheServer(t *testing.T) {
	pki := authtest.New(t)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("K4A_INDEX_TOKEN", "")

	f := flags{
		Allow:     []string{"t.*"},
		MaxLimit:  10,
		TLSCert:   pki.ServerCert,
		TLSKey:    pki.ServerKey,
		TokenFile: tokenFile,
	}
	lim, ok := validateFlags(&f)
	if !ok {
		t.Fatal("validateFlags refused a complete TLS + token configuration")
	}
	srv := newGRPCServer(lim)
	indexv1.RegisterIndexServer(srv, indexv1.UnimplementedIndexServer{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)

	code := func(o endpointauth.ClientOptions) codes.Code {
		dial, err := o.GRPCDialOptions()
		if err != nil {
			t.Fatal(err)
		}
		conn, err := grpc.NewClient(lis.Addr().String(), dial...)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck // test cleanup
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = indexv1.NewIndexClient(conn).Coverage(ctx, &indexv1.CoverageRequest{Topic: "t.x"})
		return status.Code(err)
	}
	if got := code(endpointauth.ClientOptions{CAFile: pki.CA, Token: "tok"}); got != codes.Unimplemented {
		t.Errorf("with the token: %v, want Unimplemented (admitted)", got)
	}
	if got := code(endpointauth.ClientOptions{CAFile: pki.CA}); got != codes.Unauthenticated {
		t.Errorf("without the token: %v, want Unauthenticated", got)
	}

	// The health service is registered and answers without the token.
	dial, err := endpointauth.ClientOptions{CAFile: pki.CA}.GRPCDialOptions()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), dial...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck // test cleanup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("health check without the token: %v, want SERVING", err)
	}
}

func TestValidateFlagsRefusesHalfListenerConfig(t *testing.T) {
	pki := authtest.New(t)
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("K4A_INDEX_TOKEN", "")

	for name, f := range map[string]flags{
		"cert without key":      {TLSCert: pki.ServerCert},
		"client CA without TLS": {TLSClientCA: pki.CA},
		"empty token file":      {TokenFile: empty},
		"zero max-search-limit": {MaxLimit: 0},
		"huge max-search-limit": {MaxLimit: 1 << 40},
	} {
		t.Run(name, func(t *testing.T) {
			f.Allow = []string{"t.*"}
			if f.MaxLimit == 0 && name != "zero max-search-limit" {
				f.MaxLimit = 10
			}
			if _, ok := validateFlags(&f); ok {
				t.Error("accepted")
			}
		})
	}
}
