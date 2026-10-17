// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/state"
)

const (
	teamATopic  = "team.a.v1"
	teamBTopic  = "team.b.v1"
	sharedTopic = "shared.ref.v1"
)

// authzPolicy grants alice search and follow on her team's topic and search
// (by glob) on the shared ones; bob may search his team's topic but not
// follow it.
const authzPolicy = `
callers:
  - name: alice
    cert: {cn: k4a test alice}
    search: ["team.a.*", "shared.*"]
    follow: ["team.a.*"]
  - name: bob
    cert: {cn: k4a test bob}
    search: ["team.b.*"]
`

// registered reports whether the manager follows topic or is registering it:
// a Search's side-effect follow is decided synchronously, so "not registered"
// right after the call means it was never started.
func registered(mgr *manager, topic string) bool {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	_, followed := mgr.set[topic]
	_, inflight := mgr.registering[topic]
	return followed || inflight
}

// serveWithPolicy runs the daemon's real gRPC server (Guard included) over
// mTLS with authzPolicy, against a fake cluster whose topics the --allow list
// admits in full, so every refusal below is the policy's.
func serveWithPolicy(t *testing.T, pki authtest.PKI) (string, *manager) {
	t.Helper()
	ctx := t.Context()
	fake := newFakeCluster(t, teamATopic, teamBTopic, sharedTopic)
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, fake.ListenAddrs()),
		mustPolicy(t, "*"), managerConfig{})
	t.Cleanup(mgr.stopAll)

	policyFile := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyFile, []byte(authzPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("K4A_INDEX_TOKEN", "")
	listener, cfg, err := listenerAuth(&flags{
		TLSCert: pki.ServerCert, TLSKey: pki.ServerKey, TLSClientCA: pki.CA, AuthzPolicy: policyFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := newGRPCServer(startupLimits{listener: listener, tls: cfg})
	is := newIndexServer(mgr)
	is.authz = endpointauth.Authorizer{Policy: listener.Policy}
	indexv1.RegisterIndexServer(srv, is)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), mgr
}

func dialAs(t *testing.T, addr string, o endpointauth.ClientOptions) indexv1.IndexClient {
	t.Helper()
	dial, err := o.GRPCDialOptions()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(addr, dial...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // test cleanup
	return indexv1.NewIndexClient(conn)
}

// searchCode runs a Search to completion and returns its status code and
// whether the leading coverage frame said the topic is followed.
func searchCode(ctx context.Context, c indexv1.IndexClient, topic string) (codes.Code, bool) {
	s, err := c.Search(ctx, &indexv1.SearchRequest{Topic: topic, Pattern: "x"})
	if err != nil {
		return status.Code(err), false
	}
	following := false
	for {
		ev, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return codes.OK, following
		}
		if err != nil {
			return status.Code(err), following
		}
		if cov := ev.GetCoverage(); cov != nil {
			following = cov.GetFollowing()
		}
	}
}

func TestPerCallerAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pki := authtest.New(t)
	addr, mgr := serveWithPolicy(t, pki)

	aliceCert, aliceKey := pki.Client(t, "alice")
	bobCert, bobKey := pki.Client(t, "bob")
	alice := dialAs(t, addr, endpointauth.ClientOptions{CAFile: pki.CA, CertFile: aliceCert, KeyFile: aliceKey})
	bob := dialAs(t, addr, endpointauth.ClientOptions{CAFile: pki.CA, CertFile: bobCert, KeyFile: bobKey})

	t.Run("allowed: search starts a follow the caller may start", func(t *testing.T) {
		if code, _ := searchCode(ctx, alice, teamATopic); code != codes.OK {
			t.Fatalf("alice Search(%s) = %v, want OK", teamATopic, code)
		}
		waitFollowed(t, mgr, teamATopic)
		if _, err := alice.Coverage(ctx, &indexv1.CoverageRequest{Topic: teamATopic}); err != nil {
			t.Errorf("alice Coverage(%s): %v", teamATopic, err)
		}
		resp, err := alice.Follow(ctx, &indexv1.FollowRequest{Topic: teamATopic})
		if err != nil || !resp.GetAccepted() {
			t.Errorf("alice Follow(%s) = %v, %v; want accepted", teamATopic, resp, err)
		}
	})

	t.Run("denied: another team's topic", func(t *testing.T) {
		if code, _ := searchCode(ctx, alice, teamBTopic); code != codes.PermissionDenied {
			t.Errorf("alice Search(%s) = %v, want PermissionDenied", teamBTopic, code)
		}
		if _, err := alice.Coverage(ctx, &indexv1.CoverageRequest{Topic: teamBTopic}); status.Code(err) !=
			codes.PermissionDenied {
			t.Errorf("alice Coverage(%s) = %v, want PermissionDenied", teamBTopic, err)
		}
		if _, err := alice.Follow(ctx, &indexv1.FollowRequest{Topic: teamBTopic}); status.Code(err) !=
			codes.PermissionDenied {
			t.Errorf("alice Follow(%s) = %v, want PermissionDenied", teamBTopic, err)
		}
		if registered(mgr, teamBTopic) {
			t.Error("a refused caller started a follow")
		}
	})

	t.Run("glob: search granted, follow not", func(t *testing.T) {
		code, following := searchCode(ctx, alice, sharedTopic)
		if code != codes.OK || following {
			t.Errorf("alice Search(%s) = %v following=%v, want OK and not followed", sharedTopic, code, following)
		}
		if registered(mgr, sharedTopic) {
			t.Error("a Search by a caller without the follow grant started a follow")
		}
		if _, err := bob.Follow(ctx, &indexv1.FollowRequest{Topic: teamBTopic}); status.Code(err) !=
			codes.PermissionDenied {
			t.Errorf("bob Follow(%s) = %v, want PermissionDenied", teamBTopic, err)
		}
		if code, _ := searchCode(ctx, bob, teamBTopic); code != codes.OK {
			t.Errorf("bob Search(%s) = %v, want OK", teamBTopic, code)
		}
		if registered(mgr, teamBTopic) {
			t.Error("bob's Search started a follow he may not start")
		}
	})

	t.Run("unauthenticated: CA-signed but not in the policy", func(t *testing.T) {
		stranger := dialAs(t, addr, endpointauth.ClientOptions{
			CAFile: pki.CA, CertFile: pki.ClientCert, KeyFile: pki.ClientKey,
		})
		if code, _ := searchCode(ctx, stranger, teamATopic); code != codes.Unauthenticated {
			t.Errorf("stranger Search = %v, want Unauthenticated", code)
		}
		if _, err := stranger.Coverage(ctx, &indexv1.CoverageRequest{Topic: teamATopic}); status.Code(err) !=
			codes.Unauthenticated {
			t.Errorf("stranger Coverage = %v, want Unauthenticated", err)
		}
	})
}

// TestAuthorizationDoesNotFailOpen serves an indexServer with a policy but no
// Guard in front of it: a request carrying no grant must be refused, not
// treated as "no policy".
func TestAuthorizationDoesNotFailOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fake := newFakeCluster(t, teamATopic)
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, fake.ListenAddrs()),
		mustPolicy(t, "*"), managerConfig{})
	defer mgr.stopAll()
	policy, err := endpointauth.ParsePolicy([]byte(authzPolicy))
	if err != nil {
		t.Fatal(err)
	}
	is := newIndexServer(mgr)
	is.authz = endpointauth.Authorizer{Policy: policy}
	c := dialIndexServer(t, is)
	if code, _ := searchCode(ctx, c, teamATopic); code != codes.PermissionDenied {
		t.Errorf("Search without a grant = %v, want PermissionDenied", code)
	}
	if registered(mgr, teamATopic) {
		t.Error("a request without a grant started a follow")
	}
}

func TestListenerAuthRefusesUnusablePolicy(t *testing.T) {
	t.Setenv("K4A_INDEX_TOKEN", "")
	pki := authtest.New(t)
	policyFile := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(policyFile, []byte(authzPolicy), 0o600); err != nil {
		t.Fatal(err)
	}
	// Certificate entries with no client CA could never match anyone.
	noCA := &flags{TLSCert: pki.ServerCert, TLSKey: pki.ServerKey, AuthzPolicy: policyFile}
	if _, _, err := listenerAuth(noCA); err == nil {
		t.Error("a certificate policy without --tls-client-ca was accepted")
	}
	if _, _, err := listenerAuth(&flags{AuthzPolicy: filepath.Join(t.TempDir(), "missing.yaml")}); err == nil {
		t.Error("a missing policy file was accepted")
	}
}
