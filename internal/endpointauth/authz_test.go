// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/endpointauth/authtest"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

func tokenHash(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

func mustParsePolicy(t *testing.T, doc string) *Policy {
	t.Helper()
	p, err := ParsePolicy([]byte(doc))
	if err != nil {
		t.Fatalf("ParsePolicy: %v", err)
	}
	return p
}

func TestParsePolicyRefusals(t *testing.T) {
	hash := tokenHash("t")
	for name, doc := range map[string]string{
		"no callers":       "callers: []\n",
		"unknown field":    "callers:\n- name: a\n  cert: {cn: a}\n  serach: [x]\n",
		"no name":          "callers:\n- cert: {cn: a}\n",
		"duplicate name":   "callers:\n- name: a\n  cert: {cn: a}\n- name: a\n  cert: {cn: b}\n",
		"no identity":      "callers:\n- name: a\n  search: [x]\n",
		"both identities":  "callers:\n- name: a\n  cert: {cn: a}\n  tokenSHA256: " + hash + "\n",
		"two cert fields":  "callers:\n- name: a\n  cert: {cn: a, san: b}\n",
		"empty cert":       "callers:\n- name: a\n  cert: {}\n",
		"short token hash": "callers:\n- name: a\n  tokenSHA256: abcd\n",
		"raw token":        "callers:\n- name: a\n  tokenSHA256: my-secret-token\n",
		"bad glob":         "callers:\n- name: a\n  cert: {cn: a}\n  search: ['orders.[']\n",
	} {
		if _, err := ParsePolicy([]byte(doc)); err == nil {
			t.Errorf("%s: parsed, want a refusal", name)
		}
	}
}

func TestLoadPolicy(t *testing.T) {
	if p, err := LoadPolicy(""); p != nil || err != nil {
		t.Fatalf("LoadPolicy(\"\") = %v, %v; want no policy", p, err)
	}
	if _, err := LoadPolicy(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("a missing policy file loaded")
	}
	f := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(f, []byte("callers:\n- name: a\n  cert: {cn: a}\n  search: ['*']\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadPolicy(f)
	if err != nil || p == nil {
		t.Fatalf("LoadPolicy = %v, %v", p, err)
	}
}

func testLeaf() *x509.Certificate {
	u, _ := url.Parse("spiffe://example.org/ns/data/sa/oncall") //nolint:errcheck // constant
	return &x509.Certificate{
		Subject:        pkix.Name{CommonName: "alice", Organization: []string{"Acme"}},
		DNSNames:       []string{"alice.clients.test"},
		EmailAddresses: []string{"alice@example.org"},
		URIs:           []*url.URL{u},
		IPAddresses:    []net.IP{net.ParseIP("10.1.2.3")},
	}
}

func TestIdentify(t *testing.T) {
	p := mustParsePolicy(t, `
callers:
  - name: by-cn
    cert: {cn: alice}
    search: ["orders.*"]
  - name: by-subject
    cert: {subject: "CN=alice,O=Acme"}
    follow: ["orders.eu"]
  - name: by-dns
    cert: {san: alice.clients.test}
    search: ["telemetry"]
  - name: by-uri
    cert: {san: "spiffe://example.org/ns/data/sa/oncall"}
    search: ["audit.*"]
  - name: by-email
    cert: {san: alice@example.org}
  - name: by-ip
    cert: {san: 10.1.2.3}
  - name: someone-else
    cert: {cn: bob}
    search: ["*"]
  - name: ci
    tokenSHA256: `+tokenHash("ci-token")+`
    search: ["ci.*"]
`)
	g := p.Identify("", testLeaf())
	if got, want := g.Caller(), "by-cn,by-subject,by-dns,by-uri,by-email,by-ip"; got != want {
		t.Errorf("certificate matched %q, want %q (every matching entry, and not bob's)", got, want)
	}
	for _, c := range []struct {
		topic  string
		action Action
		want   bool
	}{
		{"orders.us", ActionSearch, true},  // glob
		{"orders.eu", ActionFollow, true},  // exact, from another entry
		{"orders.us", ActionFollow, false}, // search does not imply follow
		{"telemetry", ActionSearch, true},  // exact
		{"telemetryx", ActionSearch, false},
		{"audit.log", ActionSearch, true},
		{"payments", ActionSearch, false}, // granted to nobody matched
		{"ci.build", ActionSearch, false}, // the token entry did not match
	} {
		if got := g.Allows(c.action, c.topic); got != c.want {
			t.Errorf("Allows(%s, %q) = %v, want %v", c.action, c.topic, got, c.want)
		}
	}

	if g := p.Identify("Bearer ci-token", nil); g.Caller() != "ci" || !g.Allows(ActionSearch, "ci.build") {
		t.Errorf("token caller = %q", g.Caller())
	}
	if g := p.Identify("bearer ci-token", nil); g.Caller() != "ci" {
		t.Errorf("lower-case scheme: caller = %q, want ci", g.Caller())
	}
	if g := p.Identify("Bearer wrong", nil); g != nil {
		t.Errorf("wrong token identified as %q", g.Caller())
	}
	if g := p.Identify("ci-token", nil); g != nil {
		t.Errorf("token without the Bearer scheme identified as %q", g.Caller())
	}
	if g := p.Identify("", nil); g != nil {
		t.Errorf("no credential identified as %q", g.Caller())
	}
	var none *Grant
	if none.Allows(ActionSearch, "orders.us") {
		t.Error("a nil grant allowed a search")
	}
}

func TestAuthorizer(t *testing.T) {
	ctx := context.Background()
	if err := (Authorizer{}).Check(ctx, ActionFollow, "anything"); err != nil {
		t.Errorf("no policy: %v, want everything allowed (the pre-Phase-5 behavior)", err)
	}
	p := mustParsePolicy(t, "callers:\n- name: a\n  cert: {cn: alice}\n  search: ['orders.*']\n")
	a := Authorizer{Policy: p}
	if err := a.Check(ctx, ActionSearch, "orders.us"); err == nil {
		t.Error("policy set, no grant on the context: allowed, want refused (no fail-open)")
	}
	ctx = WithGrant(ctx, p.Identify("", testLeaf()))
	if err := a.Check(ctx, ActionSearch, "orders.us"); err != nil {
		t.Errorf("granted search refused: %v", err)
	}
	err := a.Check(ctx, ActionSearch, "payments")
	var nae *NotAuthorizedError
	if !errors.As(err, &nae) || !strings.Contains(err.Error(), `caller a may not search topic "payments"`) {
		t.Errorf("ungranted search: %v", err)
	}
}

func TestCheckPolicy(t *testing.T) {
	certs := mustParsePolicy(t, "callers:\n- name: a\n  cert: {cn: alice}\n")
	tokens := mustParsePolicy(t, "callers:\n- name: a\n  tokenSHA256: "+tokenHash("x")+"\n")
	for _, c := range []struct {
		o    ServerOptions
		name string
		ok   bool
	}{
		{name: "no policy", o: ServerOptions{}, ok: true},
		{name: "cert entries without a client CA", o: ServerOptions{Policy: certs}, ok: false},
		{name: "cert entries with a client CA", o: ServerOptions{Policy: certs, ClientCAFile: "ca"}, ok: true},
		{name: "token entries", o: ServerOptions{Policy: tokens}, ok: true},
		{name: "token entries beside the shared token", o: ServerOptions{Policy: tokens, Token: "t"}, ok: false},
		{name: "cert entries beside the shared token", o: ServerOptions{Policy: certs, ClientCAFile: "ca", Token: "t"}, ok: true},
	} {
		if err := c.o.CheckPolicy(); (err == nil) != c.ok {
			t.Errorf("%s: %v, want ok=%v", c.name, err, c.ok)
		}
	}
	if !(ServerOptions{Policy: tokens}).Authenticated() {
		t.Error("a listener with a policy reports itself unauthenticated")
	}
}

// grantEcho answers Coverage with following=true only when the caller's grant
// allows searching the topic, so the code and the flag together say whether
// the Guard admitted the caller and what grant reached the handler.
type grantEcho struct {
	indexv1.UnimplementedIndexServer
}

func (grantEcho) Coverage(ctx context.Context, req *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	ok := GrantFromContext(ctx).Allows(ActionSearch, req.GetTopic())
	return &indexv1.CoverageResponse{Coverage: &indexv1.Coverage{Following: ok}}, nil
}

func (grantEcho) Search(req *indexv1.SearchRequest, stream indexv1.Index_SearchServer) error {
	ok := GrantFromContext(stream.Context()).Allows(ActionSearch, req.GetTopic())
	return stream.Send(&indexv1.SearchEvent{
		Event: &indexv1.SearchEvent_Coverage{Coverage: &indexv1.Coverage{Following: ok}},
	})
}

func serveGrantEcho(t *testing.T, o ServerOptions) string {
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
	indexv1.RegisterIndexServer(srv, grantEcho{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// coverageAndSearch reports, for one unary and one streaming RPC, the code and
// whether the handler saw a grant allowing topic.
func coverageAndSearch(t *testing.T, addr string, o ClientOptions, topic string) (codes.Code, bool, codes.Code, bool) {
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
	c := indexv1.NewIndexClient(conn)
	resp, err := c.Coverage(ctx, &indexv1.CoverageRequest{Topic: topic})
	uc, uok := status.Code(err), resp.GetCoverage().GetFollowing()
	s, err := c.Search(ctx, &indexv1.SearchRequest{Topic: topic})
	if err != nil {
		return uc, uok, status.Code(err), false
	}
	ev, err := s.Recv()
	return uc, uok, status.Code(err), ev.GetCoverage().GetFollowing()
}

func TestGuardPolicyOverGRPC(t *testing.T) {
	pki := authtest.New(t)
	aliceCert, aliceKey := pki.Client(t, "alice")
	policy := mustParsePolicy(t, `
callers:
  - name: alice
    cert: {cn: k4a test alice}
    search: ["orders.*"]
  - name: by-san
    cert: {san: alice.clients.test}
    search: ["telemetry"]
`)
	addr := serveGrantEcho(t, ServerOptions{
		CertFile: pki.ServerCert, KeyFile: pki.ServerKey, ClientCAFile: pki.CA, Policy: policy,
	})
	alice := ClientOptions{CAFile: pki.CA, CertFile: aliceCert, KeyFile: aliceKey}

	for _, c := range []struct {
		name   string
		topic  string
		client ClientOptions
		code   codes.Code
		grant  bool
	}{
		{name: "named caller, granted glob", client: alice, topic: "orders.us", code: codes.OK, grant: true},
		{name: "named caller, SAN entry", client: alice, topic: "telemetry", code: codes.OK, grant: true},
		{name: "named caller, ungranted topic", client: alice, topic: "payments", code: codes.OK, grant: false},
		{
			name:   "CA-signed but unnamed caller",
			client: ClientOptions{CAFile: pki.CA, CertFile: pki.ClientCert, KeyFile: pki.ClientKey},
			topic:  "orders.us", code: codes.Unauthenticated,
		},
		{name: "no certificate", client: ClientOptions{CAFile: pki.CA}, topic: "orders.us", code: codes.Unauthenticated},
	} {
		uc, uok, sc, sok := coverageAndSearch(t, addr, c.client, c.topic)
		if uc != c.code || sc != c.code || uok != c.grant || sok != c.grant {
			t.Errorf("%s: unary=%v/%v stream=%v/%v, want %v/%v", c.name, uc, uok, sc, sok, c.code, c.grant)
		}
	}
}

func TestGuardTokenPolicyOverGRPC(t *testing.T) {
	pki := authtest.New(t)
	policy := mustParsePolicy(t, "callers:\n- name: ci\n  tokenSHA256: "+tokenHash("ci-token")+
		"\n  search: ['ci.*']\n")
	addr := serveGrantEcho(t, ServerOptions{CertFile: pki.ServerCert, KeyFile: pki.ServerKey, Policy: policy})

	uc, uok, sc, sok := coverageAndSearch(t, addr, ClientOptions{CAFile: pki.CA, Token: "ci-token"}, "ci.build")
	if uc != codes.OK || sc != codes.OK || !uok || !sok {
		t.Errorf("token caller: %v/%v %v/%v, want OK with the grant", uc, uok, sc, sok)
	}
	for name, tok := range map[string]string{"wrong token": "other", "no token": ""} {
		uc, _, sc, _ := coverageAndSearch(t, addr, ClientOptions{CAFile: pki.CA, Token: tok}, "ci.build")
		if uc != codes.Unauthenticated || sc != codes.Unauthenticated {
			t.Errorf("%s: %v/%v, want Unauthenticated", name, uc, sc)
		}
	}
}

func TestGuardPolicyOverHTTP(t *testing.T) {
	policy := mustParsePolicy(t, "callers:\n- name: ci\n  tokenSHA256: "+tokenHash("ci-token")+
		"\n  search: ['ci.*']\n")
	g := NewGuard(ServerOptions{Policy: policy})
	h := g.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if GrantFromContext(r.Context()).Allows(ActionSearch, "ci.build") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusForbidden)
	}), "/health")
	for _, c := range []struct {
		path, authz string
		want        int
	}{
		{"/mcp", "Bearer ci-token", http.StatusOK},
		{"/mcp", "Bearer other", http.StatusUnauthorized},
		{"/mcp", "", http.StatusUnauthorized},
		{"/health", "", http.StatusForbidden}, // open path: no grant, but not refused
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, c.path, http.NoBody)
		if c.authz != "" {
			req.Header.Set("Authorization", c.authz)
		}
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s %q = %d, want %d", c.path, c.authz, rec.Code, c.want)
		}
		if rec.Code == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s %q: 401 without WWW-Authenticate", c.path, c.authz)
		}
	}
}
