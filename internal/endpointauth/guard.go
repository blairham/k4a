// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// healthMethodPrefix is the gRPC health service. It answers only SERVING or
// not, and the kubelet's gRPC probe can present neither a token nor a client
// certificate, so it stays open.
const healthMethodPrefix = "/grpc.health.v1.Health/"

// Guard enforces a listener's caller credentials: the bearer token when one is
// configured, a verified client certificate when a client CA is, and, under an
// authorization policy, that the caller matches one of its entries. An admitted
// caller's grant rides on the request context for the handler's per-topic
// check (Authorizer).
type Guard struct {
	policy     *Policy
	token      string
	clientCert bool
}

// NewGuard returns the guard for o. A guard over the zero options admits
// everything, which is the unauthenticated default.
func NewGuard(o ServerOptions) *Guard {
	return &Guard{token: o.Token, clientCert: o.ClientCAFile != "", policy: o.Policy}
}

// noPolicyMatch is the refusal for a caller the policy does not name. It says
// nothing about which identities the policy does name.
const noPolicyMatch = "the caller matches no entry in the authorization policy"

// check reports why a caller is refused, or "" to admit it, with the caller's
// grant when a policy is configured. authz is the Authorization value
// presented; leaf is the client certificate the handshake verified against the
// client CA, or nil.
func (g *Guard) check(authz string, leaf *x509.Certificate) (string, *Grant) {
	if g.clientCert && leaf == nil {
		return "a client certificate signed by the configured client CA is required", nil
	}
	if g.token != "" && !tokenMatches(authz, g.token) {
		return "a valid bearer token is required", nil
	}
	if g.policy == nil {
		return "", nil
	}
	grant := g.policy.Identify(authz, leaf)
	if grant == nil {
		return noPolicyMatch, nil
	}
	return "", grant
}

// UnaryInterceptor refuses an unauthenticated unary RPC with Unauthenticated.
func (g *Guard) UnaryInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	ctx, err := g.checkGRPC(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor refuses an unauthenticated streaming RPC.
func (g *Guard) StreamInterceptor(
	srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx, err := g.checkGRPC(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}
	if ctx != ss.Context() {
		ss = &grantStream{ServerStream: ss, ctx: ctx}
	}
	return handler(srv, ss)
}

// grantStream carries the caller's grant into a streaming handler.
type grantStream struct {
	grpc.ServerStream
	ctx context.Context //nolint:containedctx // the stream's own context, with the grant added
}

func (s *grantStream) Context() context.Context { return s.ctx }

// checkGRPC admits or refuses one RPC, returning ctx with the caller's grant
// attached when a policy is configured.
func (g *Guard) checkGRPC(ctx context.Context, method string) (context.Context, error) {
	if strings.HasPrefix(method, healthMethodPrefix) {
		return ctx, nil
	}
	var authz string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			authz = v[0]
		}
	}
	var leaf *x509.Certificate
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			leaf = verifiedLeaf(&ti.State)
		}
	}
	why, grant := g.check(authz, leaf)
	if why != "" {
		return ctx, status.Error(codes.Unauthenticated, why)
	}
	if grant != nil {
		ctx = WithGrant(ctx, grant)
	}
	return ctx, nil
}

// Middleware refuses an unauthenticated HTTP request with 401, except on the
// open paths (health checks). The response names the scheme and nothing about
// the expected credential.
func (g *Guard) Middleware(next http.Handler, open ...string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, p := range open {
			if r.URL.Path == p {
				next.ServeHTTP(w, r)
				return
			}
		}
		why, grant := g.check(r.Header.Get("Authorization"), verifiedLeaf(r.TLS))
		if why != "" {
			if g.token != "" || (g.policy != nil && g.policy.HasTokenCallers()) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="k4a"`)
			}
			http.Error(w, why, http.StatusUnauthorized)
			return
		}
		if grant != nil {
			r = r.WithContext(WithGrant(r.Context(), grant))
		}
		next.ServeHTTP(w, r)
	})
}

// verifiedLeaf is the client certificate the handshake verified against the
// client CA, or nil. An offered but unverified certificate never counts.
func verifiedLeaf(cs *tls.ConnectionState) *x509.Certificate {
	if cs == nil || len(cs.VerifiedChains) == 0 || len(cs.VerifiedChains[0]) == 0 {
		return nil
	}
	return cs.VerifiedChains[0][0]
}
