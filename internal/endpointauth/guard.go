// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"context"
	"crypto/tls"
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
// configured, and a verified client certificate when a client CA is.
type Guard struct {
	token      string
	clientCert bool
}

// NewGuard returns the guard for o. A guard over the zero options admits
// everything, which is the unauthenticated default.
func NewGuard(o ServerOptions) *Guard {
	return &Guard{token: o.Token, clientCert: o.ClientCAFile != ""}
}

// check reports why a caller is refused, or "" to admit it. authz is the
// Authorization value presented; verified reports a client certificate that
// the handshake verified against the client CA.
func (g *Guard) check(authz string, verified bool) string {
	if g.clientCert && !verified {
		return "a client certificate signed by the configured client CA is required"
	}
	if g.token != "" && !tokenMatches(authz, g.token) {
		return "a valid bearer token is required"
	}
	return ""
}

// UnaryInterceptor refuses an unauthenticated unary RPC with Unauthenticated.
func (g *Guard) UnaryInterceptor(
	ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (any, error) {
	if err := g.checkGRPC(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// StreamInterceptor refuses an unauthenticated streaming RPC.
func (g *Guard) StreamInterceptor(
	srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if err := g.checkGRPC(ss.Context(), info.FullMethod); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (g *Guard) checkGRPC(ctx context.Context, method string) error {
	if strings.HasPrefix(method, healthMethodPrefix) {
		return nil
	}
	var authz string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			authz = v[0]
		}
	}
	verified := false
	if p, ok := peer.FromContext(ctx); ok {
		if ti, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			verified = len(ti.State.VerifiedChains) > 0
		}
	}
	if why := g.check(authz, verified); why != "" {
		return status.Error(codes.Unauthenticated, why)
	}
	return nil
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
		if why := g.check(r.Header.Get("Authorization"), verifiedHTTP(r.TLS)); why != "" {
			if g.token != "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="k4a"`)
			}
			http.Error(w, why, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func verifiedHTTP(cs *tls.ConnectionState) bool {
	return cs != nil && len(cs.VerifiedChains) > 0
}
