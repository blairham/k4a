// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command k4a-mcp is a networked Model Context Protocol server fronting the
// shared k4a-index daemon. It is a thin gRPC CLIENT of k4a-index: it holds no
// Kafka client and no local index, so every tool call is answered by the one
// warm index. Users point Claude Code at this over streamable-HTTP instead
// of each running a local stdio scan (docs/design/shared-index-service.md).
//
// Register it with:  claude mcp add --transport http k4a-index http://<host>:9501/mcp
//
// Transport is streamable-HTTP on --listen (default :9501). By default it is
// plaintext with no caller credential (network-gated, same posture as the index
// gRPC); --tls-cert/--tls-key, --tls-client-ca and --token-file add TLS, mTLS
// and a bearer token, and the --index-* flags give it the credentials to present
// to a k4a-index that asks for them.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/version"
)

type flags struct {
	IndexAddr string `long:"index-addr" env:"K4A_MCP_INDEX_ADDR" default:"k4a-index:9500" description:"k4a-index gRPC endpoint (host:port)"`
	Listen    string `long:"listen"     env:"K4A_MCP_LISTEN"     default:":9501"          description:"MCP streamable-HTTP listen address"`
	Path      string `long:"path"       env:"K4A_MCP_PATH"       default:"/mcp"           description:"HTTP path the MCP endpoint is served at"`
	LogLevel  string `long:"log-level"  env:"K4A_MCP_LOG_LEVEL"  default:"info"           description:"Log level: debug, info, warn, error"`

	TLSCert     string `long:"tls-cert"      env:"K4A_MCP_TLS_CERT"      description:"Server certificate PEM file: serve HTTPS (needs --tls-key). Re-read when the file changes."`
	TLSKey      string `long:"tls-key"       env:"K4A_MCP_TLS_KEY"       description:"Server private key PEM file (with --tls-cert)"`
	TLSClientCA string `long:"tls-client-ca" env:"K4A_MCP_TLS_CLIENT_CA" description:"Require a client certificate signed by this CA on every request except /health (mTLS; needs --tls-cert)"`
	TokenFile   string `long:"token-file"    env:"K4A_MCP_TOKEN_FILE"    description:"File holding the bearer token every request except /health must present. K4A_MCP_TOKEN sets it directly."`
	AuthzPolicy string `long:"authz-policy"  env:"K4A_MCP_AUTHZ_POLICY"  description:"Per-caller authorization policy (YAML): maps client certificates and bearer tokens to the topics each caller may search and follow. Unset, every authenticated caller reaches every topic k4a-index answers for (SECURITY.md)."`

	IndexCA        string `long:"index-ca"         env:"K4A_MCP_INDEX_CA"         description:"CA PEM file that verifies k4a-index's TLS certificate (implies --index-tls)"`
	IndexCert      string `long:"index-cert"       env:"K4A_MCP_INDEX_CERT"       description:"Client certificate PEM file for a k4a-index that requires mTLS (implies --index-tls)"`
	IndexKey       string `long:"index-key"        env:"K4A_MCP_INDEX_KEY"        description:"Client private key PEM file for --index-cert"`
	IndexTokenFile string `long:"index-token-file" env:"K4A_MCP_INDEX_TOKEN_FILE" description:"File holding k4a-index's bearer token (K4A_MCP_INDEX_TOKEN sets it directly). Needs --index-tls."`
	IndexTLS       bool   `long:"index-tls"        env:"K4A_MCP_INDEX_TLS"        description:"Dial k4a-index over TLS (default: plaintext, the in-cluster hop)"`
}

// listenerOptions loads the HTTP listener's TLS material and bearer token.
func (f *flags) listenerOptions() (endpointauth.ServerOptions, error) {
	token, err := endpointauth.LoadToken(f.TokenFile, os.Getenv("K4A_MCP_TOKEN"))
	if err != nil {
		return endpointauth.ServerOptions{}, err
	}
	policy, err := endpointauth.LoadPolicy(f.AuthzPolicy)
	if err != nil {
		return endpointauth.ServerOptions{}, err
	}
	o := endpointauth.ServerOptions{
		CertFile: f.TLSCert, KeyFile: f.TLSKey, ClientCAFile: f.TLSClientCA, Token: token, Policy: policy,
	}
	if perr := o.CheckPolicy(); perr != nil {
		return endpointauth.ServerOptions{}, perr
	}
	return o, nil
}

// indexOptions is what k4a-mcp presents to k4a-index. The hop stays plaintext
// unless asked otherwise, so an upgraded install keeps dialing as before; a
// token without TLS is refused rather than sent in the clear.
func (f *flags) indexOptions() (endpointauth.ClientOptions, error) {
	token, err := endpointauth.LoadToken(f.IndexTokenFile, os.Getenv("K4A_MCP_INDEX_TOKEN"))
	if err != nil {
		return endpointauth.ClientOptions{}, err
	}
	useTLS := f.IndexTLS || f.IndexCA != "" || f.IndexCert != "" || f.IndexKey != ""
	return endpointauth.ClientOptions{
		CAFile: f.IndexCA, CertFile: f.IndexCert, KeyFile: f.IndexKey, Token: token, Plaintext: !useTLS,
	}, nil
}

// newHTTPHandler is the whole HTTP surface: the MCP endpoint behind the guard,
// and /health outside it so the kubelet's probes need no credential.
func newHTTPHandler(srv *mcp.Server, path string, guard *endpointauth.Guard) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(path, newMCPHandler(srv))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return logMiddleware(guard.Middleware(mux, "/health"))
}

// warnListenerExposure says, once at startup, what reaching the port grants.
func warnListenerExposure(o endpointauth.ServerOptions) {
	switch {
	case !o.TLSEnabled() && !o.Authenticated():
		log.Warn("HTTP listener is plaintext and unauthenticated: anyone who can reach it can search every " +
			"topic k4a-index follows; set --token-file or --tls-client-ca (SECURITY.md)")
	case !o.TLSEnabled():
		log.Warn("HTTP listener is plaintext: the bearer token crosses the network in the clear unless TLS " +
			"is terminated in front of it; set --tls-cert/--tls-key to serve HTTPS here")
	case !o.Authenticated():
		log.Warn("HTTP listener serves TLS but asks no caller credential: set --token-file or --tls-client-ca")
	}
	if o.Policy == nil && o.Authenticated() {
		log.Info("no --authz-policy: every authenticated caller may search every topic k4a-index answers for")
	}
}

func main() { os.Exit(run(os.Args[1:])) }

// newMCPHandler serves srv over streamable HTTP in Stateless mode.
//
// The go-sdk default is stateful: every initialize creates a session held in
// this process's memory, keyed by Mcp-Session-Id, with no idle timeout. That
// grows without bound, and behind a Service with replicaCount > 1 a follow-up
// request routed to another pod is answered "session not found". None of the
// tools needs a session — search and coverage are pure request/response
// proxies to k4a-index that never send server->client requests, progress
// notifications or log messages — so each POST gets a throwaway session
// instead, and any replica can answer any request.
func newMCPHandler(srv *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
}

func run(args []string) int {
	var f flags
	parser := goflags.NewParser(&f, goflags.Default)
	if _, err := parser.ParseArgs(args); err != nil {
		if goflags.WroteHelp(err) {
			return 0
		}
		log.Error("invalid flags", "err", err)
		return 1
	}

	setupLogging(f.LogLevel)
	log.Info(
		"k4a-mcp starting",
		"version",
		version.String(),
		"listen",
		f.Listen,
		"path",
		f.Path,
		"index",
		f.IndexAddr,
		"log_level",
		f.LogLevel,
	)

	listener, err := f.listenerOptions()
	if err != nil {
		log.Error("listener auth", "err", err)
		return 1
	}
	tlsCfg, err := endpointauth.ServerTLS(listener)
	if err != nil {
		log.Error("listener auth", "err", err)
		return 1
	}
	indexOpts, err := f.indexOptions()
	if err != nil {
		log.Error("k4a-index credentials", "err", err)
		return 1
	}
	dial, err := indexOpts.GRPCDialOptions()
	if err != nil {
		log.Error("k4a-index credentials", "err", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Dial k4a-index. NewClient is lazy (no blocking connect); RPCs reconnect
	// as needed, so k4a-mcp starts even if the index pod isn't ready yet.
	conn, err := grpc.NewClient(f.IndexAddr, dial...)
	if err != nil {
		log.Error("dial k4a-index", "addr", f.IndexAddr, "err", err)
		return 1
	}
	defer func() { _ = conn.Close() }() //nolint:errcheck // best-effort close on shutdown

	srv := newMCPServer(indexv1.NewIndexClient(conn), version.String(), endpointauth.Authorizer{Policy: listener.Policy})
	httpSrv := &http.Server{
		Addr:              f.Listen,
		Handler:           newHTTPHandler(srv, f.Path, endpointauth.NewGuard(listener)),
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig:         tlsCfg,
	}
	warnListenerExposure(listener)

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		// Fresh timeout on purpose: the parent ctx is already canceled here.
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = httpSrv.Shutdown(shutdownCtx) //nolint:errcheck,contextcheck // best-effort drain on a fresh ctx
	}()

	logServing(&f, listener, indexOpts)
	if tlsCfg != nil {
		// The certificate comes from TLSConfig.GetCertificate, so no file names.
		err = httpSrv.ListenAndServeTLS("", "")
	} else {
		err = httpSrv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}

// logServing records what the listener and the index hop were set up with,
// never the credentials themselves.
func logServing(f *flags, listener endpointauth.ServerOptions, indexOpts endpointauth.ClientOptions) {
	log.Info("k4a-mcp serving", "addr", f.Listen, "path", f.Path, "index", f.IndexAddr,
		"tls", listener.TLSEnabled(), "client_ca", listener.ClientCAFile != "", "token", listener.Token != "",
		"authz_policy", listener.Policy != nil, "index_tls", !indexOpts.Plaintext,
		"index_token", indexOpts.Token != "", "version", version.String())
}
