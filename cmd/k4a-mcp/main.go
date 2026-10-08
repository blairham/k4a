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
// Transport is streamable-HTTP on --listen (default :9501); network-gated
// (private network or VPN, same posture as the index gRPC), no per-request auth in v1.
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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/version"
)

type flags struct {
	IndexAddr string `long:"index-addr" env:"K4A_MCP_INDEX_ADDR" default:"k4a-index:9500" description:"k4a-index gRPC endpoint (host:port)"`
	Listen    string `long:"listen"     env:"K4A_MCP_LISTEN"     default:":9501"          description:"MCP streamable-HTTP listen address"`
	Path      string `long:"path"       env:"K4A_MCP_PATH"       default:"/mcp"           description:"HTTP path the MCP endpoint is served at"`
	LogLevel  string `long:"log-level"  env:"K4A_MCP_LOG_LEVEL"  default:"info"           description:"Log level: debug, info, warn, error"`
}

func main() { os.Exit(run(os.Args[1:])) }

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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Dial k4a-index. In-cluster ClusterIP, plaintext gRPC — a TLS edge, if any,
	// is for external MCP clients hitting THIS server, not this hop. NewClient is
	// lazy (no blocking connect); RPCs reconnect as needed, so k4a-mcp starts
	// even if the index pod isn't ready yet.
	conn, err := grpc.NewClient(f.IndexAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("dial k4a-index", "addr", f.IndexAddr, "err", err)
		return 1
	}
	defer func() { _ = conn.Close() }() //nolint:errcheck // best-effort close on shutdown

	srv := newMCPServer(indexv1.NewIndexClient(conn), version.String())
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)

	mux := http.NewServeMux()
	mux.Handle(f.Path, mcpHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	httpSrv := &http.Server{
		Addr:              f.Listen,
		Handler:           logMiddleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		// Fresh timeout on purpose: the parent ctx is already canceled here.
		shutdownCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = httpSrv.Shutdown(shutdownCtx) //nolint:errcheck,contextcheck // best-effort drain on a fresh ctx
	}()

	log.Info("k4a-mcp serving", "addr", f.Listen, "path", f.Path, "index", f.IndexAddr, "version", version.String())
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}
