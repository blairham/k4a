// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"time"

	"github.com/charmbracelet/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// setupLogging configures the process-wide logger from a level string
// (debug|info|warn|error). Timestamps + caller are on so the CloudWatch stream
// is self-describing while we tune this. Unknown level → info.
func setupLogging(level string) {
	log.SetReportTimestamp(true)
	log.SetReportCaller(true)
	lvl, err := log.ParseLevel(level)
	if err != nil {
		lvl = log.InfoLevel
	}
	log.SetLevel(lvl)
	log.Debug("logging configured", "level", lvl.String())
}

// clientAddr pulls the caller's address off the gRPC context for log context.
func clientAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "?"
}

// unaryLogInterceptor logs every unary RPC (Coverage/Follow) with its caller,
// duration, and status — one line per call, so we can see who's hitting the
// daemon and how fast it answers.
func unaryLogInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Info(
		"grpc unary",
		"method", info.FullMethod,
		"peer", clientAddr(ctx),
		"dur", time.Since(start).Round(time.Microsecond),
		"code", status.Code(err).String(),
	)
	return resp, err
}

// streamLogInterceptor logs every streaming RPC (Search) with caller, duration,
// and status.
func streamLogInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	start := time.Now()
	err := handler(srv, ss)
	log.Info(
		"grpc stream",
		"method", info.FullMethod,
		"peer", clientAddr(ss.Context()),
		"dur", time.Since(start).Round(time.Microsecond),
		"code", status.Code(err).String(),
	)
	return err
}
