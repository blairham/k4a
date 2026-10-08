// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net/http"
	"time"

	"github.com/charmbracelet/log"
)

// setupLogging configures the process-wide logger from a level string.
// Timestamps + caller on so the CloudWatch stream is self-describing.
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

// statusWriter captures the response status while passing Flush through so the
// MCP streamable-HTTP handler's SSE keeps flushing.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// logMiddleware logs one line per HTTP request (method, path, status, duration,
// remote). /health probe hits log at debug so readiness checks don't flood.
func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)

		fields := []any{
			"method", r.Method,
			"path", r.URL.Path,
			"status", sw.status,
			"dur", time.Since(start).Round(time.Millisecond),
			"remote", r.RemoteAddr,
		}
		if r.URL.Path == "/health" {
			log.Debug("http", fields...)
		} else {
			log.Info("http", fields...)
		}
	})
}
