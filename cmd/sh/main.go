// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command sh is the gated shell from github.com/blairham/sh/gated, installed
// at /bin/sh in the k4a-index and k4a-mcp images so that
// `kubectl exec <pod> -- /bin/sh -c '<script>'` works in a distroless image.
//
// It is a dash-dialect shell that cannot exec, signal or write anything, reads
// only /proc and /sys/fs/cgroup, and refuses /proc/*/environ. The model, the
// adoption recipe and the deployment rules are in
// https://github.com/blairham/sh/blob/main/gated/README.md.
//
// Only install it at /bin/sh in an image that has no other shell or userland.
// Beside busybox the gate protects nothing: `ls`, `wget` and the rest are
// still there to exec from anything that is not this binary.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/blairham/sh/gated"
)

// main is gated's ExampleMain, with the body in run so that the deferred
// stop runs before os.Exit.
func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return gated.Main(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv("HOSTNAME"))
}
