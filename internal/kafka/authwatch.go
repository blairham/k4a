// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// authFailureLimit is how many connections in a row may fail to authenticate
// before a consumer gives up. One stray refusal should not end a long tail
// (franz-go retries a refused re-authentication once for MSK IAM's), but
// several with none succeeding in between is the broker's answer. franz-go
// waits out its metadata min age (5s) after each refusal, so a refused
// consumer ends in about ten seconds.
const authFailureLimit = 3

// errAuthGaveUp marks the cause a watched context ends with, so authFailure
// can tell the watch's cancellation from the caller's.
var errAuthGaveUp = errors.New("giving up")

// authWatch ends a consumer's polls once its connections keep failing to
// authenticate. franz-go retries a refused handshake from inside the client,
// with backoff and without end, and never reports it to PollFetches, which
// goes on waiting; only the connect hook sees it. Without this, a consumer
// whose credentials had expired retried the handshake against every broker
// for as long as it ran (#153).
type authWatch struct {
	cancel   context.CancelCauseFunc
	mu       sync.Mutex
	failures int
}

// OnBrokerConnect counts failed authentications. A connection that
// authenticates resets the count; one that fails for any other reason (a
// dial error, a dropped connection) says nothing about the credentials.
func (w *authWatch) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case err == nil:
		w.failures = 0
	case IsAuthError(err):
		w.failures++
		if w.failures >= authFailureLimit {
			w.cancel(fmt.Errorf("%w after %d failed authentications in a row: %w", errAuthGaveUp, w.failures, err))
		}
	}
}

// OnClientClosed releases the watched context.
func (w *authWatch) OnClientClosed(*kgo.Client) { w.cancel(nil) }

// newWatchedClient is kgo.NewClient for a consumer. It also returns ctx
// narrowed to end, with the authentication error as its cause, once
// authFailureLimit connections in a row have failed to authenticate. Poll
// with that context, and when it ends return authFailure(ctx).
func newWatchedClient(ctx context.Context, opts ...kgo.Opt) (*kgo.Client, context.Context, error) {
	w := &authWatch{}
	ctx, w.cancel = context.WithCancelCause(ctx)
	cl, err := kgo.NewClient(append(opts, kgo.WithHooks(w))...)
	if err != nil {
		w.cancel(nil)
		return nil, nil, fmt.Errorf("creating consumer client: %w", err)
	}
	return cl, ctx, nil
}

// authFailure is the error a watched ctx ended with, or nil when ctx is live
// or was ended by the caller.
func authFailure(ctx context.Context) error {
	if err := context.Cause(ctx); errors.Is(err, errAuthGaveUp) {
		return err
	}
	return nil
}
