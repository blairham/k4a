// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package remoteindex is the client-side backend for the shared k4a-index
// daemon (docs/design/shared-index-service.md §4 — "The client remote
// backend"). It dials the daemon's gRPC surface and, coverage-gated, answers a
// search from the shared warm index; when the daemon is unreachable or does not
// cover the query, it transparently falls back to the caller's in-process scan
// and fires a fire-and-forget Follow so the next search warms. The daemon is a
// pure accelerator and never a hard dependency.
package remoteindex

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
)

// coverageTimeout bounds the pre-flight Coverage/Follow RPCs so an
// unreachable daemon degrades to a scan quickly instead of hanging the search.
const coverageTimeout = 3 * time.Second

// SearchFunc is the shape every k4a search source shares: given a topic and
// params, stream matches, progress, and errors. Mirrors views.SearchFunc and
// searchcache.Cache.Search so the remote source composes with them.
type SearchFunc func(ctx context.Context, topic string, params kafka.SearchParams) (
	<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
)

// Client is a connection to a shared k4a-index daemon.
type Client struct {
	conn *grpc.ClientConn
	idx  indexv1.IndexClient
}

// Dial opens a lazy gRPC connection to endpoint (host:port). By default it uses
// TLS verified against the system roots — the daemon serves plaintext gRPC and
// is expected behind a TLS-terminating load balancer. insecureTLS uses a plaintext
// connection instead (a local/port-forwarded daemon on its plaintext gRPC
// port). grpc.NewClient is lazy: it does not block on connectivity here; the
// first RPC connects (and times out fast via coverageTimeout if it can't).
func Dial(endpoint string, insecureTLS bool) (*Client, error) {
	if endpoint == "" {
		return nil, errors.New("remoteindex: empty endpoint")
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	if insecureTLS {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("remoteindex: dial %q: %w", endpoint, err)
	}
	return &Client{conn: conn, idx: indexv1.NewIndexClient(conn)}, nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Wrap returns a SearchFunc that consults the shared index first and falls back
// to next. It runs the SAME coverage decision the local index uses
// (index.Bookkeeper.CanServe on a Bookkeeper reconstructed from the daemon's
// wire coverage), so "indexed (shared)" can never claim coverage the local
// path wouldn't:
//
//   - Covered      → stream the daemon's matches (Source = SourceSharedIndex).
//   - Not covered  → (topic unfollowed, tail lag, gap, or unbounded query) fire
//     a fire-and-forget Follow so the next search warms, then run next() — the
//     correctness-floor scan.
//   - Unreachable  → run next(); the daemon is never a hard dependency.
func (c *Client) Wrap(next SearchFunc) SearchFunc {
	return func(ctx context.Context, topic string, params kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		if c.covers(ctx, topic, params) {
			return c.streamSearch(ctx, topic, params)
		}
		// Not covered (or unreachable): warm the topic for next time, then scan.
		// WithoutCancel detaches the warm-up from this search's lifetime (it must
		// finish even after the search returns) while keeping the context lineage.
		go c.follow(context.WithoutCancel(ctx), topic)
		return next(ctx, topic, params)
	}
}

// covers reports whether the daemon can authoritatively serve params for topic.
// A dead daemon, an unfollowed topic, or a query the coverage doesn't span all
// return false — the caller then scans.
func (c *Client) covers(ctx context.Context, topic string, params kafka.SearchParams) bool {
	cctx, cancel := context.WithTimeout(ctx, coverageTimeout)
	defer cancel()

	resp, err := c.idx.Coverage(cctx, &indexv1.CoverageRequest{Topic: topic})
	if err != nil || resp.GetCoverage() == nil || !resp.GetCoverage().GetFollowing() {
		return false
	}
	book := indexwire.BookkeeperFromCoverage(resp.GetCoverage())
	ok, _ := book.CanServe(toQueryParams(params))
	return ok
}

// streamSearch runs the query against the daemon and pipes results through the
// standard three-channel shape, tagging the terminal progress with
// SourceSharedIndex so the header reads "indexed (shared) · …".
func (c *Client) streamSearch(ctx context.Context, topic string, params kafka.SearchParams) (
	<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
) {
	matchCh := make(chan kafka.ConsumedMessage, 256)
	progCh := make(chan kafka.DeepSearchProgress, 1)
	errCh := make(chan error, 1)

	go func() {
		defer close(matchCh)
		defer close(progCh)
		defer close(errCh)

		start := time.Now()
		req := indexwire.QueryParamsToRequest(topic, toQueryParams(params))
		stream, err := c.idx.Search(ctx, req)
		if err != nil {
			sendErr(errCh, fmt.Errorf("shared index search: %w", err))
			return
		}

		matched := 0
		capped := false
		for {
			ev, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				if ctx.Err() != nil {
					return // caller canceled — not an error worth surfacing
				}
				sendErr(errCh, fmt.Errorf("shared index stream: %w", err))
				return
			}
			switch e := ev.GetEvent().(type) {
			case *indexv1.SearchEvent_Match:
				select {
				case matchCh <- indexwire.ConsumedFromMatch(topic, e.Match):
					matched++
				case <-ctx.Done():
					return
				}
			case *indexv1.SearchEvent_Progress:
				if e.Progress.GetDone() {
					if n := int(e.Progress.GetMatched()); n > matched {
						matched = n
					}
					if e.Progress.GetCapped() {
						capped = true
					}
				}
			case *indexv1.SearchEvent_Coverage:
				// Leading coverage frame — already gated in covers(); ignore.
			}
		}

		// A daemon predating the capped field has no capped field and leaves it false, so
		// do not trust the flag alone: receiving exactly the requested limit
		// means the limit bound. Inferring it here keeps a truncated result
		// from reading as complete against an un-redeployed daemon.
		if !capped && matched > 0 && matched == int(req.GetLimit()) {
			capped = true
		}

		select {
		case progCh <- kafka.DeepSearchProgress{
			Elapsed: time.Since(start),
			Matches: matched,
			Source:  kafka.SourceSharedIndex,
			Capped:  capped,
			Done:    true,
		}:
		default:
		}
	}()

	return matchCh, progCh, errCh
}

// follow registers topic with the daemon so the next search of it is warm.
// Best-effort: errors (including an unreachable daemon) are ignored.
func (c *Client) follow(ctx context.Context, topic string) {
	ctx, cancel := context.WithTimeout(ctx, coverageTimeout)
	defer cancel()
	_, _ = c.idx.Follow(ctx, &indexv1.FollowRequest{Topic: topic}) //nolint:errcheck // fire-and-forget warm-up
}

// toQueryParams converts a kafka.SearchParams into the index layer's
// QueryParams — the shape CanServe + the wire request use. The scope bitmask
// becomes the string-slice scopes; Cap becomes Limit.
//
// The cap is resolved rather than passed through: sending 0 would let the
// daemon apply its own default, which both hides the real bound from the
// caller and defeats the "matched == limit means capped" inference used
// against daemons predating the capped field.
func toQueryParams(p kafka.SearchParams) index.QueryParams {
	out := index.QueryParams{
		Pattern:    p.Pattern,
		Since:      p.Since,
		Until:      p.Until,
		Partitions: p.Partitions,
		Limit:      p.EffectiveCap(),
	}
	scope := p.Scope
	if scope == 0 {
		scope = kafka.ScopeKey | kafka.ScopeValue
	}
	if scope&kafka.ScopeKey != 0 {
		out.Scopes = append(out.Scopes, "key")
	}
	if scope&kafka.ScopeValue != 0 {
		out.Scopes = append(out.Scopes, "value")
	}
	if scope&kafka.ScopeHeaders != 0 {
		out.Scopes = append(out.Scopes, "headers")
	}
	return out
}

func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}
