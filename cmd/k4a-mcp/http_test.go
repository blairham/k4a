// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/blairham/k4a/internal/endpointauth"
)

// roundRobin sends each HTTP request to the next replica in turn, standing in
// for a Kubernetes Service with no session affinity in front of several pods.
type roundRobin struct {
	replicas []*url.URL
	hits     []atomic.Uint64
	next     atomic.Uint64
}

func (rr *roundRobin) RoundTrip(req *http.Request) (*http.Response, error) {
	i := int(rr.next.Add(1)-1) % len(rr.replicas)
	rr.hits[i].Add(1)
	req = req.Clone(req.Context())
	req.URL.Scheme = rr.replicas[i].Scheme
	req.URL.Host = rr.replicas[i].Host
	req.Host = rr.replicas[i].Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestToolCallAcrossReplicasWithoutSharedSession puts two independent
// k4a-mcp handlers — separate mcp.Server instances, sharing nothing but the
// index — behind round-robin routing, so the client's initialize lands on one
// and its tools/call on the other. A stateful handler answers the second
// request "session not found"; the stateless one serves it, which is what
// lets the chart scale replicas without sticky sessions (#33).
func TestToolCallAcrossReplicasWithoutSharedSession(t *testing.T) {
	client := dialFake(t, &fakeIndex{following: true, matches: 2})

	rr := &roundRobin{hits: make([]atomic.Uint64, 2)}
	for range 2 {
		ts := httptest.NewServer(newMCPHandler(newMCPServer(client, "test", endpointauth.Authorizer{})))
		t.Cleanup(ts.Close)
		u, err := url.Parse(ts.URL)
		if err != nil {
			t.Fatal(err)
		}
		rr.replicas = append(rr.replicas, u)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mc := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	session, err := mc.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             "http://replica/mcp",
		HTTPClient:           &http.Client{Transport: rr},
		MaxRetries:           -1,
		DisableStandaloneSSE: true,
	}, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() }) //nolint:errcheck // test cleanup

	for i := range 3 {
		res, err := session.CallTool(ctx, &mcp.CallToolParams{
			Name:      "coverage",
			Arguments: map[string]any{"topic": "orders"},
		})
		if err != nil {
			t.Fatalf("tools/call %d: %v", i, err)
		}
		if res.IsError {
			t.Fatalf("tools/call %d returned a tool error: %+v", i, res.Content)
		}
	}

	// Prove the routing actually split the session across both replicas;
	// otherwise this test would pass against a stateful handler too.
	for i := range rr.hits {
		if rr.hits[i].Load() == 0 {
			t.Fatalf("replica %d served no requests; routing did not split the session", i)
		}
	}
}
