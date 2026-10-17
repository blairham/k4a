// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"

	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

// countingIndex is fakeIndex that counts Search calls: a Search of an
// unfollowed topic is what makes k4a-index start a follow.
type countingIndex struct {
	*fakeIndex
	searches atomic.Int64
}

func (c *countingIndex) Search(req *indexv1.SearchRequest, s grpc.ServerStreamingServer[indexv1.SearchEvent]) error {
	c.searches.Add(1)
	return c.fakeIndex.Search(req, s)
}

func sha(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

// serveMCPWithPolicy runs the real HTTP surface — Guard, MCP handler, tool
// wrappers — from flags naming a policy file, so the grant has to travel from
// the Guard through the MCP SDK to the tool handler.
func serveMCPWithPolicy(t *testing.T, idx indexv1.IndexServer) string {
	t.Helper()
	t.Setenv("K4A_MCP_TOKEN", "")
	policy := "callers:\n" +
		"  - name: alice\n    tokenSHA256: " + sha("alice-tok") + "\n    search: ['orders.*']\n    follow: ['orders.*']\n" +
		"  - name: bob\n    tokenSHA256: " + sha("bob-tok") + "\n    search: ['ref.*']\n"
	file := filepath.Join(t.TempDir(), "policy.yaml")
	if err := os.WriteFile(file, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
	f := flags{Path: "/mcp", AuthzPolicy: file}
	listener, err := f.listenerOptions()
	if err != nil {
		t.Fatal(err)
	}
	srv := newMCPServer(dialFake(t, idx), "test", endpointauth.Authorizer{Policy: listener.Policy})
	ts := httptest.NewServer(newHTTPHandler(srv, f.Path, endpointauth.NewGuard(listener)))
	t.Cleanup(ts.Close)
	return ts.URL + "/mcp"
}

func connectAs(t *testing.T, endpoint, token string) (*mcp.ClientSession, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil).Connect(ctx,
		&mcp.StreamableClientTransport{
			Endpoint:             endpoint,
			HTTPClient:           &http.Client{Transport: bearerRT{next: http.DefaultTransport, token: token}},
			MaxRetries:           -1,
			DisableStandaloneSSE: true,
		}, nil)
}

// callTool returns whether the call was refused (a tool error) and, for a
// search, whether it was served from the index.
func callTool(t *testing.T, s *mcp.ClientSession, tool, topic string) (refused, served bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := s.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: map[string]any{"topic": topic}})
	if err != nil {
		t.Fatalf("%s(%s): %v", tool, topic, err)
	}
	if res.IsError {
		return true, false
	}
	var out struct {
		Served bool `json:"served_from_index"`
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return false, out.Served
}

func TestPerCallerAuthorizationOverHTTP(t *testing.T) {
	idx := &countingIndex{fakeIndex: &fakeIndex{following: false}}
	endpoint := serveMCPWithPolicy(t, idx)

	alice, err := connectAs(t, endpoint, "alice-tok")
	if err != nil {
		t.Fatalf("alice connect: %v", err)
	}
	defer alice.Close() //nolint:errcheck // test cleanup
	bob, err := connectAs(t, endpoint, "bob-tok")
	if err != nil {
		t.Fatalf("bob connect: %v", err)
	}
	defer bob.Close() //nolint:errcheck // test cleanup

	t.Run("allowed", func(t *testing.T) {
		before := idx.searches.Load()
		if refused, _ := callTool(t, alice, "search", "orders.eu"); refused {
			t.Error("alice search(orders.eu) refused")
		}
		if refused, _ := callTool(t, alice, "coverage", "orders.eu"); refused {
			t.Error("alice coverage(orders.eu) refused")
		}
		if idx.searches.Load() != before+1 {
			t.Error("alice's search, which may start a follow, did not reach the index")
		}
	})

	t.Run("denied", func(t *testing.T) {
		before := idx.searches.Load()
		for _, tool := range []string{"search", "coverage"} {
			if refused, _ := callTool(t, alice, tool, "ref.rates"); !refused {
				t.Errorf("alice %s(ref.rates) allowed, want refused", tool)
			}
			if refused, _ := callTool(t, bob, tool, "orders.eu"); !refused {
				t.Errorf("bob %s(orders.eu) allowed, want refused", tool)
			}
		}
		if idx.searches.Load() != before {
			t.Error("a refused search reached the index")
		}
	})

	t.Run("search without the follow grant never starts a follow", func(t *testing.T) {
		before := idx.searches.Load()
		refused, served := callTool(t, bob, "search", "ref.rates")
		if refused || served {
			t.Errorf("bob search(ref.rates) refused=%v served=%v, want answered as not followed", refused, served)
		}
		if idx.searches.Load() != before {
			t.Error("bob's search of an unfollowed topic reached the index, which would start a follow")
		}
	})

	t.Run("unauthenticated", func(t *testing.T) {
		for name, tok := range map[string]string{"no token": "", "unknown token": "mallory-tok"} {
			if s, err := connectAs(t, endpoint, tok); err == nil {
				s.Close() //nolint:errcheck,gosec // test cleanup
				t.Errorf("%s: an MCP session opened", name)
			}
		}
	})
}

// TestSearchWithoutFollowGrantOnAFollowedTopic: once the topic is followed (by
// someone allowed to), a caller with only the search grant is served from it.
func TestSearchWithoutFollowGrantOnAFollowedTopic(t *testing.T) {
	idx := &countingIndex{fakeIndex: &fakeIndex{following: true, matches: 1}}
	bob, err := connectAs(t, serveMCPWithPolicy(t, idx), "bob-tok")
	if err != nil {
		t.Fatal(err)
	}
	defer bob.Close() //nolint:errcheck // test cleanup
	if refused, served := callTool(t, bob, "search", "ref.rates"); refused || !served {
		t.Errorf("bob search(ref.rates) refused=%v served=%v, want served from the index", refused, served)
	}
	if idx.searches.Load() != 1 {
		t.Errorf("index saw %d searches, want 1", idx.searches.Load())
	}
}
