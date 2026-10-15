// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/health"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/state"
)

func TestLagRecords(t *testing.T) {
	ends := map[int32]int64{0: 100, 1: 50, 2: 10}
	newest := map[int32]int64{0: 89, 1: 49} // 10 behind, caught up; 2 never indexed
	if got := lagRecords(ends, newest); got != 10 {
		t.Errorf("lagRecords = %d, want 10", got)
	}
	if got := lagRecords(ends, nil); got != 0 {
		t.Errorf("no indexed partitions: %d, want 0", got)
	}
}

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/metrics") //nolint:noctx // test
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close() //nolint:errcheck // test
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestMetricsReflectAFollowedTopic follows a real topic on a fake cluster,
// searches it, samples lag, and reads the result back off /metrics, so a
// series that is declared but never fed fails here.
func TestMetricsReflectAFollowedTopic(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	produceRecords(t, seeds, testTopic, recordCount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, seeds),
		mustPolicy(t, testTopic), managerConfig{})
	defer mgr.stopAll()
	mgr.preFollow([]string{testTopic})
	w := mgr.lookup(testTopic)
	if w == nil {
		t.Fatal("pre-follow did not register the topic")
	}
	w.waitReady(ctx)
	waitIndexed(t, w, recordCount)
	w.sampleLag(ctx, w.async.Bookkeeper())

	served := testutil.ToFloat64(searchesTotal.WithLabelValues(searchServed))
	stream, err := dialServer(t, mgr).Search(ctx, &indexv1.SearchRequest{
		Topic: testTopic, Pattern: "needle", Scopes: []string{"value"}, Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	drainSearchCapped(t, stream)
	if got := testutil.ToFloat64(searchesTotal.WithLabelValues(searchServed)) - served; got != 1 {
		t.Errorf("served searches rose by %v, want 1", got)
	}

	body := scrape(t, newAdminHandler(newMetricsRegistry(mgr), health.NewServer()))
	for _, want := range []string{
		"k4a_index_followed_topics 1",
		fmt.Sprintf(`k4a_index_topic_indexed_records_total{topic=%q} %d`, testTopic, recordCount),
		fmt.Sprintf(`k4a_index_topic_dropped_records_total{topic=%q} 0`, testTopic),
		fmt.Sprintf(`k4a_index_topic_lag_records{topic=%q} 0`, testTopic),
		fmt.Sprintf(`k4a_index_topic_pinned{topic=%q} 1`, testTopic),
		fmt.Sprintf(`k4a_index_topic_newest_record_timestamp_seconds{topic=%q}`, testTopic),
		fmt.Sprintf(`k4a_index_topic_stored_bytes{topic=%q}`, testTopic),
		"k4a_index_search_duration_seconds_count",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}

func TestCapEvictionIsCounted(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, fake.ListenAddrs()),
		mustPolicy(t, topicA, topicB), managerConfig{MaxFollowed: 1})
	defer mgr.stopAll()

	before := testutil.ToFloat64(evictionsTotal.WithLabelValues(evictCap))
	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)
	mgr.ensureFollowed(topicB)
	waitFollowed(t, mgr, topicB)
	if got := testutil.ToFloat64(evictionsTotal.WithLabelValues(evictCap)) - before; got != 1 {
		t.Errorf("cap evictions rose by %v, want 1", got)
	}
}

func TestReadyzFollowsHealth(t *testing.T) {
	hs := health.NewServer()
	ts := httptest.NewServer(newAdminHandler(newMetricsRegistry(newManager(context.Background(),
		state.NewRoot(t.TempDir()), "c", nil, allowPolicy{}, managerConfig{})), hs))
	defer ts.Close()
	code := func(path string) int {
		resp, err := http.Get(ts.URL + path) //nolint:noctx // test
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close() //nolint:errcheck,gosec // test
		return resp.StatusCode
	}

	setServing(hs, false)
	if got := code("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz while NOT_SERVING = %d, want 503", got)
	}
	if got := code("/healthz"); got != http.StatusOK {
		t.Errorf("/healthz = %d, want 200 whatever readiness says", got)
	}
	setServing(hs, true)
	if got := code("/readyz"); got != http.StatusOK {
		t.Errorf("/readyz while SERVING = %d, want 200", got)
	}
	hs.Shutdown()
	if got := code("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz after shutdown began = %d, want 503", got)
	}
}
