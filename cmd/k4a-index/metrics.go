// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/charmbracelet/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Event counters and the search histogram are process-wide: the daemon runs
// one manager and one server, and tests read deltas, so nothing needs to
// thread a metrics handle through every constructor.
var (
	searchesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k4a_index_searches_total",
		Help: "Search RPCs by outcome: served (from a followed index), warming (allowlisted, " +
			"now following, client scans), denied (not allowlisted), forbidden (the caller's " +
			"--authz-policy grant does not cover the topic), error.",
	}, []string{"outcome"})
	searchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "k4a_index_search_duration_seconds",
		Help:    "Time to answer a Search served from a followed index, including streaming the matches.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	evictionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "k4a_index_evictions_total",
		Help: "Topics dropped from the follow-set by reason: cap (a new follow at --max-followed), " +
			"idle (--evict-after), disk_budget (--max-total-bytes), tail_exit (the tail stopped on its own).",
	}, []string{"reason"})
)

// Eviction reasons, as the label values of k4a_index_evictions_total.
const (
	evictCap        = "cap"
	evictIdle       = "idle"
	evictDiskBudget = "disk_budget"
	evictTailExit   = "tail_exit"
)

// Search outcomes, as the label values of k4a_index_searches_total.
const (
	searchServed    = "served"
	searchWarming   = "warming"
	searchDenied    = "denied"
	searchForbidden = "forbidden"
	searchError     = "error"
)

// followSetCollector reads the follow-set at scrape time, so a gauge can never
// disagree with the set it describes and an evicted topic's series disappear
// with it rather than freezing at their last value.
type followSetCollector struct {
	mgr *manager

	followed  *prometheus.Desc
	indexed   *prometheus.Desc
	dropped   *prometheus.Desc
	buffered  *prometheus.Desc
	stored    *prometheus.Desc
	lag       *prometheus.Desc
	newest    *prometheus.Desc
	pinnedLbl *prometheus.Desc
}

func newFollowSetCollector(mgr *manager) *followSetCollector {
	topic := []string{"topic"}
	return &followSetCollector{
		mgr:      mgr,
		followed: prometheus.NewDesc("k4a_index_followed_topics", "Topics currently followed.", nil, nil),
		indexed: prometheus.NewDesc("k4a_index_topic_indexed_records_total",
			"Records the tail handed to the indexer since this topic was followed.", topic, nil),
		dropped: prometheus.NewDesc("k4a_index_topic_dropped_records_total",
			"Records dropped because the indexer's buffer was full; each one is a coverage gap.", topic, nil),
		buffered: prometheus.NewDesc("k4a_index_topic_buffered_records",
			"Records waiting in the indexer's buffer.", topic, nil),
		stored: prometheus.NewDesc("k4a_index_topic_stored_bytes",
			"Bytes of records the topic's index holds (what --topic-max-bytes caps; on disk runs higher).", topic, nil),
		lag: prometheus.NewDesc("k4a_index_topic_lag_records",
			"Records on the broker past the newest indexed offset, summed over partitions with indexed "+
				"records; sampled every 15s, absent until the first sample.", topic, nil),
		newest: prometheus.NewDesc("k4a_index_topic_newest_record_timestamp_seconds",
			"Timestamp of the newest indexed record; time() minus this is how stale the index is.", topic, nil),
		pinnedLbl: prometheus.NewDesc("k4a_index_topic_pinned",
			"1 for a topic pinned by --topic (exempt from eviction), 0 otherwise.", topic, nil),
	}
}

func (c *followSetCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.followed, c.indexed, c.dropped, c.buffered, c.stored, c.lag, c.newest, c.pinnedLbl,
	} {
		ch <- d
	}
}

func (c *followSetCollector) Collect(ch chan<- prometheus.Metric) {
	entries := c.mgr.snapshot()
	ch <- prometheus.MustNewConstMetric(c.followed, prometheus.GaugeValue, float64(len(entries)))
	for topic, e := range entries {
		w := e.worker
		book := w.async.Bookkeeper()
		ch <- prometheus.MustNewConstMetric(c.indexed, prometheus.CounterValue, float64(w.submitted.Load()), topic)
		ch <- prometheus.MustNewConstMetric(c.dropped, prometheus.CounterValue, float64(w.async.Dropped()), topic)
		ch <- prometheus.MustNewConstMetric(c.buffered, prometheus.GaugeValue, float64(w.async.Buffered()), topic)
		ch <- prometheus.MustNewConstMetric(c.stored, prometheus.GaugeValue, float64(book.ByteSize), topic)
		if lag := w.lag.Load(); lag >= 0 {
			ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, float64(lag), topic)
		}
		var newest int64
		for _, ms := range book.NewestTimePerPartition {
			newest = max(newest, ms)
		}
		if newest > 0 {
			ch <- prometheus.MustNewConstMetric(c.newest, prometheus.GaugeValue, float64(newest)/1000, topic)
		}
		pinned := 0.0
		if e.pinned {
			pinned = 1
		}
		ch <- prometheus.MustNewConstMetric(c.pinnedLbl, prometheus.GaugeValue, pinned, topic)
	}
}

// newMetricsRegistry is a private registry rather than the global default, so
// nothing a dependency registers leaks onto the endpoint by accident.
func newMetricsRegistry(mgr *manager) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		searchesTotal, searchDuration, evictionsTotal,
		newFollowSetCollector(mgr),
	)
	return reg
}

// newAdminHandler serves /metrics plus plain-HTTP health for probes that
// cannot speak gRPC over TLS (the kubelet's gRPC probe has no TLS): /healthz
// answers while the process serves HTTP, /readyz mirrors the gRPC health
// status. The port carries topic names and counts but no record content.
func newAdminHandler(reg *prometheus.Registry, hs *health.Server) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		resp, err := hs.Check(r.Context(), &healthpb.HealthCheckRequest{})
		if err != nil || resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			http.Error(w, "not serving", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// serveAdmin runs the admin HTTP server on addr until ctx is done. A listen
// failure is returned so startup can refuse it; a later serve error is only
// logged, since losing metrics must not take the index down.
func serveAdmin(ctx context.Context, addr string, h http.Handler) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		// ctx is already done; keep its values but not its cancellation.
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx) //nolint:errcheck // best-effort drain
	}()
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin server", "addr", addr, "err", err)
		}
	}()
	log.Info("admin endpoint serving", "addr", addr, "paths", "/metrics /healthz /readyz")
	return nil
}
