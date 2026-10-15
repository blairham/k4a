// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Command k4a-index is the shared always-on index daemon: it tails a set of
// Kafka topics and answers coverage-gated search queries over gRPC for every
// k4a pointed at it, at local-index latency (docs/design/shared-index-service.md).
//
// It is a pure accelerator — a client that cannot reach it falls back to its
// in-process scan, and every response reports coverage explicitly so a stale
// index can never masquerade as current. It reuses internal/index verbatim, so
// "indexed (shared)" can never diverge from "indexed (local)".
//
// Follow-set is demand-driven (Phase 2): the first Search of an allowlisted
// topic registers it as a side effect and the daemon begins tailing it, so
// every subsequent search — by anyone — is served from the warm index. The
// --allow allowlist is the v1 authorization boundary (default-deny; auto-follow
// persists payloads to disk), --topic pre-warms a pinned set at startup, and an
// LRU + cap keep the followed set bounded. See docs/design/shared-index-service.md.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/log"
	goflags "github.com/jessevdk/go-flags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/blairham/k4a/internal/config"
	"github.com/blairham/k4a/internal/endpointauth"
	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
	"github.com/blairham/k4a/internal/version"
)

// flags configures the daemon. Connection resolution mirrors the headless CLI
// commands (config file or direct --brokers), plus the daemon-specific --topic
// and --listen.
type flags struct {
	KeyFile       string        `long:"key"                     env:"KAFKA_TLS_KEY"                     description:"Client private key PEM file (mTLS)"`
	Profile       string        `long:"profile"                 env:"AWS_PROFILE"                       description:"AWS profile (IAM auth)"                                                                                                                                                                                                                                                                                                       short:"p"`
	Brokers       string        `long:"brokers"                 env:"KAFKA_BROKERS"                     description:"Bootstrap servers (comma-separated); overrides config"                                                                                                                                                                                                                                                                        short:"b"`
	Auth          string        `long:"auth"                    env:"KAFKA_AUTH"                        description:"Auth method: plaintext, tls, scram, scram-sha-256, plain, oauthbearer, mtls, iam"                                                                                                                                                                                                                                             short:"a"`
	Username      string        `long:"username"                env:"KAFKA_USERNAME"                    description:"SASL username (scram, scram-sha-256, plain)"`
	Password      string        `long:"password"                env:"KAFKA_PASSWORD"                    description:"SASL password (scram, scram-sha-256, plain)"`
	ConfigFile    string        `long:"config"                                                          description:"Config file path (default: ~/.k4a/config.yaml)"`
	CAFile        string        `long:"ca"                      env:"KAFKA_TLS_CA"                      description:"CA certificate PEM file"`
	Region        string        `long:"region"                  env:"AWS_REGION"                        description:"AWS region (IAM auth)"                                                                                                                                                                                                                                                                                                        short:"r"`
	Context       string        `long:"context"                                                         description:"Named context from ~/.k4a/config.yaml"`
	Listen        string        `long:"listen"                  env:"K4A_INDEX_LISTEN"                  description:"gRPC listen address"                                                                                                                                                                                                                                                                                                          short:"l" default:":9500"`
	DataDir       string        `long:"data-dir"                env:"K4A_INDEX_DATA_DIR"                description:"Index storage root (default: k4a's ~/.k4a)"`
	CertFile      string        `long:"cert"                    env:"KAFKA_TLS_CERT"                    description:"Client certificate PEM file (mTLS)"`
	LogLevel      string        `long:"log-level"               env:"K4A_INDEX_LOG_LEVEL"               description:"Log level: debug, info, warn, error"                                                                                                                                                                                                                                                                                                    default:"info"`
	TopicMaxBytes string        `long:"topic-max-bytes"         env:"K4A_INDEX_TOPIC_MAX_BYTES"         description:"Per-topic index byte cap; the oldest records are trimmed once a topic's index exceeds it. Human size (e.g. 1GiB, 512MB); 0 disables. Total disk ≈ hot-set × this, so keep it under the volume sizeLimit."                                                                                                                               default:"1GiB"`
	MaxTotalBytes string        `long:"max-total-bytes"         env:"K4A_INDEX_MAX_TOTAL_BYTES"         description:"Disk budget for the WHOLE index root, measured on disk. Over budget the daemon deletes orphaned index dirs, then evicts the coldest non-pinned topics until it fits — turning an abrupt volume-full pod eviction into a graceful loss of the coldest coverage. Set it below the volume's size limit. Human size; 0 disables."           default:"0"`
	Prefollow     []string      `long:"topic"                   env:"K4A_INDEX_TOPIC"                   description:"Topics to pre-follow (warm + pinned) at startup; each must match --allow. Repeatable / comma-separated."                                                                                                                                                                                                                      short:"t"                   env-delim:","`
	Allow         []string      `long:"allow"                   env:"K4A_INDEX_ALLOW"                   description:"Follow allowlist: glob patterns of topics the daemon may auto-follow (default-deny; MUST mirror the cluster-side topic grants (IAM policy or ACLs)). Repeatable / comma-separated."                                                                                                                                                                       env-delim:"," required:"true"`
	MaxFollow     int           `long:"max-followed"            env:"K4A_INDEX_MAX_FOLLOWED"            description:"Hard cap on concurrently-followed topics; at the cap a new follow evicts the coldest."                                                                                                                                                                                                                                                  default:"256"`
	EvictAfter    time.Duration `long:"evict-after"             env:"K4A_INDEX_EVICT_AFTER"             description:"Drop a topic not searched within this idle window (LRU). Pinned pre-follow topics are exempt."                                                                                                                                                                                                                                          default:"168h"`
	DrainStall    time.Duration `long:"drain-stall-timeout"     env:"K4A_INDEX_DRAIN_STALL_TIMEOUT"     description:"Fail fast if a topic's indexing drain makes no progress for this long while records are queued: dump all goroutine stacks to stderr and exit, so the pod restarts and re-warms instead of serving nothing silently. 0 disables."                                                                                                        default:"5m"`
	MergeGrace    time.Duration `long:"drain-stall-merge-grace" env:"K4A_INDEX_DRAIN_STALL_MERGE_GRACE" description:"Tolerate a drain stall up to this long while a scorch file merge is in flight: the merger is working, and killing it restarts the merge from scratch on the same volume — a restart livelock. 0 disables the grace."                                                                                                                    default:"30m"`
	WipeStrikes   int           `long:"drain-stall-wipe-after"  env:"K4A_INDEX_DRAIN_STALL_WIPE_AFTER"  description:"On the Nth consecutive drain-stall exit for the same topic index (counted in the index dir, so the count survives container restarts), quarantine the index before exiting so the next boot re-warms fresh. 0 disables."                                                                                                                default:"3"`
	ShutdownGrace time.Duration `long:"shutdown-timeout"        env:"K4A_INDEX_SHUTDOWN_TIMEOUT"        description:"On SIGTERM, how long in-flight RPCs may run before the server stops them. Keep it under the pod's terminationGracePeriodSeconds, leaving room to flush every index. 0 stops them at once."                                                                                                                                              default:"15s"`
	TLSCert       string        `long:"tls-cert"                env:"K4A_INDEX_TLS_CERT"                description:"Server certificate PEM file: serve gRPC over TLS (needs --tls-key). The file is re-read when it changes."`
	TLSKey        string        `long:"tls-key"                 env:"K4A_INDEX_TLS_KEY"                 description:"Server private key PEM file (with --tls-cert)"`
	TLSClientCA   string        `long:"tls-client-ca"           env:"K4A_INDEX_TLS_CLIENT_CA"           description:"Require a client certificate signed by this CA on every RPC except health checks (mTLS; needs --tls-cert)"`
	TokenFile     string        `long:"token-file"              env:"K4A_INDEX_TOKEN_FILE"              description:"File holding the bearer token every RPC except health checks must present. K4A_INDEX_TOKEN sets it directly."`
	MaxLimit      int           `long:"max-search-limit"        env:"K4A_INDEX_MAX_SEARCH_LIMIT"        description:"Most matches one Search returns; a larger or unset request limit is lowered to this and the answer reports capped."                                                                                                                                                                                                                     default:"10000"`
	AdminListen   string        `long:"admin-listen"            env:"K4A_INDEX_ADMIN_LISTEN"            description:"Plain-HTTP address for /metrics (Prometheus), /healthz and /readyz, separate from the gRPC port. Empty disables it."`
	BackfillWin   time.Duration `long:"backfill-window"         env:"K4A_INDEX_BACKFILL_WINDOW"         description:"History a newly followed topic indexes before its live tail, by record time. 0 starts at the latest records."                                                                                                                                                                                                                           default:"24h"`
	BackfillMax   int64         `long:"backfill-max-records"    env:"K4A_INDEX_BACKFILL_MAX_RECORDS"    description:"Cap on that history across all partitions (split evenly); a partition with more in the window starts later. 0 = no cap."                                                                                                                                                                                                                default:"1000000"`
	Insecure      bool          `long:"insecure"                                                        description:"Skip TLS certificate verification"                                                                                                                                                                                                                                                                                            short:"k"`
	NoTLS         bool          `long:"no-tls"                                                          description:"Run a SASL method without TLS (SASL_PLAINTEXT); credentials cross the network in cleartext"`

	OAuth oauthFlags
}

// oauthFlags configures OAUTHBEARER, mirroring the CLI's flags of the same
// names: a static token, a token file, or the client-credentials grant.
type oauthFlags struct {
	Token        string `long:"oauth-token"         env:"KAFKA_OAUTH_TOKEN"         description:"OAUTHBEARER static bearer token"`
	TokenFile    string `long:"oauth-token-file"    env:"KAFKA_OAUTH_TOKEN_FILE"    description:"OAUTHBEARER token file, re-read on every connection"`
	TokenURL     string `long:"oauth-token-url"     env:"KAFKA_OAUTH_TOKEN_URL"     description:"OAUTHBEARER client-credentials token endpoint (https)"`
	ClientID     string `long:"oauth-client-id"     env:"KAFKA_OAUTH_CLIENT_ID"     description:"OAUTHBEARER client ID"`
	ClientSecret string `long:"oauth-client-secret" env:"KAFKA_OAUTH_CLIENT_SECRET" description:"OAUTHBEARER client secret"`
	Scopes       string `long:"oauth-scopes"        env:"KAFKA_OAUTH_SCOPES"        description:"OAUTHBEARER scopes (comma-separated)"`
}

func (o oauthFlags) config() kafka.OAuthConfig {
	return kafka.OAuthConfig{
		Token:        o.Token,
		TokenFile:    o.TokenFile,
		TokenURL:     o.TokenURL,
		ClientID:     o.ClientID,
		ClientSecret: o.ClientSecret,
		Scopes:       o.Scopes,
	}
}

func main() {
	os.Exit(run(os.Args[1:]))
}

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

	// Surface scorch's background failures. Its merger/persister/introducer
	// goroutines recover their own panics and then RETURN, so without this the
	// index quietly loses its merger and wedges minutes later with no trace of
	// why. Logged at error level: a dead merger means that topic's
	// index can never drain again, and the watchdog's quarantine is the cure.
	index.OnAsyncError = func(err error, path string) {
		log.Error("scorch async error — a background index goroutine failed",
			"path", path, "err", err)
	}

	lim, ok := validateFlags(&f)
	if !ok {
		return 1
	}
	logStartup(&f, lim)

	brokers, authCfg, err := resolveConnection(&f)
	if err != nil {
		log.Error("connection", "err", err)
		return 1
	}
	log.Info("resolved connection", "brokers", brokers, "auth", authCfg.Method)
	if w := kafka.CleartextWarning(authCfg); w != "" {
		log.Warn(w)
	}

	client, err := kafka.NewClient(authCfg, brokers)
	if err != nil {
		log.Error("kafka client", "err", err)
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	return serve(ctx, &f, lim, client)
}

// startupLimits is what validateFlags derives from the flags before anything
// touches Kafka.
type startupLimits struct {
	tls           *tls.Config
	listener      endpointauth.ServerOptions
	policy        allowPolicy
	topicMaxBytes int64
	maxTotalBytes int64
}

// validateFlags checks the flags that can be refused without a broker, logging
// the reason and reporting false on the first bad one.
func validateFlags(f *flags) (startupLimits, bool) {
	// The allowlist is the v1 authorization boundary — build it first so a
	// malformed pattern (or an empty list) fails fast before we touch Kafka.
	policy, err := newAllowPolicy(f.Allow)
	if err != nil {
		log.Error("allowlist", "err", err)
		return startupLimits{}, false
	}
	if policy.empty() {
		log.Error("allowlist is empty — the daemon would follow no topics; set --allow / K4A_INDEX_ALLOW")
		return startupLimits{}, false
	}
	// Pre-follow topics are operator-pinned but still subject to the allowlist:
	// a pinned topic outside the allowlist is a config contradiction, so reject
	// it here rather than silently never following it.
	for _, t := range f.Prefollow {
		if !policy.allowed(t) {
			log.Error("pre-follow topic is not in the allowlist", "topic", t, "allow", policy.String())
			return startupLimits{}, false
		}
	}

	topicMaxBytes, err := parseByteSize(f.TopicMaxBytes)
	if err != nil {
		log.Error("topic-max-bytes", "err", err)
		return startupLimits{}, false
	}

	maxTotalBytes, err := parseByteSize(f.MaxTotalBytes)
	if err != nil {
		log.Error("max-total-bytes", "err", err)
		return startupLimits{}, false
	}
	// A budget below one topic's cap would evict every topic on every sweep and
	// still never fit. Refuse it rather than thrash. Note the on-disk footprint
	// runs well above the stored-bytes cap, so this is a floor, not a target.
	if maxTotalBytes > 0 && maxTotalBytes < topicMaxBytes {
		log.Error("max-total-bytes is below topic-max-bytes — every topic would be evicted and it still would not fit",
			"max_total_bytes", maxTotalBytes, "topic_max_bytes", topicMaxBytes)
		return startupLimits{}, false
	}
	if f.MaxLimit <= 0 || f.MaxLimit > math.MaxInt32 {
		log.Error("max-search-limit must be between 1 and 2147483647", "max_search_limit", f.MaxLimit)
		return startupLimits{}, false
	}
	if f.BackfillWin < 0 || f.BackfillMax < 0 {
		log.Error("backfill-window and backfill-max-records must not be negative",
			"backfill_window", f.BackfillWin, "backfill_max_records", f.BackfillMax)
		return startupLimits{}, false
	}
	lim := startupLimits{policy: policy, topicMaxBytes: topicMaxBytes, maxTotalBytes: maxTotalBytes}
	if lim.listener, lim.tls, err = listenerAuth(f); err != nil {
		log.Error("listener auth", "err", err)
		return startupLimits{}, false
	}
	return lim, true
}

// listenerAuth loads the gRPC listener's TLS material and bearer token. A
// half-configured listener is refused rather than served without the part
// the operator meant to add.
func listenerAuth(f *flags) (endpointauth.ServerOptions, *tls.Config, error) {
	token, err := endpointauth.LoadToken(f.TokenFile, os.Getenv("K4A_INDEX_TOKEN"))
	if err != nil {
		return endpointauth.ServerOptions{}, nil, err
	}
	o := endpointauth.ServerOptions{
		CertFile:     f.TLSCert,
		KeyFile:      f.TLSKey,
		ClientCAFile: f.TLSClientCA,
		Token:        token,
	}
	cfg, err := endpointauth.ServerTLS(o)
	if err != nil {
		return endpointauth.ServerOptions{}, nil, err
	}
	return o, cfg, nil
}

// warnListenerExposure says, once at startup, what reaching the port grants.
// The default stays plaintext and open so an upgrade does not break a running
// install, but it should never be that way silently.
func warnListenerExposure(o endpointauth.ServerOptions) {
	switch {
	case !o.TLSEnabled() && !o.Authenticated():
		log.Warn("gRPC listener is plaintext and unauthenticated: anyone who can reach it can search every " +
			"followed topic and start follows; set --token-file or --tls-client-ca (SECURITY.md)")
	case !o.TLSEnabled():
		log.Warn("gRPC listener is plaintext: the bearer token crosses the network in the clear unless TLS " +
			"is terminated in front of it; set --tls-cert/--tls-key to serve TLS here")
	case !o.Authenticated():
		log.Warn("gRPC listener serves TLS but asks no caller credential: set --token-file or --tls-client-ca")
	}
}

func logStartup(f *flags, lim startupLimits) {
	log.Info(
		"k4a-index starting",
		"version", version.String(),
		"allow", lim.policy.String(),
		"prefollow", f.Prefollow,
		"max_followed", f.MaxFollow,
		"topic_max_bytes", lim.topicMaxBytes,
		"max_total_bytes", lim.maxTotalBytes,
		"evict_after", f.EvictAfter,
		"drain_stall_timeout", f.DrainStall,
		"drain_stall_merge_grace", f.MergeGrace,
		"drain_stall_wipe_after", f.WipeStrikes,
		"shutdown_timeout", f.ShutdownGrace,
		"backfill_window", f.BackfillWin,
		"backfill_max_records", f.BackfillMax,
		"listen", f.Listen,
		"admin_listen", f.AdminListen,
		"tls", lim.listener.TLSEnabled(),
		"client_ca", lim.listener.ClientCAFile != "",
		"token", lim.listener.Token != "",
		"max_search_limit", f.MaxLimit,
		"data_dir", f.dataRoot(),
		"auth", f.Auth,
		"log_level", f.LogLevel,
	)
}

// serve builds the follow-set and runs the gRPC server until ctx is signaled,
// returning the process exit code.
func serve(ctx context.Context, f *flags, lim startupLimits, client *kafka.Client) int {
	// The cluster ID keys the index directory so two clusters' identically
	// named topics never collide — the same isolation the local index uses.
	cluster, err := client.ClusterID(ctx)
	if err != nil {
		log.Error("resolving cluster id", "err", err)
		return 1
	}
	if cluster == "" {
		cluster = "unknown-cluster"
	}

	root := state.NewRoot(f.dataRoot())
	mgr := newManager(ctx, root, cluster, client, lim.policy, managerConfig{
		MaxFollowed:   f.MaxFollow,
		EvictAfter:    f.EvictAfter,
		MaxTotalBytes: lim.maxTotalBytes,
		Worker: workerOptions{
			TrimBytes:   lim.topicMaxBytes,
			StallAfter:  f.DrainStall,
			MergeGrace:  f.MergeGrace,
			WipeStrikes: f.WipeStrikes,
			Backfill:    backfillOptions{Window: f.BackfillWin, MaxRecords: f.BackfillMax},
		},
	})
	defer mgr.stopAll()

	// Warm the pinned pre-follow set before serving, then start the LRU sweeper.
	// Everything else follows on demand, gated by the allowlist.
	mgr.preFollow(f.Prefollow)
	// Registration runs on the manager's baseCtx, which is this ctx.
	restored := mgr.restoreFollowSet() //nolint:contextcheck // see comment above
	go mgr.sweep()
	log.Info("follow-set ready", "cluster", cluster, "followed", mgr.size(), "restored", restored)

	srv, hs := newGRPCServer(lim)
	is := newIndexServer(mgr)
	is.maxLimit = f.MaxLimit
	indexv1.RegisterIndexServer(srv, is)
	warnListenerExposure(lim.listener)

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", f.Listen)
	if err != nil {
		log.Error("listen", "addr", f.Listen, "err", err)
		return 1
	}

	if f.AdminListen != "" {
		if err := serveAdmin(ctx, f.AdminListen, newAdminHandler(newMetricsRegistry(mgr), hs)); err != nil {
			log.Error("admin listen", "addr", f.AdminListen, "err", err)
			return 1
		}
	}

	// Stop the gRPC server when the process is signaled. Readiness drops
	// first, so load balancers stop routing before in-flight RPCs drain.
	go func() {
		<-ctx.Done()
		log.Info("shutting down", "grace", f.ShutdownGrace)
		hs.Shutdown()
		stopServer(srv, f.ShutdownGrace)
	}()

	setServing(hs, true)
	log.Info("k4a-index serving", "addr", f.Listen, "version", version.String())
	if err := srv.Serve(lis); err != nil && ctx.Err() == nil {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
}

// newGRPCServer builds the server with its transport credentials and
// interceptors. Logging runs outside the guard so a refused call is still
// logged, with its Unauthenticated code.
func newGRPCServer(lim startupLimits) (*grpc.Server, *health.Server) {
	guard := endpointauth.NewGuard(lim.listener)
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryLogInterceptor, guard.UnaryInterceptor),
		grpc.ChainStreamInterceptor(streamLogInterceptor, guard.StreamInterceptor),
	}
	if lim.tls != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(lim.tls)))
	}
	srv := grpc.NewServer(opts...)
	// The standard health service, open whatever credentials the listener
	// asks for, so a probe never needs the deployment's token. It starts
	// NOT_SERVING; serve flips it once the listener is up and back on SIGTERM.
	hs := health.NewServer()
	setServing(hs, false)
	healthpb.RegisterHealthServer(srv, hs)
	return srv, hs
}

// indexServiceName is the health service name clients may ask about, besides
// the server-wide "".
const indexServiceName = "k4a.index.v1.Index"

func setServing(hs *health.Server, up bool) {
	st := healthpb.HealthCheckResponse_NOT_SERVING
	if up {
		st = healthpb.HealthCheckResponse_SERVING
	}
	hs.SetServingStatus("", st)
	hs.SetServingStatus(indexServiceName, st)
}

// grpcStopper is the slice of *grpc.Server stopServer needs.
type grpcStopper interface {
	GracefulStop()
	Stop()
}

// stopServer drains in-flight RPCs for up to grace, then cuts off whatever is
// left. GracefulStop alone waits for every stream to finish, so one stalled
// Search could hold shutdown open until the kubelet's SIGKILL — which skips
// the deferred stopAll, so no index gets its final flush. Stop is safe to call
// while GracefulStop is still waiting; it makes GracefulStop return.
func stopServer(srv grpcStopper, grace time.Duration) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	if grace <= 0 {
		srv.Stop()
		<-done
		return
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		log.Warn("shutdown: in-flight RPCs outlived the grace period — stopping them", "grace", grace)
		srv.Stop()
		<-done
	}
}

// byteSuffixes are tried in order; the first suffix that matches wins, so a
// longer spelling must precede any suffix it ends with ("TIB" before "B").
var byteSuffixes = []struct {
	suffix string
	mult   int64
}{
	{"TIB", 1 << 40},
	{"TI", 1 << 40},
	{"GIB", 1 << 30},
	{"GI", 1 << 30},
	{"MIB", 1 << 20},
	{"MI", 1 << 20},
	{"KIB", 1 << 10},
	{"KI", 1 << 10},
	{"TB", 1_000_000_000_000},
	{"GB", 1_000_000_000},
	{"MB", 1_000_000},
	{"KB", 1_000},
	{"B", 1},
}

// parseByteSize parses a human byte-size string into bytes. Accepts binary
// suffixes (KiB/MiB/GiB/TiB and their k8s Ki/Mi/Gi/Ti forms, ×1024), decimal
// suffixes (KB/MB/GB/TB, ×1000), a trailing "B", or a bare integer (bytes).
// Empty or "0" means disabled (no cap). Case-insensitive.
func parseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	u := strings.ToUpper(s)
	var mult int64 = 1
	for _, bs := range byteSuffixes {
		if rest, ok := strings.CutSuffix(u, bs.suffix); ok {
			u, mult = rest, bs.mult
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(u), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size %q must be non-negative", s)
	}
	// Unchecked, the product wraps: 8388608TiB came back negative, which
	// reads as "no cap", and other sizes wrap to an arbitrary value.
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("byte size %q overflows int64", s)
	}
	return n * mult, nil
}

// dataRoot picks the index storage root: --data-dir if set, else k4a's default
// ~/.k4a so the daemon and a local k4a share the same on-disk layout.
func (f *flags) dataRoot() string {
	if f.DataDir != "" {
		return f.DataDir
	}
	return config.DefaultConfigDir()
}

// resolveConnection resolves brokers + auth from either --brokers (direct) or
// the config file's named context. It reuses internal/config's primitives —
// the same ones the headless CLI ConnectionFlags.Resolve uses — rather than
// importing the whole command package into the daemon binary.
func resolveConnection(f *flags) (string, kafka.AuthConfig, error) {
	flagAuth := kafka.AuthConfig{
		Method:   kafka.AuthMethod(f.Auth),
		Username: f.Username,
		Password: f.Password,
		CertFile: f.CertFile,
		KeyFile:  f.KeyFile,
		CAFile:   f.CAFile,
		Region:   f.Region,
		Profile:  f.Profile,
		OAuth:    f.OAuth.config(),
		Insecure: f.Insecure,
		NoTLS:    f.NoTLS,
	}
	if f.Brokers != "" {
		// A direct connection has no context to inherit defaults from, so the
		// method is inferred exactly as the CLI infers it (#97).
		if flagAuth.Method == "" {
			flagAuth.Method = kafka.InferAuthMethod(flagAuth)
		}
		return f.Brokers, flagAuth, nil
	}

	cfgPath := f.ConfigFile
	if cfgPath == "" {
		cfgPath = config.DefaultConfigPath()
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", kafka.AuthConfig{}, err
	}
	cctx, err := cfg.Resolve(f.Context)
	if err != nil {
		return "", kafka.AuthConfig{}, fmt.Errorf(
			"%w\n\nUse --brokers to connect directly, or create a config at %s",
			err,
			cfgPath,
		)
	}
	brokers, err := cctx.ResolveBrokers()
	if err != nil {
		return "", kafka.AuthConfig{}, err
	}
	return brokers, overlayAuth(cctx.ToAuthConfig(), flagAuth), nil
}

// overlayAuth applies every connection flag that was set over the context's
// values — the same rule as internal/command's overlayAuth, kept here because
// the daemon resolves its connection without importing the command package.
// An empty value means "not set", which is why --region has no flag default:
// an explicit --region us-east-1 must still override a context in another
// region. --insecure can only turn verification off, and --no-tls only TLS.
func overlayAuth(base, flags kafka.AuthConfig) kafka.AuthConfig {
	for _, f := range []struct{ dst, src *string }{
		{&base.Username, &flags.Username},
		{&base.Password, &flags.Password},
		{&base.CertFile, &flags.CertFile},
		{&base.KeyFile, &flags.KeyFile},
		{&base.CAFile, &flags.CAFile},
		{&base.Region, &flags.Region},
		{&base.Profile, &flags.Profile},
		{&base.OAuth.Token, &flags.OAuth.Token},
		{&base.OAuth.TokenFile, &flags.OAuth.TokenFile},
		{&base.OAuth.TokenURL, &flags.OAuth.TokenURL},
		{&base.OAuth.ClientID, &flags.OAuth.ClientID},
		{&base.OAuth.ClientSecret, &flags.OAuth.ClientSecret},
		{&base.OAuth.Scopes, &flags.OAuth.Scopes},
	} {
		if *f.src != "" {
			*f.dst = *f.src
		}
	}
	if flags.Method != "" {
		base.Method = flags.Method
	}
	if flags.Insecure {
		base.Insecure = true
	}
	if flags.NoTLS {
		base.NoTLS = true
	}
	return base
}
