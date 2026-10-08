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
	"fmt"
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

	"github.com/blairham/k4a/internal/config"
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
	Auth          string        `long:"auth"                    env:"KAFKA_AUTH"                        description:"Auth method: plaintext, tls, scram, mtls, iam"                                                                                                                                                                                                                                                                                short:"a"`
	Username      string        `long:"username"                env:"KAFKA_USERNAME"                    description:"SASL/SCRAM username"`
	Password      string        `long:"password"                env:"KAFKA_PASSWORD"                    description:"SASL/SCRAM password"`
	ConfigFile    string        `long:"config"                                                          description:"Config file path (default: ~/.k4a/config.yaml)"`
	CAFile        string        `long:"ca"                      env:"KAFKA_TLS_CA"                      description:"CA certificate PEM file"`
	Region        string        `long:"region"                  env:"AWS_REGION"                        description:"AWS region (IAM auth)"                                                                                                                                                                                                                                                                                                        short:"r" default:"us-east-1"`
	Context       string        `long:"context"                                                         description:"Named context from ~/.k4a/config.yaml"`
	Listen        string        `long:"listen"                  env:"K4A_INDEX_LISTEN"                  description:"gRPC listen address"                                                                                                                                                                                                                                                                                                          short:"l" default:":9500"`
	DataDir       string        `long:"data-dir"                env:"K4A_INDEX_DATA_DIR"                description:"Index storage root (default: k4a's ~/.k4a)"`
	CertFile      string        `long:"cert"                    env:"KAFKA_TLS_CERT"                    description:"Client certificate PEM file (mTLS)"`
	LogLevel      string        `long:"log-level"               env:"K4A_INDEX_LOG_LEVEL"               description:"Log level: debug, info, warn, error"                                                                                                                                                                                                                                                                                                    default:"info"`
	TopicMaxBytes string        `long:"topic-max-bytes"         env:"K4A_INDEX_TOPIC_MAX_BYTES"         description:"Per-topic index byte cap; the oldest records are trimmed once a topic's index exceeds it. Human size (e.g. 1GiB, 512MB); 0 disables. Total disk ≈ hot-set × this, so keep it under the volume sizeLimit."                                                                                                                               default:"1GiB"`
	MaxTotalBytes string        `long:"max-total-bytes"         env:"K4A_INDEX_MAX_TOTAL_BYTES"         description:"Disk budget for the WHOLE index root, measured on disk. Over budget the daemon deletes orphaned index dirs, then evicts the coldest non-pinned topics until it fits — turning an abrupt volume-full pod eviction into a graceful loss of the coldest coverage. Set it below the volume's size limit. Human size; 0 disables."           default:"0"`
	Prefollow     []string      `long:"topic"                   env:"K4A_INDEX_TOPIC"                   description:"Topics to pre-follow (warm + pinned) at startup; each must match --allow. Repeatable / comma-separated."                                                                                                                                                                                                                      short:"t"                     env-delim:","`
	Allow         []string      `long:"allow"                   env:"K4A_INDEX_ALLOW"                   description:"Follow allowlist: glob patterns of topics the daemon may auto-follow (default-deny; MUST mirror the cluster-side topic grants (IAM policy or ACLs)). Repeatable / comma-separated."                                                                                                                                                                         env-delim:"," required:"true"`
	MaxFollow     int           `long:"max-followed"            env:"K4A_INDEX_MAX_FOLLOWED"            description:"Hard cap on concurrently-followed topics; at the cap a new follow evicts the coldest."                                                                                                                                                                                                                                                  default:"256"`
	EvictAfter    time.Duration `long:"evict-after"             env:"K4A_INDEX_EVICT_AFTER"             description:"Drop a topic not searched within this idle window (LRU). Pinned pre-follow topics are exempt."                                                                                                                                                                                                                                          default:"168h"`
	DrainStall    time.Duration `long:"drain-stall-timeout"     env:"K4A_INDEX_DRAIN_STALL_TIMEOUT"     description:"Fail fast if a topic's indexing drain makes no progress for this long while records are queued: dump all goroutine stacks to stderr and exit, so the pod restarts and re-warms instead of serving nothing silently. 0 disables."                                                                                                        default:"5m"`
	MergeGrace    time.Duration `long:"drain-stall-merge-grace" env:"K4A_INDEX_DRAIN_STALL_MERGE_GRACE" description:"Tolerate a drain stall up to this long while a scorch file merge is in flight: the merger is working, and killing it restarts the merge from scratch on the same volume — a restart livelock. 0 disables the grace."                                                                                                                    default:"30m"`
	WipeStrikes   int           `long:"drain-stall-wipe-after"  env:"K4A_INDEX_DRAIN_STALL_WIPE_AFTER"  description:"On the Nth consecutive drain-stall exit for the same topic index (counted in the index dir, so the count survives container restarts), quarantine the index before exiting so the next boot re-warms fresh. 0 disables."                                                                                                                default:"3"`
	Insecure      bool          `long:"insecure"                                                        description:"Skip TLS certificate verification"                                                                                                                                                                                                                                                                                            short:"k"`
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

	// The allowlist is the v1 authorization boundary — build it first so a
	// malformed pattern (or an empty list) fails fast before we touch Kafka.
	policy, err := newAllowPolicy(f.Allow)
	if err != nil {
		log.Error("allowlist", "err", err)
		return 1
	}
	if policy.empty() {
		log.Error("allowlist is empty — the daemon would follow no topics; set --allow / K4A_INDEX_ALLOW")
		return 1
	}
	// Pre-follow topics are operator-pinned but still subject to the allowlist:
	// a pinned topic outside the allowlist is a config contradiction, so reject
	// it here rather than silently never following it.
	for _, t := range f.Prefollow {
		if !policy.allowed(t) {
			log.Error("pre-follow topic is not in the allowlist", "topic", t, "allow", policy.String())
			return 1
		}
	}

	topicMaxBytes, err := parseByteSize(f.TopicMaxBytes)
	if err != nil {
		log.Error("topic-max-bytes", "err", err)
		return 1
	}

	maxTotalBytes, err := parseByteSize(f.MaxTotalBytes)
	if err != nil {
		log.Error("max-total-bytes", "err", err)
		return 1
	}
	// A budget below one topic's cap would evict every topic on every sweep and
	// still never fit. Refuse it rather than thrash. Note the on-disk footprint
	// runs well above the stored-bytes cap, so this is a floor, not a target.
	if maxTotalBytes > 0 && maxTotalBytes < topicMaxBytes {
		log.Error("max-total-bytes is below topic-max-bytes — every topic would be evicted and it still would not fit",
			"max_total_bytes", maxTotalBytes, "topic_max_bytes", topicMaxBytes)
		return 1
	}

	log.Info(
		"k4a-index starting",
		"version", version.String(),
		"allow", policy.String(),
		"prefollow", f.Prefollow,
		"max_followed", f.MaxFollow,
		"topic_max_bytes", topicMaxBytes,
		"max_total_bytes", maxTotalBytes,
		"evict_after", f.EvictAfter,
		"drain_stall_timeout", f.DrainStall,
		"drain_stall_merge_grace", f.MergeGrace,
		"drain_stall_wipe_after", f.WipeStrikes,
		"listen", f.Listen,
		"data_dir", f.dataRoot(),
		"auth", f.Auth,
		"log_level", f.LogLevel,
	)

	brokers, authCfg, err := resolveConnection(&f)
	if err != nil {
		log.Error("connection", "err", err)
		return 1
	}
	log.Info("resolved connection", "brokers", brokers, "auth", authCfg.Method)

	client, err := kafka.NewClient(authCfg, brokers)
	if err != nil {
		log.Error("kafka client", "err", err)
		return 1
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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
	mgr := newManager(ctx, root, cluster, client, policy, managerConfig{
		MaxFollowed:   f.MaxFollow,
		EvictAfter:    f.EvictAfter,
		MaxTotalBytes: maxTotalBytes,
		Worker: workerOptions{
			TrimBytes:   topicMaxBytes,
			StallAfter:  f.DrainStall,
			MergeGrace:  f.MergeGrace,
			WipeStrikes: f.WipeStrikes,
		},
	})
	defer mgr.stopAll()

	// Warm the pinned pre-follow set before serving, then start the LRU sweeper.
	// Everything else follows on demand, gated by the allowlist.
	mgr.preFollow(f.Prefollow)
	go mgr.sweep()
	log.Info("follow-set ready", "cluster", cluster, "prefollowed", mgr.size())

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(unaryLogInterceptor),
		grpc.StreamInterceptor(streamLogInterceptor),
	)
	indexv1.RegisterIndexServer(srv, newIndexServer(mgr))

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", f.Listen)
	if err != nil {
		log.Error("listen", "addr", f.Listen, "err", err)
		return 1
	}

	// Stop the gRPC server when the process is signaled.
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		srv.GracefulStop()
	}()

	log.Info("k4a-index serving", "addr", f.Listen, "version", version.String())
	if err := srv.Serve(lis); err != nil && ctx.Err() == nil {
		log.Error("serve", "err", err)
		return 1
	}
	return 0
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
	trim := func(suf string) bool {
		rest, ok := strings.CutSuffix(u, suf)
		if ok {
			u = rest
		}
		return ok
	}
	switch {
	case trim("TIB") || trim("TI"):
		mult = 1 << 40
	case trim("GIB") || trim("GI"):
		mult = 1 << 30
	case trim("MIB") || trim("MI"):
		mult = 1 << 20
	case trim("KIB") || trim("KI"):
		mult = 1 << 10
	case trim("TB"):
		mult = 1_000_000_000_000
	case trim("GB"):
		mult = 1_000_000_000
	case trim("MB"):
		mult = 1_000_000
	case trim("KB"):
		mult = 1_000
	case trim("B"):
		mult = 1
	}
	n, err := strconv.ParseInt(strings.TrimSpace(u), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte size %q must be non-negative", s)
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
	if f.Brokers != "" {
		auth := f.Auth
		if auth == "" {
			auth = "scram"
		}
		return f.Brokers, kafka.AuthConfig{
			Method:   kafka.AuthMethod(auth),
			Username: f.Username,
			Password: f.Password,
			CertFile: f.CertFile,
			KeyFile:  f.KeyFile,
			CAFile:   f.CAFile,
			Region:   f.Region,
			Profile:  f.Profile,
			Insecure: f.Insecure,
		}, nil
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
	authCfg := cctx.ToAuthConfig()
	if f.Auth != "" {
		authCfg.Method = kafka.AuthMethod(f.Auth)
	}
	if f.Profile != "" {
		authCfg.Profile = f.Profile
	}
	if f.Region != "" && f.Region != "us-east-1" {
		authCfg.Region = f.Region
	}
	return brokers, authCfg, nil
}
