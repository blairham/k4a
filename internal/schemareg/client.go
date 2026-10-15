// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package schemareg decodes Confluent Schema Registry framed payloads (Avro,
// Protobuf and JSON Schema) to JSON, fetching each schema from the registry
// once and keeping it in memory. Nothing it fetches, and none of its
// credentials, is written to disk or logged.
package schemareg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
)

// Config names a registry and how to authenticate to it. Username/Password
// (HTTP basic) and Token (bearer) are alternatives; setting both is an error
// rather than a guess about which one the registry wants.
type Config struct {
	URL      string
	Username string
	Password string
	Token    string
	CAFile   string
}

// IsSet reports whether a registry is configured at all.
func (c Config) IsSet() bool { return c.URL != "" }

const (
	requestTimeout = 10 * time.Second
	// errorTTL is how long a failed lookup is remembered. Without it a topic
	// of messages that all name a missing schema would send one request per
	// row; with it a registry that was briefly down recovers on its own.
	errorTTL = 30 * time.Second
	// maxEntries bounds the cache against a topic of garbage that happens to
	// start with the magic byte and so names a new "schema ID" per record.
	maxEntries = 4096
)

// Registry resolves schema IDs to decoders, caching each compiled schema for
// the life of the process. It is safe for concurrent use.
type Registry struct {
	cl      *sr.Client
	entries map[int]*entry
	now     func() time.Time
	mu      sync.Mutex
}

// entry is one schema ID's lookup. done closes when sch/err are final, so
// concurrent decoders of the same new ID share one registry request.
type entry struct {
	at   time.Time
	sch  *schema
	err  error
	done chan struct{}
}

// New builds a Registry for cfg. It does not contact the registry; the first
// Decode of a framed payload does.
func New(cfg Config) (*Registry, error) {
	opts, err := clientOpts(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	return &Registry{cl: cl, entries: map[int]*entry{}, now: time.Now}, nil
}

func clientOpts(cfg Config) ([]sr.ClientOpt, error) {
	if err := checkURL(cfg); err != nil {
		return nil, err
	}
	if cfg.Token != "" && (cfg.Username != "" || cfg.Password != "") {
		return nil, errors.New("schema registry: set a bearer token or a username and password, not both")
	}
	tlsCfg, err := tlsConfig(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{
		Timeout:   requestTimeout,
		Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg},
	}
	opts := []sr.ClientOpt{sr.URLs(cfg.URL), sr.HTTPClient(hc), sr.UserAgent("k4a")}
	switch {
	case cfg.Token != "":
		opts = append(opts, sr.BearerToken(cfg.Token))
	case cfg.Username != "" || cfg.Password != "":
		opts = append(opts, sr.BasicAuth(cfg.Username, cfg.Password))
	}
	return opts, nil
}

// checkURL refuses a URL that embeds credentials (they would surface in every
// error that names the URL) and refuses to send credentials over plain http
// except to a loopback registry, the same rule the OAuth token URL follows.
func checkURL(cfg Config) error {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" {
		return errors.New("schema registry URL must be an absolute http(s) URL")
	}
	if u.User != nil {
		return errors.New("schema registry URL must not embed credentials; use the username and password settings")
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		hasCreds := cfg.Token != "" || cfg.Username != "" || cfg.Password != ""
		if !hasCreds || isLoopback(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("schema registry %s must use https to send credentials (http is allowed only for localhost)",
			RedactURL(cfg.URL))
	default:
		return fmt.Errorf("schema registry %s must use http or https", RedactURL(cfg.URL))
	}
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// RedactURL drops userinfo, query and fragment, any of which could hold a
// secret, before a URL is shown to anyone.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(unparseable URL)"
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

// tlsConfig trusts the system roots plus caFile, when given.
func tlsConfig(caFile string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile) //nolint:gosec // a CA path the user configured
	if err != nil {
		return nil, fmt.Errorf("reading schema registry CA file: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("schema registry CA file %s contains no PEM certificates", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// lookup returns the compiled schema for id, fetching and compiling it on
// first use. Waiters give up when ctx ends; the fetch itself runs under its
// own timeout so one impatient caller cannot poison the cache for the rest.
func (r *Registry) lookup(ctx context.Context, id int) (*schema, error) {
	r.mu.Lock()
	e, ok := r.entries[id]
	if ok && !r.stale(e) {
		r.mu.Unlock()
		return wait(ctx, e)
	}
	if len(r.entries) >= maxEntries {
		r.evictOne()
	}
	e = &entry{done: make(chan struct{})}
	r.entries[id] = e
	r.mu.Unlock()

	// WithoutCancel: the fetch outlives this caller's cancellation, since
	// other callers may be waiting on the same entry.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*requestTimeout)
	go func() {
		defer cancel()
		sch, err := r.fetch(fctx, id)
		r.mu.Lock()
		e.sch, e.err, e.at = sch, err, r.now()
		r.mu.Unlock()
		close(e.done)
	}()
	return wait(ctx, e)
}

// stale reports whether a finished failed lookup has outlived errorTTL. A
// "no such schema" answer is final; anything else may be transient. Called
// with r.mu held.
func (r *Registry) stale(e *entry) bool {
	select {
	case <-e.done:
	default:
		return false
	}
	if e.err == nil || errors.Is(e.err, errSchemaNotFound) {
		return false
	}
	return r.now().Sub(e.at) > errorTTL
}

// evictOne drops a finished entry, preferring a failed one. Called with r.mu
// held.
func (r *Registry) evictOne() {
	victim, found := 0, false
	for id, e := range r.entries {
		select {
		case <-e.done:
		default:
			continue
		}
		victim, found = id, true
		if e.err != nil {
			break
		}
	}
	if found {
		delete(r.entries, victim)
	}
}

func wait(ctx context.Context, e *entry) (*schema, error) {
	select {
	case <-e.done:
		return e.sch, e.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

var errSchemaNotFound = errors.New("schema not found in registry")

// fetch reads schema id, its references and the subject/version pairs that
// use it, and compiles it.
func (r *Registry) fetch(ctx context.Context, id int) (*schema, error) {
	s, err := r.cl.SchemaByID(ctx, id)
	if err != nil {
		return nil, registryError(id, err)
	}
	// Subject and version are display metadata; a registry that refuses this
	// call still lets the payload decode.
	versions, err := r.cl.SchemaVersionsByID(ctx, id)
	if err != nil {
		versions = nil
	}
	sch, err := compile(ctx, r, s)
	if err != nil {
		return nil, err
	}
	sch.id = id
	sch.versions = versions
	return sch, nil
}

// registryError turns a client error into one that names the schema ID and
// never echoes a URL with whatever it may hold.
func registryError(id int, err error) error {
	var re *sr.ResponseError
	if errors.As(err, &re) {
		if re.StatusCode == http.StatusNotFound {
			return fmt.Errorf("schema id %d: %w", id, errSchemaNotFound)
		}
		msg := re.Message
		if msg == "" {
			msg = http.StatusText(re.StatusCode)
		}
		return fmt.Errorf("schema id %d: registry returned %d: %s", id, re.StatusCode, msg)
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("schema id %d: registry unreachable: %w", id, ue.Err)
	}
	return fmt.Errorf("schema id %d: %w", id, err)
}

// reference fetches one referenced schema by subject and version.
func (r *Registry) reference(ctx context.Context, ref sr.SchemaReference) (sr.Schema, error) {
	ss, err := r.cl.SchemaByVersion(ctx, ref.Subject, ref.Version)
	if err != nil {
		return sr.Schema{}, fmt.Errorf("reference %s (%s v%d): %w", ref.Name, ref.Subject, ref.Version,
			stripURL(err))
	}
	return ss.Schema, nil
}

func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	var re *sr.ResponseError
	if errors.As(err, &re) {
		return fmt.Errorf("registry returned %d: %s", re.StatusCode, strings.TrimSpace(re.Message))
	}
	return err
}
