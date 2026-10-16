// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package endpointauth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

// Action is what a caller asks to do to a topic.
type Action int

const (
	// ActionSearch reads what the daemon holds for a topic: a Search, and a
	// Coverage probe (which reveals whether and how far the topic is indexed).
	ActionSearch Action = iota
	// ActionFollow makes the daemon start tailing a topic: an explicit Follow,
	// and the warm-on-first-search follow a Search of an unfollowed topic
	// starts as a side effect.
	ActionFollow
)

func (a Action) String() string {
	if a == ActionFollow {
		return "follow"
	}
	return "search"
}

// Policy is per-caller authorization (Phase 5 of
// docs/design/shared-index-service.md): it maps a caller identity — a verified
// mTLS client certificate or a bearer token — to the topic globs that caller
// may search and follow.
//
// Once a policy is configured it is default-deny: a caller that matches no
// entry is refused outright, and a matched caller reaches only the topics its
// entries grant. A nil *Policy is "no policy", which keeps the behavior from
// before authorization existed — every authenticated caller reaches every
// topic the daemon's --allow list admits.
type Policy struct {
	callers []policyCaller
}

type policyCaller struct {
	name    string
	cn      string
	san     string
	subject string
	token   []byte // sha256 of the bearer token; nil for a certificate entry
	search  []string
	follow  []string
}

// policyFile is the on-disk shape. Unknown fields are refused, so a typo in a
// grant ("serach") fails at startup rather than silently granting nothing.
type policyFile struct {
	Callers []callerEntry `yaml:"callers"`
}

type callerEntry struct {
	Cert *struct {
		CN      string `yaml:"cn"`
		SAN     string `yaml:"san"`
		Subject string `yaml:"subject"`
	} `yaml:"cert"`
	Name        string   `yaml:"name"`
	TokenSHA256 string   `yaml:"tokenSHA256"`
	Search      []string `yaml:"search"`
	Follow      []string `yaml:"follow"`
}

// LoadPolicy reads a policy file. An empty path is "no policy" (nil, nil).
func LoadPolicy(file string) (*Policy, error) {
	if file == "" {
		return nil, nil //nolint:nilnil // nil policy is "authorization off"
	}
	b, err := os.ReadFile(file) //nolint:gosec // operator-supplied path
	if err != nil {
		return nil, fmt.Errorf("authorization policy: %w", err)
	}
	p, err := ParsePolicy(b)
	if err != nil {
		return nil, fmt.Errorf("authorization policy %s: %w", file, err)
	}
	return p, nil
}

// ParsePolicy parses and validates a policy document.
func ParsePolicy(b []byte) (*Policy, error) {
	var f policyFile
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}
	if len(f.Callers) == 0 {
		return nil, errors.New("no callers: a policy with no callers refuses everyone; " +
			"drop the policy to turn authorization off")
	}
	p := &Policy{}
	names := map[string]bool{}
	for i, c := range f.Callers {
		pc, err := parseCaller(i, c)
		if err != nil {
			return nil, err
		}
		if names[pc.name] {
			return nil, fmt.Errorf("caller %q is listed twice", pc.name)
		}
		names[pc.name] = true
		p.callers = append(p.callers, pc)
	}
	return p, nil
}

// parseCaller validates one entry: a name, exactly one identity, and legal
// globs.
func parseCaller(i int, c callerEntry) (policyCaller, error) {
	name := strings.TrimSpace(c.Name)
	if name == "" {
		return policyCaller{}, fmt.Errorf("caller %d has no name", i+1)
	}
	pc := policyCaller{name: name}
	var err error
	switch {
	case c.Cert != nil && c.TokenSHA256 != "":
		return policyCaller{}, fmt.Errorf("caller %q: set cert or tokenSHA256, not both (list the identity "+
			"twice under two names to grant either)", name)
	case c.Cert != nil:
		err = pc.setCert(strings.TrimSpace(c.Cert.CN), strings.TrimSpace(c.Cert.SAN),
			strings.TrimSpace(c.Cert.Subject))
	case c.TokenSHA256 != "":
		h, herr := hex.DecodeString(strings.TrimSpace(c.TokenSHA256))
		if herr != nil || len(h) != sha256.Size {
			err = fmt.Errorf("caller %q: tokenSHA256 must be the 64-hex-digit SHA-256 of the token "+
				"(sha256sum of the token without a trailing newline)", name)
		}
		pc.token = h
	default:
		err = fmt.Errorf("caller %q: needs a cert or a tokenSHA256 identity", name)
	}
	if err != nil {
		return policyCaller{}, err
	}
	if pc.search, err = topicGlobs(name, "search", c.Search); err != nil {
		return policyCaller{}, err
	}
	if pc.follow, err = topicGlobs(name, "follow", c.Follow); err != nil {
		return policyCaller{}, err
	}
	return pc, nil
}

func (c *policyCaller) setCert(cn, san, subject string) error {
	set := 0
	for _, v := range []string{cn, san, subject} {
		if v != "" {
			set++
		}
	}
	if set != 1 {
		return fmt.Errorf("caller %q: cert needs exactly one of cn, san or subject", c.name)
	}
	c.cn, c.san, c.subject = cn, san, subject
	return nil
}

func topicGlobs(caller, field string, in []string) ([]string, error) {
	out := make([]string, 0, len(in))
	for _, g := range in {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		if _, err := path.Match(g, ""); err != nil {
			return nil, fmt.Errorf("caller %q: invalid %s pattern %q: %w", caller, field, g, err)
		}
		out = append(out, g)
	}
	return out, nil
}

// HasCertCallers reports whether any entry names a client certificate, which
// only a listener that verifies client certificates (a client CA) can match.
func (p *Policy) HasCertCallers() bool {
	for _, c := range p.callers {
		if c.token == nil {
			return true
		}
	}
	return false
}

// HasTokenCallers reports whether any entry names a bearer token.
func (p *Policy) HasTokenCallers() bool {
	for _, c := range p.callers {
		if c.token != nil {
			return true
		}
	}
	return false
}

// Grant is what one caller may do: the union of every policy entry its
// identity matched.
type Grant struct {
	names  []string
	search []string
	follow []string
}

// Identify returns the grant for a caller presenting the Authorization value
// authz and, when the handshake verified one, the client certificate leaf. It
// returns nil when no entry matches: under a policy such a caller is refused.
func (p *Policy) Identify(authz string, leaf *x509.Certificate) *Grant {
	var presented []byte
	if tok, ok := bearerToken(authz); ok {
		h := sha256.Sum256([]byte(tok))
		presented = h[:]
	}
	var g *Grant
	for _, c := range p.callers {
		var match bool
		if c.token != nil {
			// Every token entry is compared, matched or not, so the time taken
			// says nothing about which entry (if any) a token belongs to.
			match = presented != nil && subtle.ConstantTimeCompare(presented, c.token) == 1
		} else {
			match = leaf != nil && c.matchesCert(leaf)
		}
		if !match {
			continue
		}
		if g == nil {
			g = &Grant{}
		}
		g.names = append(g.names, c.name)
		g.search = append(g.search, c.search...)
		g.follow = append(g.follow, c.follow...)
	}
	return g
}

func (c policyCaller) matchesCert(leaf *x509.Certificate) bool {
	switch {
	case c.cn != "":
		return leaf.Subject.CommonName == c.cn
	case c.subject != "":
		return leaf.Subject.String() == c.subject
	}
	for _, d := range leaf.DNSNames {
		if d == c.san {
			return true
		}
	}
	for _, e := range leaf.EmailAddresses {
		if e == c.san {
			return true
		}
	}
	for _, u := range leaf.URIs {
		if u.String() == c.san {
			return true
		}
	}
	for _, ip := range leaf.IPAddresses {
		if ip.String() == c.san {
			return true
		}
	}
	return false
}

// Allows reports whether the grant permits action on topic. A nil grant
// permits nothing.
func (g *Grant) Allows(a Action, topic string) bool {
	if g == nil {
		return false
	}
	globs := g.search
	if a == ActionFollow {
		globs = g.follow
	}
	for _, pat := range globs {
		if pat == topic {
			return true
		}
		if ok, _ := path.Match(pat, topic); ok { //nolint:errcheck // validated in ParsePolicy
			return true
		}
	}
	return false
}

// Caller names the matched policy entries, for logs. Never the credential.
func (g *Grant) Caller() string {
	if g == nil {
		return ""
	}
	return strings.Join(g.names, ",")
}

type grantKey struct{}

// WithGrant attaches a caller's grant to ctx; the Guard does this for every
// admitted request when a policy is configured.
func WithGrant(ctx context.Context, g *Grant) context.Context {
	return context.WithValue(ctx, grantKey{}, g)
}

// GrantFromContext returns the grant the Guard attached, or nil.
func GrantFromContext(ctx context.Context) *Grant {
	g, _ := ctx.Value(grantKey{}).(*Grant) //nolint:errcheck // absent is nil
	return g
}

// Authorizer is the per-request check a handler runs before touching a topic.
// The zero value (no policy) admits everything, which is the behavior before
// authorization existed.
type Authorizer struct {
	Policy *Policy
}

// Enabled reports whether a policy is configured.
func (a Authorizer) Enabled() bool { return a.Policy != nil }

// Check returns nil when the caller in ctx may perform action on topic, and
// ErrNotAuthorized otherwise. With a policy configured, a ctx that carries no
// grant is refused: a handler reached without the Guard is not a reason to
// fail open.
func (a Authorizer) Check(ctx context.Context, action Action, topic string) error {
	if a.Policy == nil {
		return nil
	}
	g := GrantFromContext(ctx)
	if g.Allows(action, topic) {
		return nil
	}
	return &NotAuthorizedError{Caller: g.Caller(), Action: action, Topic: topic}
}

// Allows is Check as a bool.
func (a Authorizer) Allows(ctx context.Context, action Action, topic string) bool {
	return a.Check(ctx, action, topic) == nil
}

// NotAuthorizedError is a policy refusal. It names the caller's policy entry,
// never its credential.
type NotAuthorizedError struct {
	Caller string
	Topic  string
	Action Action
}

func (e *NotAuthorizedError) Error() string {
	who := e.Caller
	if who == "" {
		who = "an unidentified caller"
	} else {
		who = "caller " + who
	}
	return fmt.Sprintf("%s may not %s topic %q (authorization policy)", who, e.Action, e.Topic)
}

// bearerToken extracts the token from an Authorization value, accepting the
// scheme in any case (RFC 6750).
func bearerToken(header string) (string, bool) {
	if len(header) > 7 && strings.EqualFold(header[:7], "bearer ") {
		tok := strings.TrimSpace(header[7:])
		return tok, tok != ""
	}
	return "", false
}
