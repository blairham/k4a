// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"path"
	"strings"
)

// allowPolicy is the v1 authorization boundary for demand-driven follow
// (docs/design/shared-index-service.md §Security). Auto-follow persists
// decrypted message payloads to the daemon's disk and makes them searchable to
// anyone who can reach it, so registration is DEFAULT-DENY: a topic is followed
// only if it matches an explicit allow pattern. The allowlist is meant to mirror
// the topic grants of the daemon's own Kafka principal — the daemon can only
// ever tail what that principal can read, so a pattern broader than the grant
// simply fails at consume; the allowlist is what keeps a sensitive topic from
// being followed even when the principal *could* read it.
//
// Patterns are shell-glob (path.Match): "*" matches any run of characters.
// Kafka topic names carry no "/" (path.Match's only separator), so a pattern
// like "example.ingest.*" matches every dotted suffix. An exact topic name is
// a pattern that matches only itself.
type allowPolicy struct {
	patterns []string
}

// newAllowPolicy trims/dedupes the patterns and validates each is a legal glob,
// so a malformed allowlist fails fast at startup rather than silently matching
// nothing (which would look like "the daemon follows no topics" in production).
// An empty policy denies everything — safe, but useless; callers require at
// least one pattern.
func newAllowPolicy(patterns []string) (allowPolicy, error) {
	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, err := path.Match(p, ""); err != nil {
			return allowPolicy{}, fmt.Errorf("invalid allow pattern %q: %w", p, err)
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return allowPolicy{patterns: out}, nil
}

// allowed reports whether the topic may be followed. Default-deny: an empty
// policy, or a topic matching no pattern, returns false.
func (p allowPolicy) allowed(topic string) bool {
	for _, pat := range p.patterns {
		if pat == topic {
			return true
		}
		if ok, _ := path.Match(pat, topic); ok { //nolint:errcheck // patterns validated in newAllowPolicy
			return true
		}
	}
	return false
}

// String renders the allowlist for startup logging.
func (p allowPolicy) String() string { return strings.Join(p.patterns, ",") }

// empty reports whether the policy has no patterns (would deny everything).
func (p allowPolicy) empty() bool { return len(p.patterns) == 0 }
