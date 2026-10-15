// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

// IsEnvRef reports whether s is, in its entirety, one environment variable
// reference ($NAME or ${NAME}) — the form ExpandRef resolves. Such a value
// names a secret without holding it.
func IsEnvRef(s string) bool { return envRef.MatchString(s) }

// secretFields lists, for one context, each field that holds a credential,
// keyed by the name StripInlineSecrets reports it under.
func (ctx *Context) secretFields() map[string]*string {
	return map[string]*string{
		"password":                 &ctx.Password,
		"oauth-token":              &ctx.OAuth.Token,
		"oauth-client-secret":      &ctx.OAuth.ClientSecret,
		"index-token":              &ctx.IndexToken,
		"schema-registry.password": &ctx.SchemaRegistry.Password,
		"schema-registry.token":    &ctx.SchemaRegistry.Token,
	}
}

// StripInlineSecrets returns the config file data with every inline
// credential removed, and the "context.field" names it removed, sorted. A
// credential written as a whole-value $VAR reference is kept: it names the
// secret rather than holding it.
//
// The result is re-rendered from the parsed Config, not edited in place, so
// keys k4a does not read, YAML anchors and comments are all dropped. Each of
// those can carry a secret the struct never sees (a commented-out old
// password, an anchor under an unknown top-level key), and the only way to
// be sure none survives is to emit nothing k4a did not parse.
func StripInlineSecrets(data []byte) (out []byte, stripped []string, err error) {
	var cfg Config
	if uerr := yaml.Unmarshal(data, &cfg); uerr != nil {
		return nil, nil, fmt.Errorf("parsing config: %w", uerr)
	}
	for name, ctx := range cfg.Contexts {
		for field, p := range ctx.secretFields() {
			if *p == "" || IsEnvRef(*p) {
				continue
			}
			*p = ""
			stripped = append(stripped, name+"."+field)
		}
		cfg.Contexts[name] = ctx
	}
	sort.Strings(stripped)
	out, err = yaml.Marshal(&cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("rendering config: %w", err)
	}
	return out, stripped, nil
}
