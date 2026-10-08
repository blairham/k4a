// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package config handles k4a configuration file loading (kubeconfig-style).
package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/smithy-go"
	"gopkg.in/yaml.v3"

	"github.com/blairham/k4a/internal/awscreds"
	"github.com/blairham/k4a/internal/kafka"
)

// DefaultConfigDir returns the default config directory.
func DefaultConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".k4a")
}

// DefaultConfigPath returns the default config file path.
func DefaultConfigPath() string {
	return filepath.Join(DefaultConfigDir(), "config.yaml")
}

// UI holds display preferences (mirrors k9s ui section).
type UI struct {
	Logoless bool `yaml:"logoless,omitempty"`
	Readonly bool `yaml:"readonly,omitempty"`
}

// Index holds the local-index (Tier 3) preferences. Off by default —
// users opt in per docs/design/local-index.md so first-time users keep
// the current scan-only behavior with zero disk surprise.
type Index struct {
	Enabled bool `yaml:"enabled,omitempty"`
}

// Config is the top-level k4a configuration.
type Config struct {
	Contexts       map[string]Context `yaml:"contexts"`
	CurrentContext string             `yaml:"current-context"`
	UI             UI                 `yaml:"ui,omitempty"`
	Index          Index              `yaml:"index,omitempty"`
}

// Context holds connection settings for a single Kafka cluster.
type Context struct {
	Password      string `yaml:"password,omitempty"`
	SSMBrokers    string `yaml:"ssm-brokers,omitempty"`
	Auth          string `yaml:"auth"`
	Region        string `yaml:"region,omitempty"`
	Profile       string `yaml:"profile,omitempty"`
	Username      string `yaml:"username,omitempty"`
	Brokers       string `yaml:"brokers,omitempty"`
	CertFile      string `yaml:"cert,omitempty"`
	KeyFile       string `yaml:"key,omitempty"`
	CAFile        string `yaml:"ca,omitempty"`
	IndexEndpoint string `yaml:"index-endpoint,omitempty"`
	Insecure      bool   `yaml:"insecure,omitempty"`
	IndexInsecure bool   `yaml:"index-insecure,omitempty"`
}

// Load reads the config from disk. Returns a zero config (no error) if the file doesn't exist.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path) //nolint:gosec // user config file
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	return &cfg, nil
}

// Save writes the config to disk, creating the directory if needed.
func Save(path string, cfg *Config) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	return nil
}

// Resolve's two failures are sentinels because callers need to tell them
// apart. "Nothing is selected" is recoverable inside the TUI — it opens on
// the context picker, which exists precisely to choose one — while "you
// named a context that isn't there" is a bad argument and stays fatal.
// Before these were distinguishable, both collapsed into one error and the
// recoverable case took the fatal path.
var (
	// ErrNoCurrentContext means no context was requested and the config
	// has no current-context set.
	ErrNoCurrentContext = errors.New("no context specified and no current-context set in config")
	// ErrContextNotFound means a context was named but the config has no
	// such entry.
	ErrContextNotFound = errors.New("context not found in config")
)

// Resolve returns the named context (or current-context if name is empty).
func (c *Config) Resolve(name string) (*Context, error) {
	if name == "" {
		name = c.CurrentContext
	}
	if name == "" {
		return nil, ErrNoCurrentContext
	}

	ctx, ok := c.Contexts[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q (available: %s)", ErrContextNotFound, name, c.ContextNames())
	}

	return &ctx, nil
}

// ContextNames returns the configured context names, comma-separated and
// sorted. Sorted because this goes in front of a human trying to spell one
// back at us, and Go's map order would shuffle it on every run.
func (c *Config) ContextNames() string {
	names := make([]string, 0, len(c.Contexts))
	for k := range c.Contexts {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// ResolveBrokers returns the broker string, fetching from SSM if configured.
func (ctx *Context) ResolveBrokers() (string, error) {
	// Direct brokers take priority.
	if ctx.Brokers != "" {
		return os.ExpandEnv(ctx.Brokers), nil
	}

	// Fetch from SSM.
	if ctx.SSMBrokers != "" {
		return ctx.fetchSSMParameter(ctx.SSMBrokers)
	}

	return "", fmt.Errorf("context has no brokers or ssm-brokers configured")
}

func (ctx *Context) fetchSSMParameter(paramName string) (string, error) {
	region := ctx.Region
	if region == "" {
		region = awscreds.DefaultRegion
	}

	// Resolution is in-process (internal/awscreds): the SDK reads the profile's SSO
	// session from ~/.aws/config, invoking granted when it's the profile's
	// credential_process — so `k4a -p <Profile>` needs no `assume --exec` wrapper.
	cfg, err := awscreds.Load(context.Background(), ctx.Profile, region)
	if err != nil {
		return "", fmt.Errorf("loading AWS config for SSM: %w", err)
	}

	client := ssm.NewFromConfig(cfg)
	resp, err := client.GetParameter(context.Background(), &ssm.GetParameterInput{
		Name:           aws.String(paramName),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return "", classifySSMError(paramName, ctx.Profile, err)
	}

	if resp.Parameter == nil || resp.Parameter.Value == nil {
		return "", fmt.Errorf(
			"SSM parameter %q exists but has no value — check the parameter in the AWS console",
			paramName,
		)
	}

	value := strings.TrimSpace(*resp.Parameter.Value)
	if value == "" {
		return "", fmt.Errorf(
			"SSM parameter %q resolved to an empty string — check the parameter in the AWS console",
			paramName,
		)
	}

	return value, nil
}

// classifySSMError translates AWS SDK errors from GetParameter into
// messages that say what's actually wrong, rather than letting users
// chase the failure into a downstream DNS error. The AWS SDK's typed
// errors (ParameterNotFound, AccessDeniedException, etc.) flow through
// smithy.APIError; we match on the code where we can and fall back to
// the wrapped error for anything else.
func classifySSMError(paramName, profile string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "ParameterNotFound":
			return fmt.Errorf(
				"SSM parameter %q not found — check the parameter path in your context config",
				paramName,
			)
		case "AccessDeniedException", "UnauthorizedOperation":
			return fmt.Errorf(
				"access denied reading SSM parameter %q — confirm the IAM profile has ssm:GetParameter on this path",
				paramName,
			)
		case "ExpiredTokenException", "ExpiredToken":
			// Name the renewal command rather than saying "refresh credentials":
			// the SDK reads a cached SSO token but can't open the browser itself.
			return awscreds.LoginHint(fmt.Errorf(
				"AWS credentials expired while reading SSM parameter %q",
				paramName,
			), profile)
		}
	}
	// A lapsed SSO session usually never reaches the API — the SDK fails while
	// resolving the identity, so it arrives as a credential error rather than an
	// ExpiredTokenException. Give it the same renewal hint.
	if kafka.IsAuthError(err) {
		return awscreds.LoginHint(
			fmt.Errorf("could not resolve AWS credentials to read SSM parameter %q: %w", paramName, err),
			profile,
		)
	}
	return fmt.Errorf("fetching SSM parameter %q: %w", paramName, err)
}

// ToAuthConfig converts a context into a kafka.AuthConfig.
func (ctx *Context) ToAuthConfig() kafka.AuthConfig {
	return kafka.AuthConfig{
		Method:   kafka.AuthMethod(ctx.Auth),
		Username: os.ExpandEnv(ctx.Username),
		Password: os.ExpandEnv(ctx.Password),
		CertFile: ctx.CertFile,
		KeyFile:  ctx.KeyFile,
		CAFile:   ctx.CAFile,
		Region:   ctx.Region,
		Profile:  ctx.Profile,
		Insecure: ctx.Insecure,
	}
}
