// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package awscreds resolves AWS configuration for a named shared-config
// profile, in-process, so callers need no `aws-vault exec`/`assume` wrapper.
package awscreds

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

// DefaultRegion is used when neither the caller nor the profile names one.
const DefaultRegion = "us-east-1"

// Load returns an AWS config for profile (empty means the SDK's default
// chain) in region (empty means DefaultRegion). The SDK reads the profile's
// SSO session or credential_process from ~/.aws/config itself.
func Load(ctx context.Context, profile, region string) (aws.Config, error) {
	if region == "" {
		region = DefaultRegion
	}
	opts := []func(*config.LoadOptions) error{config.WithRegion(region)}
	if profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("loading AWS config (profile %q): %w", profile, err)
	}
	return cfg, nil
}

// LoginHint wraps err with the command that renews an expired SSO session.
// The SDK can read a cached SSO token but cannot open the browser itself, so
// naming the command is more useful than "refresh your credentials".
func LoginHint(err error, profile string) error {
	if profile == "" {
		return fmt.Errorf("%w — run `aws sso login` to renew the session", err)
	}
	return fmt.Errorf("%w — run `granted sso login %s` (or `aws sso login --profile %s`) to renew the session",
		err, profile, profile)
}
