// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package awscreds

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// isolateAWS points every file the SDK reads at t.TempDir and clears the
// environment variables that would otherwise win over them, so a test never
// sees the developer's real ~/.aws or ambient credentials.
func isolateAWS(t *testing.T, config string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config")
	credPath := filepath.Join(dir, "credentials")
	if err := os.WriteFile(cfgPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(credPath, []byte(testCredentials), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credPath)
	for _, k := range []string{
		"AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

const testConfig = `[default]
region = ap-south-1

[profile analytics]
region = eu-west-1
`

const testCredentials = `[default]
aws_access_key_id = AKIDDEFAULT
aws_secret_access_key = default-secret

[analytics]
aws_access_key_id = AKIDANALYTICS
aws_secret_access_key = analytics-secret
`

// A named profile's credentials come from the shared credentials file, not
// from the default profile.
func TestLoadNamedProfileCredentials(t *testing.T) {
	isolateAWS(t, testConfig)

	cfg, err := Load(context.Background(), "analytics", "eu-central-1")
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if creds.AccessKeyID != "AKIDANALYTICS" {
		t.Errorf("AccessKeyID = %q, want AKIDANALYTICS (the analytics profile)", creds.AccessKeyID)
	}
	if cfg.Region != "eu-central-1" {
		t.Errorf("Region = %q, want the caller's eu-central-1", cfg.Region)
	}
}

// An empty profile means the SDK default chain, which reads [default].
func TestLoadEmptyProfileUsesDefault(t *testing.T) {
	isolateAWS(t, testConfig)

	cfg, err := Load(context.Background(), "", "us-west-2")
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if creds.AccessKeyID != "AKIDDEFAULT" {
		t.Errorf("AccessKeyID = %q, want AKIDDEFAULT", creds.AccessKeyID)
	}
}

// A profile that exists in neither file is an error that names the profile,
// not a silent fall back to [default].
func TestLoadMissingProfileFails(t *testing.T) {
	isolateAWS(t, testConfig)

	_, err := Load(context.Background(), "no-such-profile", "")
	if err == nil {
		t.Fatal("Load of a missing profile succeeded")
	}
	if !strings.Contains(err.Error(), `"no-such-profile"`) {
		t.Errorf("error %q does not name the profile", err)
	}
}

// With no region from the caller, Load uses DefaultRegion.
func TestLoadEmptyRegionDefaults(t *testing.T) {
	isolateAWS(t, "")

	cfg, err := Load(context.Background(), "analytics", "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Region != DefaultRegion {
		t.Errorf("Region = %q, want %q", cfg.Region, DefaultRegion)
	}
}

func TestLoginHint(t *testing.T) {
	t.Parallel()

	base := errors.New("token expired")

	named := LoginHint(base, "prod-admin")
	if !errors.Is(named, base) {
		t.Error("LoginHint with a profile does not wrap the cause")
	}
	for _, want := range []string{"granted sso login prod-admin", "aws sso login --profile prod-admin", "token expired"} {
		if !strings.Contains(named.Error(), want) {
			t.Errorf("hint %q is missing %q", named, want)
		}
	}

	bare := LoginHint(base, "")
	if !errors.Is(bare, base) {
		t.Error("LoginHint without a profile does not wrap the cause")
	}
	if !strings.Contains(bare.Error(), "run `aws sso login`") || strings.Contains(bare.Error(), "--profile") {
		t.Errorf("hint without a profile = %q, want a bare `aws sso login`", bare)
	}
}
