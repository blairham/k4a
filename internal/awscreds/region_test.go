// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package awscreds_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/awscreds"
)

// regionConfig names a region only in the analytics profile, so a region that
// comes back as eu-west-1 can only have come from the profile.
const regionConfig = `[default]
aws_access_key_id = AKIADEFAULT
aws_secret_access_key = default-secret

[profile analytics]
region = eu-west-1
aws_access_key_id = AKIAANALYTICS
aws_secret_access_key = analytics-secret

[profile noregion]
aws_access_key_id = AKIANOREGION
aws_secret_access_key = noregion-secret
`

// isolateRegion points the SDK at a config file in t.TempDir and clears every
// other AWS_* variable, so neither the real ~/.aws nor the shell's region can
// leak into the result.
func isolateRegion(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "AWS_") {
			t.Setenv(k, "") // registers the restore
			os.Unsetenv(k)
		}
	}
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config")
	if err := os.WriteFile(cfgPath, []byte(regionConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	credsPath := filepath.Join(dir, "credentials")
	if err := os.WriteFile(credsPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", cfgPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credsPath)
	t.Setenv("HOME", dir)
}

// TestLoadRegionPrecedence pins #102: explicit region > AWS_REGION /
// AWS_DEFAULT_REGION > the profile's region > DefaultRegion. Load used to
// pass DefaultRegion to the SDK as an override, so the profile's region was
// never read.
func TestLoadRegionPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile string
		region  string
		env     map[string]string
		want    string
	}{
		{
			name: "explicit beats env and profile", profile: "analytics", region: "eu-central-1",
			env: map[string]string{"AWS_REGION": "ap-south-1"}, want: "eu-central-1",
		},
		{
			name: "AWS_REGION beats profile", profile: "analytics",
			env: map[string]string{"AWS_REGION": "ap-south-1"}, want: "ap-south-1",
		},
		{
			name: "AWS_DEFAULT_REGION beats profile", profile: "analytics",
			env: map[string]string{"AWS_DEFAULT_REGION": "sa-east-1"}, want: "sa-east-1",
		},
		{name: "profile region", profile: "analytics", want: "eu-west-1"},
		{name: "AWS_PROFILE region", env: map[string]string{"AWS_PROFILE": "analytics"}, want: "eu-west-1"},
		{name: "fallback when profile has none", profile: "noregion", want: awscreds.DefaultRegion},
		{name: "fallback with default chain", want: awscreds.DefaultRegion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateRegion(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := awscreds.Load(context.Background(), tc.profile, tc.region)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Region != tc.want {
				t.Errorf("Load(%q, %q) region = %q, want %q", tc.profile, tc.region, cfg.Region, tc.want)
			}
		})
	}
}
