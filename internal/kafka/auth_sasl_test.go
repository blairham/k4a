// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/tls"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

// builtTransport builds a (never-dialed) client from cfg and reports the SASL
// mechanism name and whether TLS is configured, read back through kgo's own
// OptValue so the test sees what franz-go will actually do.
func builtTransport(t *testing.T, cfg AuthConfig) (mech string, hasTLS bool) {
	t.Helper()
	opts, err := NewClientOpts(cfg, testBrokers)
	if err != nil {
		t.Fatalf("NewClientOpts(%s): %v", cfg.Method, err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	if mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism); len(mechs) > 0 {
		mech = mechs[0].Name()
	}
	tlsCfg, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config)
	return mech, tlsCfg != nil
}

func TestNewClientOpts_SASLMechanisms(t *testing.T) {
	t.Parallel()

	creds := AuthConfig{Username: "u", Password: "p"}
	with := func(m AuthMethod, noTLS bool) AuthConfig {
		c := creds
		c.Method, c.NoTLS = m, noTLS
		if m == AuthOAuthBearer {
			c.OAuth = OAuthConfig{Token: "tok"}
		}
		return c
	}
	for _, tc := range []struct {
		mech    string
		cfg     AuthConfig
		wantTLS bool
	}{
		{"SCRAM-SHA-512", with(AuthSCRAM, false), true},
		{"SCRAM-SHA-512", with(AuthSCRAMSHA512, false), true},
		{"SCRAM-SHA-256", with(AuthSCRAMSHA256, false), true},
		{"PLAIN", with(AuthPlain, false), true},
		{"OAUTHBEARER", with(AuthOAuthBearer, false), true},
		{"SCRAM-SHA-512", with(AuthSCRAM, true), false},
		{"SCRAM-SHA-256", with(AuthSCRAMSHA256, true), false},
		{"PLAIN", with(AuthPlain, true), false},
		{"OAUTHBEARER", with(AuthOAuthBearer, true), false},
		{"", AuthConfig{Method: AuthTLS}, true},
		{"", AuthConfig{Method: AuthPlaintext}, false},
		// --no-tls on plaintext changes nothing: there was no TLS to drop.
		{"", AuthConfig{Method: AuthPlaintext, NoTLS: true}, false},
	} {
		mech, hasTLS := builtTransport(t, tc.cfg)
		if mech != tc.mech || hasTLS != tc.wantTLS {
			t.Errorf("%s no-tls=%v: got mech %q tls=%v, want %q tls=%v",
				tc.cfg.Method, tc.cfg.NoTLS, mech, hasTLS, tc.mech, tc.wantTLS)
		}
	}
}

// --no-tls must be refused, not ignored, where TLS is the whole point of the
// method: silently dialing TLS anyway would hide a misconfiguration, and
// dialing without it would downgrade a method the user chose for its TLS.
func TestNewClientOpts_NoTLSRejectedForTLSMethods(t *testing.T) {
	t.Parallel()

	for _, m := range []AuthMethod{AuthTLS, AuthMTLS, AuthIAM} {
		_, err := NewClientOpts(AuthConfig{Method: m, NoTLS: true}, testBrokers)
		if err == nil || !strings.Contains(err.Error(), "no-tls") {
			t.Errorf("%s with NoTLS: err = %v, want a no-tls refusal", m, err)
		}
	}
}

func TestNewClientOpts_SASLMissingCredentials(t *testing.T) {
	t.Parallel()

	for _, cfg := range []AuthConfig{
		{Method: AuthPlain},
		{Method: AuthPlain, Username: "u"},
		{Method: AuthSCRAMSHA256, Password: "p"},
		{Method: AuthSCRAMSHA512},
		{Method: AuthOAuthBearer},
		{Method: AuthPlain, NoTLS: true},
	} {
		if _, err := NewClientOpts(cfg, testBrokers); err == nil {
			t.Errorf("%s %+v: expected a missing-credentials error", cfg.Method, cfg)
		}
	}
}

func TestCleartextWarning(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		want string // substring; "" means no warning
		cfg  AuthConfig
	}{
		{"password are sent in cleartext", AuthConfig{Method: AuthPlain, NoTLS: true}},
		{"guessed offline", AuthConfig{Method: AuthSCRAM, NoTLS: true}},
		{"guessed offline", AuthConfig{Method: AuthSCRAMSHA256, NoTLS: true}},
		{"bearer token is sent in cleartext", AuthConfig{Method: AuthOAuthBearer, NoTLS: true}},
		{"", AuthConfig{Method: AuthPlain}},
		{"", AuthConfig{Method: AuthPlaintext, NoTLS: true}},
		{"", AuthConfig{Method: AuthIAM, NoTLS: true}},
	} {
		got := CleartextWarning(tc.cfg)
		if tc.want == "" {
			if got != "" {
				t.Errorf("%s no-tls=%v: unexpected warning %q", tc.cfg.Method, tc.cfg.NoTLS, got)
			}
			continue
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s no-tls=%v: warning %q, want it to contain %q", tc.cfg.Method, tc.cfg.NoTLS, got, tc.want)
		}
	}
}
