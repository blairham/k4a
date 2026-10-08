// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestIsAuthError_Nil(t *testing.T) {
	if IsAuthError(nil) {
		t.Error("expected false for nil error")
	}
}

func TestIsAuthError_SmithyAPIError(t *testing.T) {
	for _, code := range []string{
		"ExpiredToken",
		"ExpiredTokenException",
		"RequestExpired",
		"InvalidIdentityToken",
		"AccessDeniedException",
		"UnrecognizedClientException",
		"InvalidClientTokenId",
	} {
		err := &smithy.GenericAPIError{Code: code, Message: "test"}
		if !IsAuthError(err) {
			t.Errorf("expected true for API error code %q", code)
		}
	}
}

func TestIsAuthError_WrappedSmithyAPIError(t *testing.T) {
	inner := &smithy.GenericAPIError{Code: "ExpiredTokenException", Message: "token expired"}
	wrapped := fmt.Errorf("fetching metadata: %w", inner)
	if !IsAuthError(wrapped) {
		t.Error("expected true for wrapped smithy API error")
	}
}

func TestIsAuthError_SASLAuthFailed(t *testing.T) {
	if !IsAuthError(kerr.SaslAuthenticationFailed) {
		t.Error("expected true for SaslAuthenticationFailed")
	}
}

func TestIsAuthError_WrappedSASLAuthFailed(t *testing.T) {
	wrapped := fmt.Errorf("connection failed: %w", kerr.SaslAuthenticationFailed)
	if !IsAuthError(wrapped) {
		t.Error("expected true for wrapped SaslAuthenticationFailed")
	}
}

func TestIsAuthError_StringPatterns(t *testing.T) {
	for _, msg := range []string{
		"expired token: session has expired",
		"no security token found in request",
		"no credential providers found",
		"NoCredentialProviders: no valid providers in chain",
		// Real-world failures #19 was about — credential_process backends
		// like `granted credential-process` returning non-zero, SSO
		// session lapses, generic chain-refresh failures.
		"failed to refresh cached credentials, process provider error: exit status 1",
		"credential process failed: SSO session has expired",
		"failed to retrieve credentials from any provider",
		"web identity token file not found",
		"AWS credentials unavailable: process provider error",
	} {
		err := errors.New(msg)
		if !IsAuthError(err) {
			t.Errorf("expected true for error message %q", msg)
		}
	}
}

func TestIsAuthError_NonCredentialErrors(t *testing.T) {
	for _, err := range []error{
		errors.New("connection refused"),
		errors.New("timeout waiting for response"),
		errors.New("topic not found"),
		fmt.Errorf("fetching metadata: %w", errors.New("i/o timeout")),
	} {
		if IsAuthError(err) {
			t.Errorf("expected false for error %q", err)
		}
	}
}

func TestIsAuthError_UnrelatedSmithyAPIError(t *testing.T) {
	err := &smithy.GenericAPIError{Code: "ThrottlingException", Message: "rate exceeded"}
	if IsAuthError(err) {
		t.Error("expected false for ThrottlingException")
	}
}

func TestFormatUserError_AuthError(t *testing.T) {
	original := errors.New("request failed: expired token")
	formatted := FormatUserError(original)
	if formatted == nil {
		t.Fatal("expected non-nil error")
	}
	msg := formatted.Error()
	if !contains(msg, "authentication failed") {
		t.Errorf("expected auth guidance, got %q", msg)
	}
	// Original error should be preserved.
	if !contains(msg, "expired token") {
		t.Errorf("expected original error text, got %q", msg)
	}
}

func TestFormatUserError_ConnectionError(t *testing.T) {
	original := errors.New("dial tcp: lookup broker.example.com: no such host")
	formatted := FormatUserError(original)
	if formatted == nil {
		t.Fatal("expected non-nil error")
	}
	msg := formatted.Error()
	if !contains(msg, "connection failed") {
		t.Errorf("expected connection guidance, got %q", msg)
	}
	// Original error should be preserved.
	if !contains(msg, "no such host") {
		t.Errorf("expected original error text, got %q", msg)
	}
}

func TestFormatUserError_UnknownError(t *testing.T) {
	original := errors.New("some random internal error")
	formatted := FormatUserError(original)
	if !errors.Is(formatted, original) {
		t.Errorf("unknown errors should pass through unchanged, got %q", formatted)
	}
}

func TestFormatUserError_Nil(t *testing.T) {
	if FormatUserError(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

// ---------------------------------------------------------------------------
// sendErr
// ---------------------------------------------------------------------------

func TestSendErrDeliversError(t *testing.T) {
	ch := make(chan error, 1)
	err := errors.New("auth failed")
	sendErr(ch, err)

	select {
	case got := <-ch:
		if !errors.Is(got, err) {
			t.Errorf("got %v, want %v", got, err)
		}
	default:
		t.Error("expected error on channel")
	}
}

func TestSendErrNonBlocking(t *testing.T) {
	// Channel already has an error — sendErr should not block.
	ch := make(chan error, 1)
	ch <- errors.New("first")

	sendErr(ch, errors.New("second"))

	// Only the first error should be in the channel.
	got := <-ch
	if got.Error() != "first" {
		t.Errorf("expected first error, got %q", got.Error())
	}

	// Channel should be empty now.
	select {
	case extra := <-ch:
		t.Errorf("expected empty channel, got %v", extra)
	default:
		// OK
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
