// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/smithy-go"
	"github.com/twmb/franz-go/pkg/kerr"
)

// credentialErrorCodes are AWS API error codes that indicate expired or invalid credentials.
var credentialErrorCodes = map[string]bool{
	"ExpiredToken":                true,
	"ExpiredTokenException":       true,
	"RequestExpired":              true,
	"InvalidIdentityToken":        true,
	"AccessDeniedException":       true,
	"UnrecognizedClientException": true,
	"InvalidClientTokenId":        true,
}

// credentialErrorSubstrings are patterns found in wrapped credential
// errors. Covers explicit token expiry, missing or exhausted credential
// chains, AWS SSO session lapses, and credential-process helpers that
// fail (for example, when the SSO login has not been run yet).
var credentialErrorSubstrings = []string{
	"expired token",
	"security token",
	"no credential",
	"nocredentialproviders",
	"credential process", // SDK formats credential_process failures with this phrase
	"credential-process", // hyphenated variant some providers use
	"process provider",   // alternate phrasing from the SDK's process provider
	"sso session",        // expired SSO session error
	"ssooidc",            // common substring in SSO OIDC failures
	"failed to refresh",  // SDK's generic refresh-cached-creds wrapper
	"failed to retrieve", // SDK's generic retrieve wrapper for chain failures
	"web identity",       // web identity token issues
	"aws credentials",    // our own wrapper in iamMechanism (auth.go) — catches anything that fell through the patterns above
}

// IsAuthError reports whether the error indicates an authentication failure.
// It checks Kafka SASL failures, AWS SDK credential errors, and common
// error message patterns. The "AWS credentials" prefix produced by the
// IAM mechanism in this package always matches via the substring path.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	// Check AWS SDK typed errors via smithy.APIError interface.
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok {
		if credentialErrorCodes[apiErr.ErrorCode()] {
			return true
		}
	}

	// Check Kafka SASL authentication failure (error code 58).
	if errors.Is(err, kerr.SaslAuthenticationFailed) {
		return true
	}

	// Fallback: check error string for common credential error patterns.
	msg := strings.ToLower(err.Error())
	for _, sub := range credentialErrorSubstrings {
		if strings.Contains(msg, strings.ToLower(sub)) {
			return true
		}
	}

	return false
}

// connectionErrorSubstrings are patterns that indicate a network/connection failure.
var connectionErrorSubstrings = []string{
	"no such host",
	"connection refused",
	"connection reset",
	"i/o timeout",
	"network is unreachable",
}

// isConnectionError reports whether the error indicates a network/connection failure.
func isConnectionError(err error) bool {
	msg := strings.ToLower(err.Error())
	for _, sub := range connectionErrorSubstrings {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

// FormatAuthError returns a user-friendly error message for authentication failures.
func FormatAuthError(err error) error {
	return FormatUserError(err)
}

// FormatUserError returns a user-friendly error with actionable guidance.
// It prepends a short diagnosis while keeping the original error visible.
func FormatUserError(err error) error {
	if err == nil {
		return nil
	}

	if IsAuthError(err) {
		return fmt.Errorf("authentication failed — refresh your credentials, then run :reconnect\n  %w", err)
	}

	if isConnectionError(err) {
		return fmt.Errorf("connection failed — if using temporary credentials, refresh them and run :reconnect\n  %w", err)
	}

	return err
}
