// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
)

// LoginFlags defines flags for the login command.
type LoginFlags struct {
	ConnectionFlags
	Force  bool `long:"force"  description:"Log in again even when a cached token is still usable"`
	Logout bool `long:"logout" description:"Forget the cached token instead of logging in"`
}

// LoginCommand implements the "login" CLI command.
type LoginCommand struct {
	// Out receives the login instructions; nil means stderr.
	Out io.Writer
	// OpenBrowser overrides the system browser opener (tests).
	OpenBrowser func(string) error
}

// Help returns the detailed help text.
func (c *LoginCommand) Help() string {
	return `Usage: k4a login [options]

  Log in to an OAUTHBEARER context's OIDC identity provider and cache the
  token, without connecting to Kafka. Applies to --oauth-issuer with the
  device flow (a code to enter on any device; the default) or the browser
  flow (authorization code with PKCE and a loopback redirect).

  Every other command logs in by itself when it has to; this is for logging
  in ahead of time, after a handshake reports that a login is required (for
  example on a context switch inside the TUI), or to switch accounts.

  The token and its refresh token are cached owner-only under the user
  config directory, not ~/.k4a, and refreshed as they expire.

` + clusterFlagsHelp + `
      --oauth-issuer=URL   OIDC issuer (discovery at /.well-known/openid-configuration)
      --oauth-client-id=ID Client ID (a client secret is optional)
      --oauth-flow=FLOW    device (default) or browser
      --oauth-redirect-port=PORT  Fixed loopback port for the browser flow

Options:
      --force              Log in again even when a cached token is usable
      --logout             Forget the cached token

Examples:
  k4a login --context confluent
  k4a login --oauth-issuer https://idp.example.com --oauth-client-id k4a --oauth-flow browser
  k4a login --context confluent --logout`
}

// Synopsis returns the one-line description.
func (c *LoginCommand) Synopsis() string {
	return "Log in to an OIDC identity provider for OAUTHBEARER"
}

// Run executes the login command.
func (c *LoginCommand) Run(args []string) int {
	var flags LoginFlags
	if _, code, done := parseCmdArgs(&flags, args); done {
		return code
	}
	auth, err := flags.loginAuth()
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	if auth.Method != kafka.AuthOAuthBearer || !auth.OAuth.Interactive() {
		log.Error("login applies only to auth oauthbearer with an OIDC issuer (--oauth-issuer)")
		return 1
	}
	out := c.Out
	if out == nil {
		out = os.Stderr
	}
	if flags.Logout {
		if logoutErr := kafka.OAuthLogout(auth.OAuth); logoutErr != nil {
			log.Error(logoutErr.Error())
			return 1
		}
		fmt.Fprintln(out, "Logged out: the cached token is gone.") //nolint:errcheck // best-effort output
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	prompted, err := kafka.OAuthLogin(ctx, auth.OAuth, kafka.LoginOptions{
		Out:         out,
		OpenBrowser: c.OpenBrowser,
		Force:       flags.Force,
	})
	if err != nil {
		log.Error("OIDC login failed", "err", err)
		return 1
	}
	if prompted {
		fmt.Fprintln(out, "Logged in.") //nolint:errcheck // best-effort output
	} else {
		fmt.Fprintln(out, "Already logged in (--force to log in again).") //nolint:errcheck // best-effort output
	}
	return 0
}

// loginAuth resolves the OAuth settings to log in with. Flags that name an
// issuer stand on their own, since a login needs no brokers; otherwise the
// context supplies them, with any flag laid over it as usual.
func (f *LoginFlags) loginAuth() (kafka.AuthConfig, error) {
	if f.Brokers == "" && f.Context == "" && f.OAuth.Issuer != "" {
		auth := f.AuthConfig()
		if auth.Method == "" {
			auth.Method = kafka.AuthOAuthBearer
		}
		return auth, nil
	}
	_, auth, err := f.Resolve()
	return auth, err
}
