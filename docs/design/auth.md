# Design: Authentication

**Status:** living document — describes current behavior
**Code:** `internal/kafka/auth.go`, `internal/kafka/oauth.go`

## Purpose

Build the franz-go client options for one of the authentication methods
Kafka clusters in the wild actually use, and surface configuration via
flags + env + config-file context with sensible precedence.

The auth layer doesn't talk to Kafka. It just builds the franz-go client
options (`NewClientOpts`: seed brokers, `kgo.DialTLSConfig`, `kgo.SASL`);
the rest of the kafka package passes them to `kgo.NewClient`.

## Methods

| Method | TLS | SASL | Required inputs |
|---|---|---|---|
| `plaintext` | no | no | (nothing) |
| `tls` | yes | no | optional `--ca` |
| `scram` (alias `scram-sha-512`) | yes¹ | SCRAM-SHA-512 | `--username`, `--password` |
| `scram-sha-256` | yes¹ | SCRAM-SHA-256 | `--username`, `--password` |
| `plain` | yes¹ | PLAIN | `--username`, `--password` |
| `oauthbearer` | yes¹ | OAUTHBEARER | one token source (below) |
| `mtls` | yes (mutual) | no | `--cert`, `--key`, `--ca` |
| `iam` | yes | AWS_MSK_IAM | `--region`, optional `--profile` |

¹ unless `--no-tls` / `no-tls: true` (see *SASL without TLS*).

`scram` stays SCRAM-SHA-512: it predates the SHA-256 variant, and changing
what it means would silently break every existing context. Each mechanism
is its own method rather than a `mechanism:` field beside `auth: sasl`,
because one string already selects everything else here, and a second field
would only add combinations that mean nothing (`auth: tls, mechanism:
plain`).

`AuthMethod` is a typed string. `AuthConfig` is a flat struct holding
every possible field; each constructor reads the subset it needs.
Anything else is ignored — we don't validate "only the right fields are
set", we just build with what's relevant.

## Method selection

`NewClientOpts(cfg, brokers)` switches on `cfg.Method`. Unknown method
returns an error listing the supported set. The method is never probed on
the wire: it comes from `--auth` or the context's `auth:` field, and when
`--brokers` is given without `--auth` it is inferred from the other flags
(`inferAuthMethod`, below), not by trying connections. We considered
probing (try plaintext, fall back to TLS), but Kafka brokers handle bad
protocols by hanging rather than rejecting cleanly, so probing always felt
wrong.

## TLS shape

Every TLS config (`newTLSConfig`, which `newMTLSConfig` builds on) sets
`MinVersion: tls.VersionTLS12`. We don't expose `MaxVersion` or cipher
suite knobs — Go's defaults are fine and any cluster requiring weaker than
1.2 isn't a cluster we want to make easy to connect to.

`--insecure` (`InsecureSkipVerify`) is honored by every TLS method. The
intent was that mTLS would not honor it — authenticating with a client
certificate while not verifying the server makes little sense — but
`newMTLSConfig` inherits it from `newTLSConfig`, and always has; #91
decides which way to settle it.

The CA bundle is read in `newTLSConfig`: optional for every TLS method,
required for mTLS. When given it is the whole root set — the system roots
are not consulted. `AppendCertsFromPEM` returns false on malformed input;
we surface that as an error with the file path so operators get something
actionable.

## IAM specifics

IAM auth is the de facto MSK path. Three things happen:

1. Default region is `awscreds.DefaultRegion` (`us-east-1`) if neither
   flag nor config provides one (matches the AWS SDK default but explicit
   so a misconfigured profile doesn't pick a surprising region).
2. AWS config loads via `internal/awscreds.Load` — `LoadDefaultConfig` with
   optional `WithSharedConfigProfile`. Credentials come from whatever
   `~/.aws/config`/`credentials`, env, or pod identity provides.
3. The SASL mechanism is franz-go's `sasl/aws.ManagedStreamingIAM`, whose
   callback retrieves credentials from the AWS config on every connection. Tokens are
   short-lived; rotation is automatic as long as the credential chain
   itself can refresh.

### Named profiles are assumed natively

A `--profile` is resolved **in-process**: the SDK reads that profile's SSO
session from the shared config and token cache, and when granted is wired
as the profile's `credential_process`, granted's login and refresh are
invoked by the SDK itself. So `k4a -p my-profile`
is run directly — there is no need to wrap it in
`assume --exec 'k4a …' <Profile>`. An ambient assumed session (exported
`AWS_*`, an outer `assume --exec`) still works: it's the default credential
chain, which is what an empty profile falls back to.

The one thing the SDK can't do is open a browser, so a *missing or expired*
SSO session is reported with the command that renews it —
`granted sso login <profile>` (`awscreds.LoginHint`). This applies to both
credential paths: the SSM broker lookup (`internal/config`) and the SASL
mechanism's per-handshake retrieve (`internal/kafka`).

When IAM tokens go stale (expired role session, rotated profile), the
symptom is connection errors, not silent data corruption. The TUI's
refresh-error handler calls `CloseIdleConnections`, so the next refresh
dials new connections that pick up new tokens; `:reconnect` rebuilds the
client and re-resolves credentials outright.

## SASL without TLS

`--no-tls` (context `no-tls: true`) runs one of the SASL methods (`scram`,
`scram-sha-256`, `plain`, `oauthbearer`) over a plaintext connection, Kafka's
`SASL_PLAINTEXT`, for local and dev clusters. It is an explicit opt-in and
never a fallback: without it every SASL method dials TLS, a TLS failure is
an error, and `tls`, `mtls` and `iam` refuse `--no-tls` instead of ignoring
it (MSK accepts IAM only over TLS). It is spelled as a positive opt-out
rather than `tls: false` because a re-saved context cannot tell `false` from
"not set". Like `--insecure`, the flag can only turn TLS off over a context.

Before connecting, the headless commands print a warning to stderr and the
TUI prints it before taking the screen, and `k4a-index` logs it at warn
(`kafka.CleartextWarning`). The daemon takes the same `--no-tls` and
`--oauth-*` flags and `KAFKA_OAUTH_*` env, which its Helm chart renders from
`kafka.noTLS` and `kafka.oauth`. The wording is per mechanism: PLAIN sends the
password itself, OAUTHBEARER sends a replayable token, and SCRAM sends
neither but exposes the exchange to offline guessing. In every case the
traffic is unencrypted.

## OAUTHBEARER

franz-go's `sasl/oauth` mechanism, fed by exactly one token source:

| Source | Flags (env) | Context fields |
|---|---|---|
| Static token | `--oauth-token` (`KAFKA_OAUTH_TOKEN`) | `oauth-token` |
| Token file, re-read on every handshake | `--oauth-token-file` (`KAFKA_OAUTH_TOKEN_FILE`) | `oauth-token-file` |
| Client-credentials grant | `--oauth-token-url`, `--oauth-client-id`, `--oauth-client-secret`, `--oauth-scopes` (`KAFKA_OAUTH_*`) | `oauth-token-url`, `oauth-client-id`, `oauth-client-secret`, `oauth-scopes` |

Setting none, or more than one, is an error. The token is resolved on each
handshake, not at startup, so an unreachable identity provider is a
connection error and franz-go's re-authentication at session expiry
(KIP-368) gets a current token. The token file suits a sidecar or a
Kubernetes projected token that rotates on disk.

The client-credentials token is fetched with `golang.org/x/oauth2`'s
`clientcredentials` and cached **in memory only**; it is fetched again once
it is within oauth2's expiry margin (10s) of expiring. The token URL must be
`https`; `http` is allowed only for a loopback host (a local dev IdP), since
the request carries the client secret. A URL with embedded userinfo is
refused, and errors print the URL without userinfo or query.

Secrets (`oauth-token`, `oauth-client-secret`) take the same whole-value
`$VAR` form as `password`. No token, secret or token response is logged or
written under `~/.k4a`.

## Why split `tls` and `scram`?

`tls` is "encrypted connection, anonymous client". A surprising number
of internal Kafka clusters run this way. `scram` adds SASL on top.
Combining them into "tls with optional SASL" looked tempting until the
flag matrix made the help text unreadable. Five named methods is the
compromise, and the SASL mechanisms above follow it.

## Field precedence (resolution rules)

Resolution happens in `internal/command/`, not here, but the ordering
matters for the auth surface:

1. CLI flags (`--auth`, `--username`, `--cert`, ...).
2. Env vars (`KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TLS_CERT`,
   `KAFKA_TLS_KEY`, `KAFKA_TLS_CA`, `AWS_REGION`, `AWS_PROFILE`).
3. Config-file context fields.

This is the kubectl-style flag-overrides-env-overrides-config pattern.
Every connection flag takes part (`overlayAuth` in
`internal/command/flags.go`, shared by the TUI and the headless
commands): an empty value means "not set" and keeps the context's.
`--region` therefore has no flag default — `us-east-1` is filled in only
for a `--brokers` connection with no context behind it — so an explicit
`--region us-east-1` still overrides a context in another region.
A `--brokers` connection without `--auth` infers the method
(`inferAuthMethod`): credentials mean `scram`, any OAuth setting means
`oauthbearer`, a client cert or key means `mtls`, and nothing means
`plaintext`. `--no-tls` never implies a method. It used to be `scram` regardless, so
`k4a -b localhost:9092` failed asking for a password.
`-k` can only turn verification off; it cannot re-enable it for a
context with `insecure: true`.

Env exists primarily for IAM profile/region (which often flow from the
shell) and for SCRAM creds (which we'd rather not ask users to put on
the command line).

## What's intentionally NOT here

- **No client-cert IAM hybrid.** MSK supports IAM *or* mTLS *or* SCRAM
  per-broker, not a combination. We don't build composite mechanisms.
- **No Kerberos / GSSAPI.** No demand. Adding it would be
  a GSSAPI `sasl.Mechanism` (franz-go ships none) plus another method;
  mostly mechanical, but not free in test surface.
- **No OIDC device or browser flow for OAUTHBEARER.** Client credentials
  and a token from env or file cover service accounts and anything an
  external tool can mint; an interactive login belongs in that tool.
- **No SASL extensions or authorization ID** on OAUTHBEARER yet. franz-go
  supports both; add them as `oauth-*` fields when a broker needs one.
- **No credential reload during a session.** The client options are built
  once at startup. Picking up new IAM tokens works because the SDK
  inside the SASL mechanism refreshes; for SCRAM/mTLS, restarting k4a
  is the answer. `:reconnect` rebuilds the client and is the documented
  escape hatch.
- **No platform keychain integration.** SCRAM passwords come from env
  or config (or interactively typed in a context, future). We don't
  reach into Keychain / Secret Service.

## Pickup notes

- **Adding a method:** add an `AuthMethod` constant, a switch arm in
  `NewClientOpts` (or `saslMechanism` for a SASL mechanism, plus
  `IsSASL` if it has a no-TLS form), and the flag plumbing in
  `internal/command/` (`ConnectionFlags`, `UIFlags`, `overlayAuth`),
  `internal/config` (`Context`, `ToAuthConfig`) and the daemon's own
  flags in `cmd/k4a-index`.
  Add a row to the table above.
- **Adding a TLS knob:** consider whether it belongs on every
  TLS-using method or only one. Most things (cipher suites, min
  version) should be uniform — push them into a shared helper rather
  than repeating in each constructor.
- **Debugging an auth failure:** the failure mode is always a hang or a
  cryptic franz-go error. Try `verify` subcommand (`internal/command`)
  which exercises just the handshake — it's the smallest reproducer.
- **Don't add `Insecure` to mTLS.** It's been requested. The answer is
  always to fix the CA bundle.
