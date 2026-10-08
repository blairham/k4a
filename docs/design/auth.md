# Design: Authentication

**Status:** living document — describes current behavior
**Code:** `internal/kafka/auth.go`

## Purpose

Build a `kafkago.Transport` for one of the five authentication methods
Kafka clusters in the wild actually use, and surface configuration via
flags + env + config-file context with sensible precedence.

The auth layer doesn't talk to Kafka. It just constructs the Transport;
the rest of the kafka package consumes it.

## Methods

| Method | TLS | SASL | Required inputs |
|---|---|---|---|
| `plaintext` | no | no | (nothing) |
| `tls` | yes | no | optional `--ca` |
| `scram` | yes | SCRAM-SHA-512 | `--username`, `--password` |
| `mtls` | yes (mutual) | no | `--cert`, `--key`, `--ca` |
| `iam` | yes | AWS_MSK_IAM | `--region`, optional `--profile` |

`AuthMethod` is a typed string. `AuthConfig` is a flat struct holding
every possible field; each constructor reads the subset it needs.
Anything else is ignored — we don't validate "only the right fields are
set", we just build with what's relevant.

## Method selection

`NewTransport(cfg)` switches on `cfg.Method`. Unknown method returns an
error listing the supported set. There's no auto-detection; the user
specifies the method explicitly via `--auth` or in their context's
`auth:` field. We considered probing (try plaintext, fall back to TLS),
but Kafka brokers handle bad protocols by hanging rather than rejecting
cleanly, so probing always felt wrong.

## TLS shape

Every TLS-bearing transport sets `MinVersion: tls.VersionTLS12`. We
don't expose `MaxVersion` or cipher suite knobs — Go's defaults are
fine and any cluster requiring weaker than 1.2 isn't a cluster we want
to make easy to connect to.

`--insecure` (`InsecureSkipVerify`) is honored on plaintext-TLS, SCRAM,
and IAM transports. mTLS deliberately doesn't honor it — if you're
authenticating with a client certificate, asserting the server identity
isn't optional. If the user needs both, they should fix their CA bundle.

CA bundle loading lives in `loadCAPool`. It's used by SCRAM (optional
override) and mTLS (required). `AppendCertsFromPEM` returns false on
malformed input; we surface that as an error with the file path so
operators get something actionable.

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

## Why split `tls` and `scram`?

`tls` is "encrypted connection, anonymous client". A surprising number
of internal Kafka clusters run this way. `scram` adds SASL on top.
Combining them into "tls with optional SASL" looked tempting until the
flag matrix made the help text unreadable. Five named methods is the
compromise.

## Field precedence (resolution rules)

Resolution happens in `internal/command/`, not here, but the ordering
matters for the auth surface:

1. CLI flags (`--auth`, `--username`, `--cert`, ...).
2. Env vars (`KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TLS_CERT`,
   `KAFKA_TLS_KEY`, `KAFKA_TLS_CA`, `AWS_REGION`, `AWS_PROFILE`).
3. Config-file context fields.

This is the kubectl-style flag-overrides-env-overrides-config pattern.
Env exists primarily for IAM profile/region (which often flow from the
shell) and for SCRAM creds (which we'd rather not ask users to put on
the command line).

## What's intentionally NOT here

- **No client-cert IAM hybrid.** MSK supports IAM *or* mTLS *or* SCRAM
  per-broker, not a combination. We don't build composite Transports.
- **No Kerberos / GSSAPI.** No demand. Adding it would be
  `kafka-go/sasl/gssapi` plus another method; mostly mechanical, but
  not free in test surface.
- **No OAUTHBEARER.** Same reason. If a real user needs it, the auth
  enum was designed to extend.
- **No credential reload during a session.** The Transport is built
  once at startup. Picking up new IAM tokens works because the SDK
  inside the SASL mechanism refreshes; for SCRAM/mTLS, restarting k4a
  is the answer. `:reconnect` rebuilds the client and is the documented
  escape hatch.
- **No platform keychain integration.** SCRAM passwords come from env
  or config (or interactively typed in a context, future). We don't
  reach into Keychain / Secret Service.

## Pickup notes

- **Adding a method:** add an `AuthMethod` constant, a constructor
  (`new<Name>Transport`), a switch arm in `NewTransport`, and the
  CLI flag plumbing in `internal/command/`. Add a row to the table
  above.
- **Adding a TLS knob:** consider whether it belongs on every
  TLS-using transport or only one. Most things (cipher suites, min
  version) should be uniform — push them into a shared helper rather
  than repeating in each constructor.
- **Debugging an auth failure:** the failure mode is always a hang or a
  cryptic kafka-go error. Try `verify` subcommand (`internal/command`)
  which exercises just the handshake — it's the smallest reproducer.
- **Don't add `Insecure` to mTLS.** It's been requested. The answer is
  always to fix the CA bundle.
