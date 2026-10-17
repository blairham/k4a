# Design: Authentication

**Status:** living document — describes current behavior
**Code:** `internal/kafka/auth.go`, `internal/kafka/oauth.go`,
`internal/kafka/oidc.go`, `internal/kafka/tokenstore.go`, `internal/command/login.go`,
`internal/secrets`, `internal/command/credentials.go`, `internal/tui/credentials.go`

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

`--insecure` (`InsecureSkipVerify`) is honored by every TLS method except
mTLS. Authenticating with a client certificate while not verifying the
server makes little sense, and mTLS already requires the CA bundle that
fixes the verification failure, so `newMTLSConfig` **refuses** `Insecure`
with an error (`errMTLSInsecure`) rather than ignoring it: a context that
says `insecure: true` and `auth: mtls` fails to connect instead of quietly
verifying anyway. Every surface (CLI, TUI, `k4a-index`) builds its client
through `NewClient`, so the one check covers them all (#91).

The CA bundle is read in `newTLSConfig`: optional for every TLS method,
required for mTLS. When given it is the whole root set — the system roots
are not consulted. `AppendCertsFromPEM` returns false on malformed input;
we surface that as an error with the file path so operators get something
actionable.

## IAM specifics

IAM auth is the de facto MSK path. Three things happen:

1. The region resolves as: `--region` / the context's `region:` (an
   explicit value always wins), else `AWS_REGION` / `AWS_DEFAULT_REGION`
   as the SDK reads them, else the shared-config profile's `region`, else
   `awscreds.DefaultRegion` (`us-east-1`). Callers pass an empty region
   through rather than pre-filling `us-east-1`; `awscreds.Load` applies the
   fallback only after `LoadDefaultConfig` has resolved nothing. Until #102
   the fallback was passed to the SDK as `WithRegion`, which overrode the
   profile, so a profile's `region` was never used.
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
| Interactive OIDC login (device or browser) | `--oauth-issuer`, `--oauth-client-id`, optional `--oauth-client-secret`, `--oauth-scopes`, `--oauth-flow`, `--oauth-redirect-port` (`KAFKA_OAUTH_*`) | `oauth-issuer`, `oauth-client-id`, `oauth-client-secret`, `oauth-scopes`, `oauth-flow`, `oauth-redirect-port` |

Setting none, or more than one, is an error. `--oauth-flow` names the grant
(`client-credentials`, `device`, `browser`) and must fit the source: the
client-credentials grant takes a token URL, the two interactive flows an
issuer, and an issuer without a flow means `device`. The token is resolved on each
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

### Interactive OIDC login

For a person rather than a service account, `oauth-issuer` names an OpenID
Provider. Its endpoints come from discovery
(`<issuer>/.well-known/openid-configuration`), which must name the same
issuer; the issuer and every endpoint k4a sends a code or refresh token to
pass the same https-or-loopback check as the token URL. Two flows:

- **`device`** (the default; RFC 8628). k4a prints a URL and a code, the
  person enters the code on any device, and k4a polls until the IdP issues
  the token. It needs no browser on the machine running k4a, so it works
  over SSH. The URL and code come from the IdP and have control characters
  stripped before they reach the terminal.
- **`browser`** — authorization code with PKCE (RFC 7636, `S256`) and a
  loopback redirect (RFC 8252). k4a listens on `127.0.0.1`, on a free port
  or `--oauth-redirect-port` for an IdP that matches redirect URIs exactly,
  opens the system browser (`open`, `xdg-open`, `rundll32`) and prints the
  URL as well, and exchanges the code the browser brings back to
  `http://127.0.0.1:<port>/callback`. A callback without the login's random
  `state` is answered 400 and ignored, so it cannot end or hijack the wait;
  the wait gives up after five minutes.

The client may be public (no `oauth-client-secret`); then the client ID goes
in the form body rather than a Basic header. The token sent to Kafka is the
**access token**. A refresh token is issued only if the IdP's policy allows
it, which for many means asking for `offline_access` in `oauth-scopes`;
without one, every expiry is a new login.

**The handshake never prompts.** Logging in happens *before* connecting —
`ConnectionFlags.BuildClient` and the TUI's startup call `ensureOAuthLogin`,
which prints to stderr before the TUI takes the screen — because a device
login outlasts any dial timeout and the TUI owns the terminal once it runs.
On the handshake (and on KIP-368 re-authentication) the source uses the
cached token, refreshes it when it is within oauth2's 10s margin of expiry,
and otherwise fails with `ErrOAuthLoginRequired`, which names `k4a login`.
That is what a context switch inside the TUI to a context with no cached
login reports. A refresh the IdP refuses (expired, revoked, or rotated by
another k4a process) deletes the cached entry, so the next run logs in
rather than retrying a dead token. `k4a login` logs in ahead of time,
`--force` logs in again (to switch accounts), and `--logout` forgets the
cached token. `k4a-index` refuses an issuer outright: a daemon has nobody to
log in and must not ride on a person's refresh token.

**Token cache.** A login is cached as JSON (access token, expiry, refresh
token), keyed by a hash of issuer, client ID and sorted scopes, so two
contexts that share an IdP client share one login. `kafka.TokenStore` is
injected through `OAuthConfig.Store`. The command layer (`withTokenStore`,
from `fillSecrets` and `k4a login`) and a context switch in the TUI give it
a `KeychainTokenStore` over the same `secrets.Store` as the context
credentials: the entry is account `oauth/<hash>` under the service `k4a`.
When the keychain refuses the write — no Secret Service on a headless Linux
host, or a token past the macOS (~3 KB) or Windows (2.5 KB) entry limit,
which large Entra ID tokens can reach — it falls back to `FileTokenStore`:
one file per login in `os.UserConfigDir()/k4a/oauth/` (`~/Library/Application
Support/k4a/oauth` on macOS, `$XDG_CONFIG_HOME/k4a/oauth` on Linux), `0600`
in a `0700` directory, written atomically through a temp file and a rename.
A keychain save deletes any file copy, a load reads the keychain and then
the file, and `--logout` clears both. `FileTokenStore` alone is also what a
nil `Store` means. Neither copy is under `~/.k4a`, which holds nothing
secret and is what `k4a state export` archives. Whoever can read this
user's keychain or files can use the tokens until they expire or are
revoked, the same exposure as a cloud CLI's cache. Tests reach neither:
`internal/command`'s `TestMain` keychain is `secrets.NewMemory()`, the
kafka tests inject a memory store, and the file store is pointed at a temp
dir through `HOME` / `XDG_CONFIG_HOME`.

### SASL extensions and authorization ID

Any OAUTHBEARER source can carry RFC 7628 SASL extensions
(`--oauth-extensions` / `oauth-extensions`: `key=value,key=value`, as
Confluent Cloud's `logicalCluster` and `identityPoolId` require) and an
authorization ID (`--oauth-authzid` / `oauth-authzid`: act as that principal
where the broker allows it). franz-go writes both into the initial response
as given, so k4a validates them first: extension keys are letters only and
never `auth` (the token's own key), values are printable ASCII, a key may
not repeat, and the authzid is UTF-8 without control characters with `=`
and `,` escaped as `=3D` and `=2C` (RFC 5801). A token containing `\x01`
is refused at the handshake for the same reason. A value cannot contain a
comma. Both are also `k4a-index` flags (`KAFKA_OAUTH_EXTENSIONS`,
`KAFKA_OAUTH_AUTHZID`) and chart values (`kafka.oauth.extensions`,
`kafka.oauth.authzid`).

## Keychain and typed passwords

A context's secrets can live in the OS keychain (macOS Keychain, the Secret
Service on Linux, Windows Credential Manager, through `zalando/go-keyring`)
instead of the file or the environment. The context names the entries in a
`keychain:` block, each value an account under the service `k4a`:

```yaml
contexts:
  prod:
    brokers: b1:9096
    auth: scram
    username: alice
    keychain:
      password: prod/password   # also oauth-token, oauth-client-secret
```

`k4a set-credential prod [--field password|oauth-token|oauth-client-secret]`
reads the secret — typed twice without echo on a terminal, one line from a
pipe otherwise, so a password manager can feed it — stores it as
`k4a` / `prod/<field>`, writes the reference and **removes any literal value
of that field from the file**, so the two cannot disagree.
`k4a delete-credential prod [--field ...]` removes the entry and the
reference; an entry already gone is not an error.

A keychain entry fills its field only when nothing else did, so the order
is **flag > env > config value > keychain > typed**. It is read after the
flags are laid over the context (`Context.ApplyKeychain`), which means a
`--password` or `KAFKA_PASSWORD` never touches the keychain, and a context
with no `keychain:` block never reads it at all. A reference to a missing
entry is an error naming `set-credential`, not a silent fallback.

When a username-and-password method (`scram`, `scram-sha-256`, `plain`)
has a username and still no password, k4a asks for it:

- the headless commands and `k4a ui` at startup prompt on the terminal,
  without echo, **only when stdin is a terminal**; a pipe or `/dev/null` is
  left alone and the connection fails with the usual missing-password error,
  so a script never hangs on a prompt;
- a context chosen inside the TUI (the picker, `:reconnect`) asks in the
  prompt bar, masked; esc cancels the switch.

A typed password goes into that one client and is not kept: the next switch
or `:reconnect` to that context asks again. `set-credential` is how to stop
being asked. Nothing typed or read from the keychain is logged or written
under `~/.k4a`.

Tests never reach the real keychain. Everything takes a `secrets.Store`:
`internal/command` reads its `secretStore` variable, which its `TestMain`
replaces with `secrets.NewMemory()` (and the prompt with a stub), and the
TUI has no store at all until `k4a ui` sets one (`App.SetSecretStore`).

Limits: the keychain holds the Kafka secrets only — the schema-registry
password and token and the index token keep the file-or-`$VAR` forms.
`k4a-index` is a daemon with no keychain or terminal, so it ignores
`keychain:` and never prompts. The TUI's prompt bar trims surrounding
spaces, so a password that begins or ends with a space has to come from
the keychain or a flag.

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
`--region` therefore has no flag default — an unset region stays empty
all the way to `awscreds.Load`, which consults the env and the profile
before falling back to `us-east-1` — so an explicit `--region us-east-1`
still overrides a context in another region.
A `--brokers` connection without `--auth` infers the method
(`kafka.InferAuthMethod`, used by the CLI, the TUI and `k4a-index` alike):
credentials mean `scram`, any OAuth setting means
`oauthbearer`, a client cert or key means `mtls`, and nothing means
`plaintext`. `--no-tls` never implies a method. It used to be `scram` regardless, so
`k4a -b localhost:9092` failed asking for a password.
`-k` can only turn verification off; it cannot re-enable it for a
context with `insecure: true`.

Env exists primarily for IAM profile/region (which often flow from the
shell) and for SCRAM creds (which we'd rather not ask users to put on
the command line). After those three come the keychain and then a typed
password (see *Keychain and typed passwords*).

## What's intentionally NOT here

- **No client-cert IAM hybrid.** MSK supports IAM *or* mTLS *or* SCRAM
  per-broker, not a combination. We don't build composite mechanisms.
- **No Kerberos / GSSAPI.** No demand. Adding it would be
  a GSSAPI `sasl.Mechanism` (franz-go ships none) plus another method;
  mostly mechanical, but not free in test surface.
- **No interactive OIDC without discovery.** The device and browser flows
  need an issuer that serves `/.well-known/openid-configuration`; there is
  no way to name the authorization or device endpoint by hand. The ID token
  is never sent, only the access token.
- **No credential reload during a session.** The client options are built
  once at startup. Picking up new IAM tokens works because the SDK
  inside the SASL mechanism refreshes; for SCRAM/mTLS, restarting k4a
  is the answer. `:reconnect` rebuilds the client and is the documented
  escape hatch.

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
- **Don't add `Insecure` to mTLS.** It's been requested, and
  `newMTLSConfig` refuses it (`TestMTLSRefusesInsecure`). The answer is
  always to fix the CA bundle.
