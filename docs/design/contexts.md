# Design: Contexts

**Status:** living document — describes current behavior
**Code:** `internal/config/config.go`, `internal/tui/views/context.go`,
`internal/command/`

## Purpose

Let users keep multiple Kafka clusters configured in one place and
switch between them by name, including in-app. Borrows directly from
kubeconfig: a top-level `current-context`, a map of named `contexts`,
each with the connection settings.

The shape was chosen so engineers who already know `kubectl` get a tool
that feels familiar — `current-context`, `contexts`, the same word for
the same concept. We resisted prefixing or renaming.

## File location

Default path: `~/.k4a/config.yaml`. Overridable with `--config`. Loader
treats "file does not exist" as "empty config" so first-run users can
launch with `--brokers` and never touch the file.

`Save()` creates `~/.k4a` with mode `0o700` and writes the file with
mode `0o600`. The config commonly carries SCRAM passwords or AWS
profile names; we don't want it world-readable.

## Schema

```yaml
current-context: staging
contexts:
  staging:
    ssm-brokers: /my-cluster/msk-brokers
    auth: iam
    region: us-east-1
    profile: my-staging-profile
  production:
    brokers: broker1:9098,broker2:9098
    auth: iam
    region: us-east-1
    profile: my-production-profile
ui:
  logoless: true
  readonly: true
```

Each `Context` is a flat struct mapping 1:1 to YAML fields. There's no
nested `auth:` object — auth fields are siblings of `brokers`. We
considered nesting (`auth: { method: iam, region: us-east-1 }`) but the
flat form reads better at the YAML level and keeps the Go struct
trivially serializable. The OAUTHBEARER settings follow the same rule:
they are a Go sub-struct (`config.OAuth`, inlined) but flat `oauth-token`,
`oauth-token-file`, `oauth-token-url`, `oauth-client-id`,
`oauth-client-secret` and `oauth-scopes` keys in YAML, and `no-tls: true`
sits beside `insecure`. See `docs/design/auth.md`.

The one nested block is `schema-registry:` (`url`, `username`, `password`,
`token`, `ca`). The registry is a second service with credentials of its own,
and flat keys would have to be `schema-registry-username` and so on beside
Kafka's own `username`; nesting keeps the two sets from being mistaken for
each other. Its secrets take the same whole-value `$VAR` form. See
`docs/design/schema-registry.md`.

`keychain:` is the other nested block: it maps `password`, `oauth-token`
and `oauth-client-secret` to OS keychain accounts, so the file names the
secret instead of holding it. `k4a set-credential` writes it; a context
with a username and no password is asked for one instead. See *Keychain and
typed passwords* in `docs/design/auth.md`.

## Brokers vs SSM-brokers

Two ways to specify the bootstrap broker list:

- `brokers:` — a literal comma-separated string. Supports `os.ExpandEnv`
  so `$VAR` references env vars, useful for per-machine overrides
  without forking the file.
- `ssm-brokers:` — an SSM Parameter Store key. Fetched at connect time.

Direct `brokers` always wins if both are set. If neither is set,
`ResolveBrokers` errors at connect — the failure surfaces in the
context selector, not silently as a connection timeout.

SSM fetching uses the same region/profile as the auth config. This
matters: in MSK shops, the same AWS identity that's allowed to
authenticate to the cluster is usually allowed to read the bootstrap
parameter. Splitting them would mean a second auth dance.

## In-app context switching

The Context view (`views/context.go`) lists contexts and lets the user
pick one. On selection it sends a `switchContextMsg` to the App, which:

1. Closes the existing `kafka.Client`.
2. Builds a new `AuthConfig` from the selected context.
3. Rebuilds the client, including SSM resolution.
4. Resets `viewMap` so the new client is wired in everywhere.
5. Switches to the Topics view.
6. Updates `current-context` in the config and `Save()`s it.

Step 6 is why `Save()` exists. The user picking a context in-app is an
implicit "remember this for next time"; we persist that with no
prompt. If the user has a write-protected config (read-only mount), the
save fails with a flash but the switch itself still works for the
session.

## Startup flow

`main.go` resolves the connection in this order:

1. If `--brokers` was passed: build an ad-hoc context from flags, skip
   the config entirely.
2. Else if `--context` was passed: load config, look up that context.
3. Else: load config, use `current-context`, fall back to the context
   selector view if nothing's set.

`SetSkipContextView()` on the App tells it to land directly on Topics
after splash (cases 1 and 2) instead of showing the selector. This is
the kubectl-style "explicit beats implicit" behavior — passing flags
means "I know what I want, don't ask".

## Env expansion details

Two rules, the same for a context field and for the matching CLI flag
(`expandEnvFlags` in `internal/command/expand.go`):

- **`brokers`** gets full `os.ExpandEnv`, so `$KAFKA_HOST:9092` works
  for per-host overrides. A host list never holds a literal `$`.
- **`username`, `password`, `cert`, `key`, `ca`, the `oauth-*` fields**
  (and every other string flag) expand only a *whole-value* reference
  (`config.ExpandRef`): a value that is exactly `$NAME` or `${NAME}` is
  replaced with that variable (empty when unset); anything else is used
  literally. `password: ${SCRAM_PASSWORD}` keeps working, and
  `pa$$word` is no longer mangled into `pa` plus whatever `$word` held.
  There is no escape syntax: a secret that is literally `$NAME` in
  shape has to come from the environment instead.

Not expanded at all: `ssm-brokers`, `region`, `profile`, `auth`.
Reasoning:

- SCRAM creds are commonly env-injected by secret managers, but they
  also contain `$` often enough that substring expansion corrupted them.
- File paths are not expanded inside the value — `$HOME/certs/ca.pem`
  stays literal, because it is too easy to mask a path bug with
  expansion that silently produces wrong paths in CI. A whole path in
  a variable (`ca: $KAFKA_TLS_CA`) is supported, and is what the flags
  already allowed; before #40 the context ignored it.
- AWS region/profile have their own env conventions handled inside the
  SDK (`AWS_REGION`, `AWS_PROFILE`); duplicating expansion at this
  layer would conflict.

## What's intentionally NOT here

- **No context inheritance / merging.** Each context stands alone. We
  considered a base `cluster:` field that contexts could reference (a la
  kubeconfig users + clusters split) but it added cognitive load
  without anyone asking for the deduplication.
- **No remote config loading.** The file is local. Pulling from S3 /
  HTTP / etc. would be a feature, not a fix; not on the roadmap.
- **No multi-file config.** kubectl supports `KUBECONFIG=file1:file2`.
  We don't. If we ever need it, follow the same pattern.
- **No context validation on load.** A context with `auth: iam` but no
  region just fails at connect time, not load time. Validating up
  front would mean refusing to load a file with one bad context just
  because the user asked for a different one.

## Pickup notes

- **Adding a context field:** add to `Context` struct, decide whether
  it gets `config.ExpandRef` (or `os.ExpandEnv`, for broker lists only), plumb through `ToAuthConfig` (or through
  `ResolveBrokers` if it's broker-related). Update the schema example
  in this doc and `AGENTS.md`.
- **Adding a non-AWS secret-store backend** (e.g. Vault for brokers
  list): mirror `fetchSSMParameter` as a separate method, pick by
  another field (`vault-brokers:`), keep the resolution chain
  deterministic (`brokers > ssm-brokers > vault-brokers`).
- **Migrating the schema:** there's no version field. If we change
  shapes incompatibly, add a `apiVersion: v2` and key the loader off
  it; don't try to autodetect.
- **Sensitive context fields:** if you add one (token, key material),
  remember the file is `0o600` but `git diff` doesn't care. If it should
  be keychain-capable, add it to `config.Keychain`, `keychainSlot`,
  `ApplyKeychain` and the `Field*` constants (and the `--field` choices).
