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
trivially serializable.

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

`os.ExpandEnv` runs on `brokers`, `username`, and `password`. It does
*not* run on `ssm-brokers`, `cert`, `key`, `ca`, `region`, `profile`,
or `auth`. Reasoning:

- `brokers` benefits from per-host overrides (`$KAFKA_HOST`).
- SCRAM creds are commonly env-injected by secret managers.
- File paths are deliberately literal — too easy to mask a path bug
  with `$HOME` expansion that silently produces wrong paths in CI.
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
- **No password prompting.** SCRAM `password:` is read literally (or via
  env expansion). We don't pop a TUI prompt on missing creds. The
  rationale is that interactive prompting tangles with the bubbletea
  startup sequence; users with this need can `read -s -p` in their
  shell wrapper.
- **No context validation on load.** A context with `auth: iam` but no
  region just fails at connect time, not load time. Validating up
  front would mean refusing to load a file with one bad context just
  because the user asked for a different one.

## Pickup notes

- **Adding a context field:** add to `Context` struct, decide whether
  it gets `os.ExpandEnv`, plumb through `ToAuthConfig` (or through
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
  remember the file is `0o600` but `git diff` doesn't care. Document
  it as "do not check this file in" rather than building a separate
  secret store.
