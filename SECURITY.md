# Security Policy

## What k4a can do

k4a is three programs with different trust surfaces.

### `k4a`, the CLI and TUI

`k4a` connects to Kafka **as you**: with whatever credentials you give it, it
can read every topic and run every admin operation those credentials allow,
including deleting topics, consumer groups, ACLs and records.

- **The broker's authorization is the only boundary.** `ui.readonly: true`
  (and the TUI's `--readonly`) hides and blocks the mutating actions in the
  TUI and refuses the mutating headless subcommands (`produce`,
  `create-topic`, `delete-topic`, `alter-config`, `alter-replication`,
  `create-acl`, `index clear`, `state clear`), so a slip cannot delete a
  topic. `delete-topic` also asks before deleting unless given `--yes`. It
  is a guard rail, not a security control: anyone who can edit the config
  can turn it off. To be unable to change a cluster, connect with a
  principal that cannot.
- **Credentials** come from one of:
  - **AWS** (`--auth iam`): the AWS SDK's shared configuration, for the named
    `profile` or the default chain. An SSO session, `credential_process` or
    static keys in `~/.aws` are read by the SDK; k4a stores none of them. The
    same credentials resolve `ssm-brokers` through SSM.
  - **SASL username and password** (`--auth scram`, which is SCRAM-SHA-512;
    `--auth scram-sha-256`; `--auth plain`): from `--username`/`--password`,
    `KAFKA_USERNAME`/`KAFKA_PASSWORD`, or a context in `~/.k4a/config.yaml`.
    A context value that is exactly `$VAR` or `${VAR}` is read from that
    environment variable, so the file can name one instead of holding the
    password; a `$` anywhere else is literal. Prefer the environment to
    `--password`, which other local users can see in the process list.
    Better still, `k4a set-credential CONTEXT` moves the password into the OS
    keychain and leaves the file a `keychain:` reference; a context with no
    password at all is asked for one on a terminal (or in the TUI's masked
    prompt), and the typed password is held only by that connection.
  - **OAUTHBEARER** (`--auth oauthbearer`): a static token
    (`--oauth-token`/`KAFKA_OAUTH_TOKEN`), a token file re-read on every
    connection, or the OAuth 2.0 client-credentials grant
    (`--oauth-token-url` with a client ID and secret). The client secret is
    sent only to an `https` token URL (or `http` on a loopback host), and the
    fetched token is held in memory and never written to disk or logged. The
    same `$VAR`, process-list and keychain (`--field oauth-token` /
    `oauth-client-secret`) advice applies to the token and the secret.
    An **interactive OIDC login** (`--oauth-issuer`, device-code or browser
    flow with PKCE) caches its access and refresh tokens: in the OS keychain
    (service `k4a`, account `oauth/<hash>`) when it takes them, otherwise
    (no keychain, or a token too large for one) in an owner-only file
    (`0600` in a `0700` directory) under the user config directory
    (`k4a/oauth/`), never under `~/.k4a`, so `k4a state export` does not
    carry them. Anyone who can read your keychain or files can use them
    until they expire or are revoked; `k4a login --logout` deletes them.
    Codes and refresh tokens go only to endpoints that pass the same
    https-or-loopback check, discovered from an issuer that must name
    itself, and the browser flow's loopback listener ignores a callback
    without the login's random `state`. `k4a-index` refuses an issuer, so a
    daemon never runs on a person's login.
  - **mTLS** (`--auth mtls`): certificate, key and CA PEM files named by path;
    k4a reads them and never copies them.
- **TLS** is verified against the system roots, plus `--ca` when given, at TLS
  1.2 or later. `--insecure` (`-k`, or `insecure: true` on a context) **turns
  verification off**: anyone on the network path can then impersonate the
  broker and, under PLAIN or OAUTHBEARER, receive the password or token (or,
  under SCRAM, an exchange to guess it from). Use it only against a
  cluster you can reach no other way. `--auth mtls` **refuses** `--insecure`
  with an error rather than present the client certificate to an unverified
  broker; fix the `--ca` bundle instead. `--auth plaintext` uses no TLS at all.
- **SASL without TLS** is opt-in only: `--no-tls` (or `no-tls: true` on a
  context) runs `scram`, `scram-sha-256`, `plain` or `oauthbearer` over a
  plaintext connection, and k4a warns before connecting. Under PLAIN the
  password, and under OAUTHBEARER the bearer token, then cross the network in
  cleartext; SCRAM does not send the password but its exchange can be
  captured and guessed offline. All traffic is unencrypted. Without the flag
  a SASL method always uses TLS, and a TLS failure is an error, not a
  fallback.
- **Schema Registry** credentials (a context's `schema-registry:` block or
  `--schema-registry-*`, HTTP basic or a bearer token) are sent only to the
  registry URL, only over https or http to a loopback host, and are never
  logged. A registry URL with embedded credentials is refused. Schemas the
  registry returns are held in memory and never written to disk. Decoded
  values are displayed (or saved, copied or exported, when you ask) and,
  with a registry configured, searched: the search cache and the local and
  shared indexes then hold a framed key or value as its decoded JSON, the
  same record data in another encoding, where they would otherwise hold the
  bytes. Beside it they keep only a registry identity, a hash of the URL
  without its credentials, query or fragment, which is also what `k4a-index`
  reports to its clients. `k4a-index` reads registry credentials from
  `K4A_SCHEMA_REGISTRY_*` (the chart takes them from an existing Secret) and
  never logs them.
- **Copy and export** are explicit: on a local session a copy pipes the
  text to the platform clipboard tool (`pbcopy`, `wl-copy`, `xclip` or
  `xsel`, found on `PATH`, as stdin, never on the command line); over SSH,
  or with `ui.clipboard: osc52`, it hands the text to your terminal as an
  OSC 52 escape (the terminal, not k4a, writes the clipboard). The
  confirmation names what was copied, its size and the tool, never its
  content. An
  export writes JSONL, `0600`, to the path you type (relative to the working
  directory by default), refuses a path inside `~/.k4a`, and asks before it
  replaces a file.
- **Files k4a writes** live under `~/.k4a`, in directories created `0700`:
  the config file, session state, the search-result cache (which holds the
  matched messages) and, when you opt in with `index.enabled: true`, a local
  index that **stores message keys, values and headers** of the topics you
  search. Treat the directory as holding a copy of that data.
- **`k4a state export`** writes that data (the index only with `--index`)
  to a `0600` tar.gz you name, never inside `~/.k4a`. Inline passwords and
  tokens are stripped from the exported config; `$VAR` references are
  kept. `--include-secrets` keeps them, and both export and import print a
  warning. `k4a state import` validates the whole archive first (no links,
  absolute paths or `..`, nothing outside the export layout), writes only
  inside the state directory, honors `ui.readonly` and asks before
  replacing anything.
- **`k4a upgrade`** downloads the latest release from
  `github.com/blairham/k4a` over HTTPS and checks the archive against the
  release's `checksums.txt` before replacing the running binary. To raise
  GitHub's rate limit it sends a token from `gh auth token`, `GH_TOKEN` or
  `GITHUB_TOKEN` when one is available, to the GitHub API only. It does not
  verify the cosign signature; do that by hand (below) when it matters.
- **`k4a mcp`** serves the `search` tool over stdio to the local process that
  started it, with the same credentials and the same reach as the CLI.

### `k4a-index`, the shared index daemon

`k4a-index` tails Kafka topics with **its own** credentials and answers
searches over gRPC for anyone who can reach it.

- **Transport authentication is opt-in; by default there is none.** The gRPC
  listener (`--listen`, default `:9500`) is plaintext and accepts every
  caller unless configured otherwise, and the daemon logs a warning at
  startup when it is. It can serve TLS (`--tls-cert`/`--tls-key`), require a
  client certificate from a CA (`--tls-client-ca`, mTLS), and require a bearer
  token (`--token-file` or `K4A_INDEX_TOKEN`, compared in constant time).
  Health checks stay open; every other RPC is refused `Unauthenticated`
  without the configured credential. Clients refuse to send a token over a
  plaintext dial.
- **Authentication is not authorization.** Anyone holding the deployment's
  token or a client certificate the CA signed can search every topic the
  daemon follows and start follows within `--allow`, whatever their own Kafka
  permissions are. Per-caller authorization (Phase 5 of
  `docs/design/shared-index-service.md`) does not exist yet. Without
  credentials configured, **the network is the boundary**: expose the port
  only on a private network.
- **The admin port** (`--admin-listen`; the chart's `metrics.port`, 9502)
  serves Prometheus metrics and plain-HTTP health with no credential. It
  exposes followed topic names and counters, never record content; restrict
  it to the scraper (`networkPolicy.metricsFrom`).
- **A search is bounded.** `--max-search-limit` (default 10000) lowers any
  larger or missing request limit and reports the answer as capped. Search
  patterns, which can hold what a caller is looking for (an email address,
  an account number), are logged only at `debug`.
- **The `--allow` topic allowlist is the authorization boundary.** It is
  required and default-deny: a topic is followed only if it matches one of the
  glob patterns, and a search for any other topic is answered with no
  coverage and never starts a follow. A search for an allowed topic *does*
  start one. Keep the allowlist no wider than what you would let every client
  read, and give the daemon's Kafka principal no more than the allowlist
  needs. A name Kafka itself would reject (`.`, `..`, anything outside
  `[a-zA-Z0-9._-]` or over 249 characters) is never followed, whatever the
  patterns say, since the topic name becomes a directory under `--data-dir`.
- **It persists payloads to disk.** Followed topics' keys, values and headers
  are written, as the consumer received them (after TLS, so decrypted), to an
  index under `--data-dir`. Protect that volume as you would the topics, and
  bound it with `--topic-max-bytes` and `--max-total-bytes`.
- **It writes to Kafka only with `--followset-topic`.** Then it keeps the
  follow-set (topic names and last-use times, never record content) in that
  compacted topic and follows what it finds there. Whoever can write to the
  topic can make every replica follow a topic, but only one `--allow`
  already admits, which is no more than a `Follow` call can do; give that
  write grant to the daemon's identity alone.
- Its Kafka credentials and TLS options are the CLI's (above), including
  `--insecure` and `--no-tls`.

### `k4a-mcp`, the networked MCP server

`k4a-mcp` exposes `search` and `coverage` over streamable HTTP
(`--listen`, default `:9501`) and forwards them to `k4a-index`. It holds no
Kafka credentials of its own.

- **By default it is plaintext and unauthenticated** (and logs a warning
  saying so): it serves anyone who can reach it whatever the index will
  answer, so a private network only. It can serve HTTPS (`--tls-cert`),
  require client certificates (`--tls-client-ca`) and require a bearer token
  (`--token-file` / `K4A_MCP_TOKEN`), with the same guard and the same
  limits as `k4a-index`: `/health` stays open, and authentication is not
  per-caller authorization.
- **Its hop to `k4a-index` is plaintext gRPC by default.** `--index-tls`,
  `--index-ca`, `--index-cert`/`--index-key` and `--index-token-file` give
  it what an authenticated `k4a-index` asks for; it refuses to start with an
  index token and no TLS rather than send the token in the clear.
- The tool's search query is logged at `debug` only.

## Supported versions

k4a is pre-stable (`v0.0.x`). Only the latest release receives fixes.

## Verifying a release

Releases are signed with [cosign](https://github.com/sigstore/cosign) keyless
signing: the signature is tied to the GitHub Actions workflow that built the
release, not to a key someone could leak. `.github/workflows/release.yml` and
`chart.yml` run the shared workflows in
[blairham/.github](https://github.com/blairham/.github)
(`.github/workflows/go-release.yml` and `go-chart.yml`), so the signing
identity is that shared workflow; the certificate also names this repository
and the tag.

**Downloads.** `checksums.txt` is signed; it lists the digest of every
archive. Verify the signature, then the archives against it:

```sh
VERSION=v0.0.2
cosign verify-blob \
  --certificate-identity-regexp '^https://github\.com/blairham/\.github/\.github/workflows/go-release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository blairham/k4a \
  --certificate-github-workflow-ref "refs/tags/$VERSION" \
  --bundle checksums.txt.sigstore.json checksums.txt
sha256sum --check --ignore-missing checksums.txt
```

**Provenance.** Every archive carries SLSA build provenance, generated by
[`actions/attest-build-provenance`](https://github.com/actions/attest-build-provenance)
in the release workflow and stored in the repository's attestations:

```sh
gh attestation verify k4a_Linux_x86_64.tar.gz --repo blairham/k4a \
  --signer-workflow blairham/.github/.github/workflows/go-release.yml
```

The same bundle is attached to the release as `k4a-$VERSION.intoto.jsonl`,
for checking offline:

```sh
gh attestation verify k4a_Linux_x86_64.tar.gz --repo blairham/k4a \
  --bundle "k4a-$VERSION.intoto.jsonl" \
  --signer-workflow blairham/.github/.github/workflows/go-release.yml
```

**Images.** `ghcr.io/blairham/k4a-index` and `ghcr.io/blairham/k4a-mcp` are
signed by digest:

```sh
cosign verify ghcr.io/blairham/k4a-index:0.0.2 \
  --certificate-identity-regexp '^https://github\.com/blairham/\.github/\.github/workflows/go-release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository blairham/k4a
```

and carry SLSA build provenance, stored in ghcr.io beside them and in the
repository's attestations. The subject is the multi-arch index, so check it
by tag (image tags carry no `v`):

```sh
gh attestation verify oci://ghcr.io/blairham/k4a-index:0.0.2 --repo blairham/k4a \
  --signer-workflow blairham/.github/.github/workflows/go-release.yml
gh attestation verify oci://ghcr.io/blairham/k4a-mcp:0.0.2 --repo blairham/k4a \
  --signer-workflow blairham/.github/.github/workflows/go-release.yml
```

**Helm charts.** `k4a-index` and `k4a-mcp` are published to
`oci://ghcr.io/blairham/charts` by `chart.yml` (blairham/.github's
`go-chart.yml`) and signed by digest the same way:

```sh
cosign verify ghcr.io/blairham/charts/k4a-index:0.0.2 \
  --certificate-identity-regexp '^https://github\.com/blairham/\.github/\.github/workflows/go-chart\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository blairham/k4a
cosign verify ghcr.io/blairham/charts/k4a-mcp:0.0.2 \
  --certificate-identity-regexp '^https://github\.com/blairham/\.github/\.github/workflows/go-chart\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-github-workflow-repository blairham/k4a
```

**Tags released before the move to blairham/.github** (v0.0.1 and
earlier) were signed by this repository's own workflows. Verify those with
`--certificate-identity "https://github.com/blairham/k4a/.github/workflows/goreleaser.yml@refs/tags/$VERSION"`
(images: `--certificate-identity-regexp '^https://github\.com/blairham/k4a/\.github/workflows/goreleaser\.yml@refs/tags/v'`;
the charts: `.../workflows/chart\.yml@`) in place of the identity flags
above, and without `--signer-workflow`.

## Reporting a vulnerability

**Do not open a public issue.** Report it privately through GitHub:
[Security → Report a vulnerability](https://github.com/blairham/k4a/security/advisories/new).

Please include the affected version or commit, what an attacker can do, and
the steps to reproduce. You should receive a response within a week.

In scope, among others:

- `k4a-index` following, indexing or returning records of a topic that does
  not match `--allow`
- a password, SCRAM credential, OAuth token or client secret, AWS credential
  or private key written to a log, the state directory, the search cache or
  the index
- a TLS connection that skips verification without `--insecure`, or SASL
  credentials sent without TLS when `--no-tls` was not given
- files under `~/.k4a`, or the OIDC token cache, created readable by other
  users
- `k4a upgrade` installing an archive that does not match `checksums.txt`
- a crash or unbounded resource use in `k4a-index` or `k4a-mcp` caused by a
  request

Also in scope: a `k4a-index` or `k4a-mcp` configured with a token or client
CA answering a request that lacks it (other than a health check), or a k4a
client or `k4a-mcp` sending an index token over plaintext.

Out of scope: anything that follows from exposing an unauthenticated
`k4a-index` or `k4a-mcp` to a network you do not trust, from running with
`--insecure`, from an allowlist wider than you meant, or from an
authenticated caller reading a followed topic their own Kafka permissions
would not allow (there is no per-caller authorization yet).
