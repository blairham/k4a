# Security Policy

## What k4a can do

k4a is three programs with different trust surfaces.

### `k4a`, the CLI and TUI

`k4a` connects to Kafka **as you**: with whatever credentials you give it, it
can read every topic and run every admin operation those credentials allow,
including deleting topics, consumer groups, ACLs and records.

- **The broker's authorization is the only boundary.** `--readonly` (and
  `ui.readonly: true`) hides and blocks the mutating actions in the TUI so a
  slip of the keyboard cannot delete a topic; it is a guard rail, not a
  security control, and the headless subcommands (`delete-topic`,
  `alter-config`, `create-acl`, …) do not consult it. To be unable to change
  a cluster, connect with a principal that cannot.
- **Credentials** come from one of:
  - **AWS** (`--auth iam`): the AWS SDK's shared configuration, for the named
    `profile` or the default chain. An SSO session, `credential_process` or
    static keys in `~/.aws` are read by the SDK; k4a stores none of them. The
    same credentials resolve `ssm-brokers` through SSM.
  - **SASL/SCRAM** (`--auth scram`, SHA-512, always over TLS): a username and
    password from `--username`/`--password`, `KAFKA_USERNAME`/`KAFKA_PASSWORD`,
    or a context in `~/.k4a/config.yaml`. Context values expand `$VAR`, so the
    file can name an environment variable instead of holding the password.
    Prefer the environment to `--password`, which other local users can see
    in the process list.
  - **mTLS** (`--auth mtls`): certificate, key and CA PEM files named by path;
    k4a reads them and never copies them.
- **TLS** is verified against the system roots, plus `--ca` when given, at TLS
  1.2 or later. `--insecure` (`-k`, or `insecure: true` on a context) **turns
  verification off**: anyone on the network path can then impersonate the
  broker and, under SCRAM, receive the password. Use it only against a
  cluster you can reach no other way. `--auth plaintext` uses no TLS at all.
- **Files k4a writes** live under `~/.k4a`, in directories created `0700`:
  the config file, session state, the search-result cache (which holds the
  matched messages) and, when you opt in with `index.enabled: true`, a local
  index that **stores message keys, values and headers** of the topics you
  search. Treat the directory as holding a copy of that data.
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

- **There is no per-request authentication.** The gRPC listener
  (`--listen`, default `:9500`) is plaintext and accepts every caller. **The
  network is the boundary**: expose it only on a private network, and put a
  TLS-terminating load balancer in front if clients are to use the default
  (verified TLS) dial. Anyone who can reach the port can search every topic
  the daemon follows, whatever their own Kafka permissions are.
- **The `--allow` topic allowlist is the authorization boundary.** It is
  required and default-deny: a topic is followed only if it matches one of the
  glob patterns, and a search for any other topic is answered with no
  coverage and never starts a follow. A search for an allowed topic *does*
  start one. Keep the allowlist no wider than what you would let every client
  read, and give the daemon's Kafka principal no more than the allowlist
  needs.
- **It persists payloads to disk.** Followed topics' keys, values and headers
  are written, as the consumer received them (after TLS, so decrypted), to an
  index under `--data-dir`. Protect that volume as you would the topics, and
  bound it with `--topic-max-bytes` and `--max-total-bytes`.
- Its Kafka credentials and TLS options are the CLI's (above), including
  `--insecure`.

### `k4a-mcp`, the networked MCP server

`k4a-mcp` exposes `search` and `coverage` over streamable HTTP
(`--listen`, default `:9501`) and forwards them to `k4a-index` over
**plaintext** gRPC. It holds no Kafka credentials of its own, has **no
per-request authentication**, and serves anyone who can reach it whatever
the index will answer. The same network rule applies: a private network
only.

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
- a password, SCRAM credential, AWS credential or private key written to a
  log, the state directory, the search cache or the index
- a TLS connection that skips verification without `--insecure`, or SCRAM
  sent without TLS
- files under `~/.k4a` created readable by other users
- `k4a upgrade` installing an archive that does not match `checksums.txt`
- a crash or unbounded resource use in `k4a-index` or `k4a-mcp` caused by a
  request

Out of scope: anything that follows from exposing `k4a-index` or `k4a-mcp`
to a network you do not trust, from running with `--insecure`, or from an
allowlist wider than you meant.
