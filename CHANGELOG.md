# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-stable releases (`v0.x.y`) make no API-stability promise -- breaking changes can land in any `v0.x` bump.

## [0.1.1](https://github.com/blairham/k4a/compare/v0.1.0...v0.1.1) (2026-10-24)


### Fixed

* scan matches the displayed text, so it agrees with the index on hex: payloads ([#150](https://github.com/blairham/k4a/issues/150)) ([6286d9b](https://github.com/blairham/k4a/commit/6286d9b62501e4f063d2e279221bce1a130c0a7c))

## [0.1.0](https://github.com/blairham/k4a/compare/v0.0.3...v0.1.0) (2026-10-24)


### Added

* **auth:** OAUTHBEARER interactive OIDC login by device code or in the browser (PKCE, loopback redirect), with the token cached in the OS keychain and refreshed; `k4a login` (`--force`, `--logout`); SASL extensions (`--oauth-extensions`) and an authorization ID (`--oauth-authzid`) for any token source, also in `k4a-index` and its chart ([#121](https://github.com/blairham/k4a/issues/121)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* context secrets in the OS keychain: `k4a set-credential CONTEXT` stores the password (or `--field oauth-token` / `oauth-client-secret`) in the macOS Keychain, Secret Service or Windows Credential Manager and replaces it in the config with a `keychain:` reference; `delete-credential` removes it; a context with a username and no password is asked for one, on a terminal or in the TUI's masked prompt ([#119](https://github.com/blairham/k4a/issues/119)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **index:** field search from the index (Tier 5): a JSON-path equality or its `field:value` shorthand (`--path orderId:42`, `path:orderId:42` in the TUI, MCP `path`) is answered by the local index and `k4a-index` instead of always scanning ([#116](https://github.com/blairham/k4a/issues/116)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **k4a-index:** keep the follow-set in a compacted Kafka topic (`--followset-topic`, chart `config.followSetTopic`), so a new pod re-follows what the deployment followed and the replicas of a set follow what any one of them starts; off by default, it needs read and write on that topic (and create, unless it exists compacted) ([#123](https://github.com/blairham/k4a/issues/123)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **k4a-index:** per-caller authorization for `k4a-index` and `k4a-mcp`: `--authz-policy` maps client certificates (CN, SAN or subject) and per-caller bearer tokens to the topic globs each may search and follow, default-deny once configured, `listener.authz` in both charts; without a policy nothing changes ([#118](https://github.com/blairham/k4a/issues/118)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **search:** `k4a search --archive` searches a topic's S3 archive with Athena (per-context `archive.topics`): requires `--since`, prunes by time partition, asks above `archive.confirm-bytes`, refuses and stops above `archive.max-bytes`; `--explain` prints the SQL ([#117](https://github.com/blairham/k4a/issues/117)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **search:** cross-topic search: `k4a search -t 'orders.*'` and `:search orders.*` search every topic a glob selects, with coverage per topic; `--format summary` emits one summary per topic ([#114](https://github.com/blairham/k4a/issues/114)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **search:** with a schema registry configured, search, the local index and `k4a-index` match a framed Avro, Protobuf or JSON-Schema key or value as its decoded JSON (`--schema-registry-*`, `--decode raw` for bytes, chart `schemaRegistry`); an index serves a search only when it decoded with the same registry (`Coverage.decoder`). The index schema is now version 4, so indexes rebuild once; upgrade clients before giving `k4a-index` a registry ([#115](https://github.com/blairham/k4a/issues/115)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))
* **tui:** background search: a scan keeps running when you leave the results, with progress in the status bar; `:results` brings it back, `x` or `:search-stop` stops it and keeps what it found, marked partial ([#114](https://github.com/blairham/k4a/issues/114)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))


### Fixed

* **tui:** starting a second search no longer leaves the first scanning unseen, and its late matches no longer leak into the new results ([#114](https://github.com/blairham/k4a/issues/114)) ([2d4d06e](https://github.com/blairham/k4a/commit/2d4d06e75f2d1c90b0a6e259657f5944b94b5e92))

## [0.0.3] - 2026-10-15

### Breaking

- The Helm charts now name resources `<release>-<chart>`. A release named
  `k4a-index` or `k4a-mcp` keeps its names; any other release is renamed
  unless `fullnameOverride` is set, and `serviceAccount.name` follows, which
  can break an existing EKS Pod Identity association (#66).
- Without `--auth`, the method is inferred from what was given (credentials
  mean `scram`, a cert or key means `mtls`, OAuth settings mean
  `oauthbearer`, nothing means `plaintext`) instead of always `scram`, in the
  CLI, the TUI and `k4a-index` (#69, #99).
- `--auth mtls` refuses `--insecure` instead of silently skipping server
  verification, as `docs/design/auth.md` always said (#98).
- `$VAR` in a password, a path or a flag is expanded only when it is the whole
  value (`$VAR` or `${VAR}`), so a literal `$` is no longer mangled (#71).
- `ui.readonly` now blocks the mutating headless commands too, and
  `delete-topic` asks for confirmation (`--yes` to skip; it refuses rather
  than hangs without a terminal) (#88).

### Added

- Authentication: SCRAM-SHA-256, SASL/PLAIN and OAUTHBEARER (static token,
  token file, or client credentials with refresh), and SASL without TLS as an
  explicit `--no-tls` opt-in that warns (#78, #82).
- Confluent Schema Registry decoding of Avro, Protobuf and JSON Schema keys
  and values in Messages, Message Detail and `k4a consume --decode`, with a
  per-context `schema-registry:` block; `x` toggles raw and decoded. Schema IDs
  in Confluent 8 record headers, `google/type` imports and `Any` fields are
  resolved (#86, #92, #129).
- Headless commands for what the TUI could already do: `list-topics`,
  `list-groups`, `describe-group`, `describe-broker-config`, `reset-offsets`,
  `delete-offsets`, `delete-group`, `delete-acl`, `add-partitions`,
  `purge-topic` (#101, #106, #108).
- `k4a produce --value`, `--record-key`, `-H k=v` and `--stdin`, and Value,
  Key and Headers fields in the TUI Produce form (#110).
- Seeking: `k4a consume --from-beginning | --offset N | --since`, and `t` in
  the Messages view; `consume --no-group` reads every partition, or one with
  `--partition` (#85, #126, #127).
- `k4a search --path` and `path:` in the TUI: a jq-style JSON-path predicate,
  always answered by a scan (#125).
- Copy a message's value, key or record (`c`/`K`/`C`) over OSC 52 or a native
  clipboard tool (`ui.clipboard`), export the visible messages or search
  results to JSONL (`e`), and step through messages in Message Detail
  (`n`/`N`) (#100, #128).
- TUI administration: edit broker configs, edit an ACL binding, a client
  quotas view (`:quotas`), partition reassignment from Topic Detail (`a`), and
  re-run/edit/filter in the Search view (#96, #105, #107, #109, #111).
- `k4a index status` / `k4a index clear`, and `k4a state export` /
  `k4a state import` (a validated tar.gz; inline secrets stripped unless
  `--include-secrets`) (#73, #124).
- `k4a-index` and `k4a-mcp`: optional TLS, mTLS and bearer-token auth on
  their listeners, a server-side `--max-search-limit`, Prometheus metrics and
  `/healthz` / `/readyz` on `--admin-listen`, `grpc.health.v1`, and
  `--shutdown-timeout` (#77, #84, #90, #93).
- `k4a-index`: bounded backfill when it starts following a topic, catch-up
  after a restart, and a follow-set that survives a container restart (#95).
- Helm charts: Kafka credentials and TLS from existing Secrets, OAuth and
  `noTLS`, log level, a hardened pod and container security context, and
  optional NetworkPolicy, PodDisruptionBudget and ServiceMonitor (#66, #82,
  #90, #93).

### Changed

- The TUI's hint bars and `?` help are generated per view from one key table,
  so they list only keys that work there (#83, #94).
- `k4a-mcp` serves MCP statelessly, so replicas need no sticky sessions
  (#61).
- The on-disk search cache is capped at 256 MiB and drops entries unused for
  7 days (#68).
- Search patterns are logged at debug, not info, by `k4a-index` (#84).

### Fixed

- Form fields in the TUI take keys that are also global shortcuts, so
  `orders-1` can be typed (#65).
- Every connection flag (`--username`, `--password`, `--cert`, `--key`,
  `--ca`, `-k`, `--region`) overrides the selected context, in the CLI and in
  `k4a-index` (#63, #80).
- An AWS profile's own `region`, and `AWS_REGION`, are used instead of
  `us-east-1` (#104).
- `k4a upgrade` refuses unknown flags instead of installing (#62).
- Out-of-range partition counts, replication factors and partition IDs are
  refused instead of wrapping (#67).
- `:upgrade` restarts the TUI with the original command line (#74).
- `:produce` matches only the whole word and keeps the topic's case;
  `:search` uses the topic on screen; errors the TUI swallowed are shown
  (#76, #79, #87).
- The ACLs tab (`4`) showed an empty box; it now has its view (#109).
- `enter` in a filtered Messages view opened the wrong message (#100).
- Headless commands close their Kafka client; `produce` reports what was
  actually sent after Ctrl+C (#72, #81).
- `k4a-index`: illegal topic names (including `.` and `..`) are refused before
  they become index paths; a tail that dies leaves the follow-set; a Coverage
  probe no longer keeps a topic from being evicted; shutdown and eviction
  races are closed; byte sizes that overflow are refused (#64, #70, #75, #77,
  #89).

## [0.0.2] - 2026-10-09

### Fixed

- `ctrl-d` on the messages view now opens the purge prompt its hint bar
  advertises; it used to fall through to navigation and do nothing (#24).

### Changed

- `k4a upgrade` no longer depends on go-selfupdate: it finds, verifies and
  installs the release itself, so the build no longer links
  `golang.org/x/crypto/openpgp` (GO-2026-5932) and the go-vulncheck commit
  hook is back on (#17).
- Releases, the images and the charts are now built, signed and attested by
  the shared workflows in [blairham/.github](https://github.com/blairham/.github)
  (`go-release.yml`, `go-chart.yml`), so the keyless signing identity is
  that workflow rather than this repository's own; SECURITY.md has the new
  verify commands and how to verify 0.0.1 and earlier.
- `k4a upgrade` looks up a `gh` token under the upgrade's own context, so
  canceling an upgrade no longer waits out the token lookup.

### Security

- Built with Go 1.26.9 (GO-2026-6603 to 6617 in `net/http` and
  `crypto/tls`).

## [0.0.1] - 2026-10-08

### Added

- The README documents installing with `brew install blairham/tap/k4a`.

### Changed

- Dependency updates: aws-sdk-go-v2, franz-go (and kadm/kmsg), bubbletea,
  tuikit, gRPC, bleve, the MCP Go SDK, go-selfupdate and smithy-go.

### Fixed

- `k4a upgrade` and `:upgrade` no longer overwrite a Homebrew-installed
  binary; they point at `brew upgrade blairham/tap/k4a` instead, and
  `k4a upgrade --check` names that command (#12).

## [0.0.0] - 2026-10-08

The first open-source release of k4a, under the Apache License 2.0.

### Added

- `k4a`, a terminal UI for Kafka in the style of k9s: topics, consumer
  groups, brokers, ACLs and their configuration; a live message tail with a
  message detail view; producing; creating and deleting topics and ACLs;
  growing partitions and changing replication; resetting group offsets; a
  regex filter on every list and a `:` command mode. `--readonly` blocks the
  mutating actions.
- Headless subcommands for the same operations (`produce`, `consume`,
  `create-topic`, `describe-topic`, `topic-size`, `alter-config`,
  `alter-replication`, `list-acls`, `create-acl`, `delete-topic`, `verify`),
  and `search` over a whole topic with key, value and header scopes, time
  bounds and partition filters.
- Connections over plaintext, TLS, SASL/SCRAM-SHA-512, mTLS and MSK IAM.
  AWS profiles (SSO, `credential_process`) resolve in-process through the
  AWS SDK; broker lists can come from SSM parameters.
- Named contexts in `~/.k4a/config.yaml`, kubeconfig-style, with `contexts`,
  `use-context` and `config` subcommands.
- An opt-in local index (`index.enabled`) that serves repeated searches
  without rescanning, and an on-disk cache of search results. A search the
  index cannot answer with full recall falls back to a scan.
- `k4a-index`, a shared index daemon that tails an allowlisted set of topics
  and answers searches over gRPC, reporting its coverage so a client scans
  whatever it does not hold; and `k4a-mcp`, a Model Context Protocol server
  in front of it. `k4a mcp` serves the same search tool over stdio.
- Helm charts for `k4a-index` and `k4a-mcp`, and signed multi-arch images
  `ghcr.io/blairham/k4a-index` and `ghcr.io/blairham/k4a-mcp`.
- `k4a upgrade`, which replaces the binary with the latest release after
  checking it against the release checksums.
