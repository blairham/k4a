# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Pre-stable releases (`v0.x.y`) make no API-stability promise -- breaking changes can land in any `v0.x` bump.

## [Unreleased]

### Changed

- `k4a upgrade` no longer depends on go-selfupdate: it finds, verifies and
  installs the release itself, so the build no longer links
  `golang.org/x/crypto/openpgp` (GO-2026-5932) and the go-vulncheck commit
  hook is back on (#17).

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
