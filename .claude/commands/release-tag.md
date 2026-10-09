---
description: Cut a v0.0.x release tag (the tag publishes the k4a archives, both images, both charts and signed evidence).
argument-hint: "<version, e.g. v0.0.0>"
allowed-tools: Bash(make:*), Bash(go test:*), Bash(go vet:*), Bash(go build:*), Read, Edit, Glob, Grep
---

Cut a k4a release. Pushing a `v*` tag runs `.github/workflows/release.yml`
(blairham/.github's `go-release.yml`), which publishes the `k4a` archives, the `k4a` formula in
`blairham/homebrew-tap`, the multi-arch images `ghcr.io/blairham/k4a-index`
and `ghcr.io/blairham/k4a-mcp`, a cosign-signed `checksums.txt`, signed
images and SLSA provenance, with the tag's CHANGELOG section as the notes. It
fails if that section is missing, or if either chart's `appVersion` does not
match the tag. The same tag runs `chart.yml`, which publishes both charts.
Target version: `$1` (must be `v0.0.x`, pre-stable; the first tag is
`v0.0.0`).

Steps:
1. Confirm the working tree is clean and we're on `main`. If dirty, stop and
   report.
2. Check CI on the commit you will tag (`gh run list --branch main -L 3`) --
   the tag must point at a green commit. Do not run `make check` locally; CI
   ran it. If CI is red, stop.
3. Verify `$1` is a valid, monotonically-increasing `v0.0.x` tag -- check
   `git tag --list 'v0.0.*'` and `git describe --tags --abbrev=0`.
4. In one change: move CHANGELOG.md's Unreleased entries under
   `## [X.Y.Z] - YYYY-MM-DD` (no `v`; the workflow matches that exact shape)
   and leave a fresh Unreleased section; set `version` and `appVersion` in
   both `charts/k4a-index/Chart.yaml` and `charts/k4a-mcp/Chart.yaml` to
   `X.Y.Z`.
5. STOP and show the user the diff and the exact commands before anything
   that mutates the remote. `main` is protected, so the change lands as a
   `Release $1` PR (squash-merged once CI is green); then
   `git tag -s $1 -m "$1: <summary>" <merge commit>` and
   `git push origin $1`. Do not push tags without explicit confirmation.
6. Watch the Release workflow, and check the release carries the `k4a_*`
   archives, `checksums.txt`, `checksums.txt.sigstore.json` and
   `k4a-$1.intoto.jsonl`; that `ghcr.io/blairham/k4a-index:X.Y.Z` and
   `ghcr.io/blairham/k4a-mcp:X.Y.Z` are signed (SECURITY.md); and that
   `blairham/homebrew-tap` has the new `Formula/k4a.rb`.
7. Watch the Publish charts workflow (`chart.yml`, same tag) and check that
   `ghcr.io/blairham/charts/k4a-index:X.Y.Z` and
   `ghcr.io/blairham/charts/k4a-mcp:X.Y.Z` exist and are signed
   (SECURITY.md). If it failed, rerun it alone with
   `gh workflow run chart.yml -f tag=$1`.
