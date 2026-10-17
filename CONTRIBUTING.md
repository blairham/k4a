# Contributing

Thank you for considering a contribution. Please read the two rules below
before opening a pull request -- both are non-negotiable.

## 1. Nothing secret reaches logs, state or the index

k4a holds Kafka credentials -- SCRAM passwords, client keys, AWS sessions --
and `k4a-index` holds them on behalf of everyone who searches it. A change
that writes a credential to a log line, `~/.k4a`, the search cache or an index
will not be merged. The same goes for the daemon's boundary: `k4a-index` must
never follow, index or return a topic outside `--allow`, and a change that
widens what it follows needs a design discussion in an issue first. See
[`SECURITY.md`](SECURITY.md) for the trust model.

## 2. The Contributor License Agreement

Contributions require a signed CLA; the text is in [`CLA.md`](CLA.md).

**Why.** The project may need to offer different licensing terms in future.
That is only possible if one party can license the whole work, and copyright
in a contribution stays with its author unless licensed onward.

The CLA does **not** take your copyright. You keep it; you grant a license
broad enough to include sublicensing, and you affirm the work is your own --
including that no employer holds rights to it.

## Practical

- Open an issue before a large change, so the design can be agreed first.
  Read the owning design doc under [`docs/design/`](docs/design/) before
  changing the area it covers, and update it in the same pull request when
  your change makes it stale.
- Work on a branch and open a pull request against `main`.
- Commit messages explain the **why**, not a restatement of the diff.
  Conventional-commit prefixes (`feat:`, `fix:`, `docs:`, ...).
- Commits must be signed.
- Every hand-written `.go` file carries the two-line SPDX header; the
  pre-commit hook fails without it.
- Never hand-edit `internal/indexwire/indexv1/*.pb.go`. Change
  `proto/indexv1/index.proto`, run `make proto` and commit both; CI fails on
  stale generated code.
- **The pull request title is a [Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/)**,
  and it is the changelog entry and the version bump: the repository
  squash-merges with the title as the commit subject, and release-please
  reads the subjects. The required **PR title** check fails one that is not.

  | Title | Bump | [`CHANGELOG.md`](CHANGELOG.md) |
  |---|---|---|
  | `feat!:` / `fix!:` (any type with `!`) | minor before 1.0, then major | ⚠ Breaking Changes |
  | `feat:` | minor | Added |
  | `fix:` (a compatible security fix too) | patch | Fixed |
  | `perf:` / `revert:` | patch | Performance / Reverted |
  | `docs:` `ci:` `build:` `refactor:` `test:` `chore:` `style:` | none | hidden |

  The text after the colon is the entry, so write it for someone upgrading.
  A `BREAKING CHANGE: …` footer in the PR body also marks it breaking.
  Before 1.0 a breaking change bumps the minor version, as the changelog's
  header promises; it never makes 1.0.0 by itself.
- **Releases are release-please's.** Every merge updates one standing pull
  request, `chore(main): release X.Y.Z`, with the next version, its
  `CHANGELOG.md` section and both charts' `version` and `appVersion`. Merging
  it is the release: it tags `vX.Y.Z`, and `release-please.yml` publishes the
  archives, images and charts. Never tag by hand. `Release-As: X.Y.Z` in a PR
  body forces a version. A title that went wrong after the merge is fixed on
  the merged pull request: add the messages it should have had, one per line,
  between `BEGIN_COMMIT_OVERRIDE` and `END_COMMIT_OVERRIDE` in its body. The
  PR title check on the release pull request fails on any commit in the
  release that is neither a Conventional Commit nor overridden.
- `pre-commit install` once per checkout. The hooks format, lint and scan for
  secrets on every commit; never bypass
  them with `--no-verify`.
- `go test -race ./...` must pass, and needs no Kafka cluster. **New
  functionality comes with tests in the same pull request**, and a pull
  request that adds behavior without them will not be merged. A bug fix comes
  with the test that would have caught it. Input that arrives from outside
  the process and is parsed -- search patterns, time bounds, schema files --
  has a fuzz target.

Please also read the [Code of Conduct](CODE_OF_CONDUCT.md). Security issues go
through [SECURITY.md](SECURITY.md), never a public issue.
