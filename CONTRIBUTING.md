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
- Every user-visible change gets a line under `[Unreleased]` in
  [`CHANGELOG.md`](CHANGELOG.md); a release's notes are that section.
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
