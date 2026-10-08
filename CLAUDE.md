# CLAUDE.md

@AGENTS.md

<!-- AGENTS.md is the cross-tool source of truth; durable project context goes there, not here. -->

## Claude Code-specific notes

- **Slash commands** (`.claude/commands/`): `/check` (build, vet, format check, proto check, race tests — the pre-push sanity pass; never lint) and `/release-tag <vX.Y.Z>` (CHANGELOG and both charts bumped through a Release PR, then a signed tag).
- **Subagents** (`.claude/agents/`): `design-doc-guard` (which design doc gates a change; read-only), `kafka-tui-builder` (new views and `kafka.Client` operations), `index-daemon-reviewer` (index/daemon honesty contracts and proto discipline; read-only), `auth-surface-reviewer` (TLS, credentials and secrets handling against SECURITY.md; read-only).
- `.claude/settings.json` allows builds, vet, narrowed tests, `make proto` / `check-proto` / `helm-lint` and read-only git. It deliberately does not allow golangci-lint or `make check`: lint is the commit hook's job and `make check` is CI's.
- Workflow (worktrees, branches, commits) follows `~/Developer/github.com/blairham/AGENTS.md`.
- After changing `proto/indexv1/index.proto`, run `make proto` and commit the regenerated `internal/indexwire/indexv1/*.pb.go` with it.
