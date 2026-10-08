---
description: Build, vet, format-check and test k4a.
allowed-tools: Bash(go build:*), Bash(go vet:*), Bash(go tool gofumpt:*), Bash(go test:*), Bash(make check-proto)
---

Run each step and report the first failure with its full output. Do not
summarize a failure — paste it.

```sh
go build ./...
go vet ./...
go tool gofumpt -l .   # must print nothing
make check-proto       # generated gRPC code is current
go test -race ./... -count=1
```

Never run golangci-lint by hand, directly or via `pre-commit run --all-files`:
it runs as the pre-commit hook on every commit and in CI.
