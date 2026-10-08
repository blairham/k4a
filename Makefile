SHELL := /bin/bash

BINARY := k4a
MODULE := github.com/blairham/k4a
INSTALL_DIR := $(HOME)/.local/bin

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -X $(MODULE)/internal/version.Version=$(VERSION) -X $(MODULE)/internal/version.Commit=$(COMMIT) -X $(MODULE)/internal/version.Date=$(DATE)

# Proto/gRPC codegen runs pinned versions outside go.mod, so only
# google.golang.org/grpc and .../protobuf (which the generated code imports)
# enter the module graph. buf carries its own compiler; the two protoc-gen-*
# plugins are installed into ./bin and put on PATH for it.
BUF_VERSION                ?= v1.47.0
PROTOC_GEN_GO_VERSION      ?= v1.36.11
PROTOC_GEN_GO_GRPC_VERSION ?= v1.5.1
TOOLBIN                    := $(CURDIR)/bin
GENERATED_PROTO            := internal/indexwire/indexv1

IMG_INDEX ?= k4a-index:dev
IMG_MCP   ?= k4a-mcp:dev

.DEFAULT_GOAL := help

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_\/-]+:.*?## / {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: proto
proto: ## Regenerate gRPC code from proto/ (buf + protoc-gen-go/-grpc, all pinned)
	@mkdir -p $(TOOLBIN)
	GOFLAGS=-mod=mod GOBIN=$(TOOLBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOFLAGS=-mod=mod GOBIN=$(TOOLBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	PATH="$(TOOLBIN):$$PATH" GOFLAGS=-mod=mod go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate

.PHONY: check-proto
check-proto: proto ## Fail if the generated gRPC code is out of date
	@git diff --exit-code -- $(GENERATED_PROTO) || \
		{ echo "generated gRPC code is stale -- run 'make proto' and commit"; exit 1; }

.PHONY: build
build: ## Build k4a, k4a-index and k4a-mcp into dist/
	go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY) ./cmd/k4a
	go build -ldflags "$(LDFLAGS)" -o dist/k4a-index ./cmd/k4a-index
	go build -ldflags "$(LDFLAGS)" -o dist/k4a-mcp ./cmd/k4a-mcp

.PHONY: install
install: ## Build k4a and install it to ~/.local/bin
	go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY) ./cmd/k4a
	@mkdir -p $(INSTALL_DIR)
	cp dist/$(BINARY) $(INSTALL_DIR)/$(BINARY)
	@command -v codesign >/dev/null 2>&1 && codesign -s - $(INSTALL_DIR)/$(BINARY) || true
	@echo "Installed: $(INSTALL_DIR)/$(BINARY)"

.PHONY: test
test: ## Unit tests with the race detector. No Kafka needed.
	go test -race ./...

.PHONY: fuzz
fuzz: ## Run each fuzz target for FUZZTIME (default 30s)
	go test ./internal/index -run '^$$' -fuzz '^FuzzIndexablePattern$$' -fuzztime $(or $(FUZZTIME),30s)
	go test ./internal/timebound -run '^$$' -fuzz '^FuzzParseDuration$$' -fuzztime $(or $(FUZZTIME),30s)
	go test ./internal/kafka -run '^$$' -fuzz '^FuzzParseTopicSchema$$' -fuzztime $(or $(FUZZTIME),30s)

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: fmt
fmt: ## Format with gofumpt (the commit hook also runs the configured formatters)
	go tool gofumpt -w .

.PHONY: tidy
tidy: ## Tidy go.mod/go.sum
	go mod tidy

# The k4a-index chart refuses to render without brokers and an allowlist;
# the last line proves that guard still fires.
.PHONY: helm-lint
helm-lint: ## Lint and render both charts
	helm lint charts/k4a-index charts/k4a-mcp
	helm template k4a-index charts/k4a-index --set kafka.brokers=b-1.example:9098 --set config.allow='example.*' >/dev/null
	helm template k4a-mcp charts/k4a-mcp >/dev/null
	@if helm template k4a-index charts/k4a-index --set kafka.brokers=b-1.example:9098 >/dev/null 2>&1; then \
		echo "charts/k4a-index rendered without config.allow -- the default-deny guard is gone"; exit 1; fi

.PHONY: docker-build
docker-build: ## Build both images from source
	docker build --target index -t $(IMG_INDEX) .
	docker build --target mcp -t $(IMG_MCP) .

# There is no lint target: golangci-lint runs as a pre-commit hook and in CI.
.PHONY: check
check: vet test check-proto helm-lint ## What CI's Build and test and Helm chart jobs run

.PHONY: clean
clean: ## Clean build artifacts
	rm -rf dist/ $(TOOLBIN)
