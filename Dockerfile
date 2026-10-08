# k4a's two daemons, one image each, from one Dockerfile:
#   target `index` -> k4a-index, the shared Kafka search index (gRPC, 9500)
#   target `mcp`   -> k4a-mcp, an MCP server in front of it (HTTP, 9501)
# The CLI/TUI ships as release archives, not an image.
#
# Base images are pinned by digest and pulled from AWS's public mirror of
# Docker Hub (no anonymous rate limits). The build cross-compiles rather than
# emulating the target.
FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:1.26-alpine@sha256:8ac98ca534ac3f51e1f420a1dd2c15e74c75cfa0f23f3ad27eb5d7236c349a0c AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY . .
# Cache mounts rather than `go mod download`, which would fetch the tooling too.
# bleve's scorch backend is pure Go, so CGO stays off.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    export CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH && \
    go build -trimpath -ldflags "-s -w -X github.com/blairham/k4a/internal/version.Version=${VERSION}" -o /out/k4a-index ./cmd/k4a-index && \
    go build -trimpath -ldflags "-s -w -X github.com/blairham/k4a/internal/version.Version=${VERSION}" -o /out/k4a-mcp ./cmd/k4a-mcp

# The distroless `nonroot` variant runs as uid 65532 and carries system CAs,
# which the TLS handshake with the brokers needs.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS runtime
LABEL org.opencontainers.image.source="https://github.com/blairham/k4a" \
      org.opencontainers.image.licenses="Apache-2.0"
USER 65532:65532

FROM runtime AS index-runtime
LABEL org.opencontainers.image.title="k4a-index" \
      org.opencontainers.image.description="Shared Kafka search index daemon for k4a"
# gRPC: Search, Follow, Coverage. No authentication; see SECURITY.md.
EXPOSE 9500
ENTRYPOINT ["/usr/local/bin/k4a-index"]

FROM runtime AS mcp-runtime
LABEL org.opencontainers.image.title="k4a-mcp" \
      org.opencontainers.image.description="Model Context Protocol server in front of k4a-index"
# MCP streamable HTTP. A gRPC client of k4a-index; no Kafka or AWS access.
EXPOSE 9501
ENTRYPOINT ["/usr/local/bin/k4a-mcp"]

# The published images: GoReleaser's prebuilt <os>/<arch> binaries, the same
# builds as the release.
FROM index-runtime AS index-release
ARG TARGETOS TARGETARCH
COPY ${TARGETOS}/${TARGETARCH}/k4a-index /usr/local/bin/k4a-index

FROM mcp-runtime AS mcp-release
ARG TARGETOS TARGETARCH
COPY ${TARGETOS}/${TARGETARCH}/k4a-mcp /usr/local/bin/k4a-mcp

# Built from source: `docker build --target index .` / `--target mcp`.
FROM index-runtime AS index
COPY --from=build /out/k4a-index /usr/local/bin/k4a-index

FROM mcp-runtime AS mcp
COPY --from=build /out/k4a-mcp /usr/local/bin/k4a-mcp
