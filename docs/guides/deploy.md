# Deploying and operating `k4a-index` / `k4a-mcp`

The operator-facing companion to
[`design/shared-index-service.md`](../design/shared-index-service.md) (why the
daemon is shaped the way it is) and
[`guides/mcp-setup.md`](mcp-setup.md) (how a *client* connects to it).
This page is about the deployed thing: what the Helm charts install, what to
change when you want the daemon to follow another topic, and what the
daemon's runtime knobs are.

Both services are optional. `k4a` works with no daemon at all; the daemon only
makes searches over the topics it follows faster.

## What gets deployed

| | `k4a-index` | `k4a-mcp` |
|---|---|---|
| Chart | `charts/k4a-index` | `charts/k4a-mcp` |
| Image | Dockerfile target `index` | Dockerfile target `mcp` |
| Port | 9500, gRPC | 9501, MCP streamable-HTTP |
| State | emptyDir Bleve index (disposable) | none — stateless proxy |
| Kafka access | yes (reads followed topics) | none — talks only to `k4a-index` |

The CLI/TUI binary is **not** containerized — it ships via GoReleaser on a
version tag. One repo-root `Dockerfile` produces both daemon images from a
shared compile stage:

```sh
docker build --target index -t <registry>/k4a-index:<tag> .
docker build --target mcp   -t <registry>/k4a-mcp:<tag>   .
```

Both images are distroless `nonroot`; the charts run them as UID 65532.

## Installing

Each chart has a single `values.yaml` holding the defaults; supply your
environment's values with `--set` or your own `-f` file. A minimal install
into a `k4a` namespace:

```sh
helm install k4a-index oci://ghcr.io/blairham/charts/k4a-index --version <version> -n k4a --create-namespace \
  --set image.repository=<registry>/k4a-index,image.tag=<tag> \
  --set kafka.brokers=<bootstrap-servers> \
  --set kafka.auth=iam,aws.region=us-east-1 \
  --set config.allow='example.ingest.*'

helm install k4a-mcp oci://ghcr.io/blairham/charts/k4a-mcp --version <version> -n k4a \
  --set image.repository=<registry>/k4a-mcp,image.tag=<tag>
```

The charts are published, signed, to `oci://ghcr.io/blairham/charts` on every
release (see SECURITY.md to verify them); from a checkout, `charts/k4a-index`
and `charts/k4a-mcp` work in place of the `oci://` references.

`kafka.brokers` and `config.allow` are **required** — the `k4a-index` chart
refuses to render without them. `k4a-mcp`'s default `config.indexAddr`
(`k4a-index:9500`) already points at the in-cluster `k4a-index` Service when
both are installed in the same namespace.

### Kafka connection values

| Value | Env | Notes |
|---|---|---|
| `kafka.brokers` | `KAFKA_BROKERS` | Bootstrap servers, comma-separated. Required. |
| `kafka.auth` | `KAFKA_AUTH` | `plaintext`, `tls`, `scram`, `mtls`, `iam`. Default `iam`. |
| `aws.region` | `AWS_REGION` | Region for IAM auth. Rendered only when set; the daemon defaults to `us-east-1`. |
| `serviceAccount.name` / `.annotations` | — | The pod's identity. For `iam` on EKS, bind it to an IAM role with EKS Pod Identity, or annotate it with the role ARN for IRSA. |

With `iam` the daemon picks up the pod's AWS credentials, so no SASL secret is
involved. Other auth methods read `KAFKA_USERNAME` / `KAFKA_PASSWORD` /
`KAFKA_TLS_*` (see the table below); the chart does not template those, so add
them to the deployment (ideally from a Secret) if you need them.

## Adding a topic to the follow set

The follow surface is gated **twice**, and both gates must be widened or
nothing changes:

1. **Kafka authorization (outer gate)** — whatever grants the daemon's
   identity read access: an IAM policy on the topic set for MSK IAM auth,
   or ACLs for SCRAM/mTLS principals. The daemon physically cannot read
   anything outside this grant.
2. **Runtime allowlist (inner gate)** — the chart's `config.allow`, which
   becomes `K4A_INDEX_ALLOW`. Glob patterns. The daemon refuses to
   follow — and therefore refuses to persist — anything that doesn't
   match, even if the outer grant were wider.

Adding a topic to only one of them is a silent no-op: widen the grant alone
and the daemon still won't follow it; widen the allowlist alone and the tail
fails on authorization. Watch for asymmetry between the two: if
`config.allow` uses a glob like `quotes-*-book` but the IAM policy
enumerates exact topic names, a *new* topic matching the glob is allowed by
the daemon but not followable until its exact name is added to the grant.

**Do not add sensitive topics.** Orders, fills, and PII-bearing topics stay
off both lists until per-caller authorization (Phase 5) lands. Reaching the
daemon grants broad read over everything it follows, and the index persists
decrypted payloads to disk.

Two more things worth checking when you widen the set:

- `config.topicMaxBytes` (chart default `512MiB`, daemon default `1GiB`)
  bounds each topic's index. Total disk ≈ concurrently-hot topics × this
  (×~3 on disk while scorch holds unreclaimed segments), and it has to stay
  under `storage.sizeLimit` (10Gi) and the pod's `ephemeral-storage` limit
  (12Gi). A high-volume topic is safe *because* of this cap — without it a
  fat topic fills the emptyDir and evicts the pod, taking every other
  topic's index with it. `config.maxTotalBytes` (`7GiB`) is the backstop
  for the whole index root.
- `config.prefollow` pins topics that should be warm before the first
  search (pinned topics are exempt from LRU eviction). Every pinned topic
  must also match `config.allow`, or the daemon exits at startup rather
  than silently never following it.

## Daemon runtime configuration

`cmd/k4a-index/main.go`. Every flag has an env equivalent; the chart sets
these via env vars, so in-cluster the env column is what you actually edit.

| Flag | Env | Default | What it does |
|---|---|---|---|
| `--allow` | `K4A_INDEX_ALLOW` | *(none — **required**)* | Glob allowlist of topics the daemon may follow. Default-deny; an empty list is a startup error. Keep it in step with the daemon's Kafka read grant. |
| `-t`, `--topic` | `K4A_INDEX_TOPIC` | *(empty)* | Topics to pre-follow (warm + pinned) at startup. Repeatable / comma-separated. Each must match `--allow`. |
| `--max-followed` | `K4A_INDEX_MAX_FOLLOWED` | `256` | Hard cap on concurrently-followed topics; at the cap a new follow evicts the coldest. |
| `--evict-after` | `K4A_INDEX_EVICT_AFTER` | `168h` | Drop a topic not searched within this idle window (LRU). Pinned topics are exempt. |
| `--topic-max-bytes` | `K4A_INDEX_TOPIC_MAX_BYTES` | `1GiB` | Per-topic index byte cap; oldest records are trimmed inline once exceeded. `0` disables. |
| `--max-total-bytes` | `K4A_INDEX_MAX_TOTAL_BYTES` | `0` | Disk budget for the whole index root, measured on disk. Over budget the daemon deletes orphaned index dirs, then evicts the coldest non-pinned topics. Set it below the volume's size limit. `0` disables. |
| `--drain-stall-timeout` | `K4A_INDEX_DRAIN_STALL_TIMEOUT` | `5m` | If a topic's indexing drain makes no progress while records are queued, dump all goroutine stacks to stderr and exit. `0` disables. |
| `--drain-stall-merge-grace` | `K4A_INDEX_DRAIN_STALL_MERGE_GRACE` | `30m` | Tolerate a stall this long while a scorch segment merge is in flight, so the watchdog doesn't kill a working merge and livelock on restart. `0` disables the grace. |
| `--drain-stall-wipe-after` | `K4A_INDEX_DRAIN_STALL_WIPE_AFTER` | `3` | On the Nth consecutive stall exit for the same topic index, quarantine that index so the next boot re-warms fresh. `0` disables. |
| `-l`, `--listen` | `K4A_INDEX_LISTEN` | `:9500` | gRPC listen address. |
| `--data-dir` | `K4A_INDEX_DATA_DIR` | k4a's `~/.k4a` | Index storage root. |
| `--log-level` | `K4A_INDEX_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

Connection flags mirror the headless CLI (`--context`/`--config`, or
`-b/--brokers` + `--auth`/`--region`/`--profile`/TLS files), with env
equivalents `KAFKA_BROKERS`, `KAFKA_AUTH`, `AWS_REGION`, `AWS_PROFILE`,
`KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TLS_CA`, `KAFKA_TLS_CERT`,
`KAFKA_TLS_KEY`.

The chart wires `KAFKA_BROKERS` (`kafka.brokers`), `KAFKA_AUTH`
(`kafka.auth`), `AWS_REGION` (`aws.region`, when set), `K4A_INDEX_ALLOW`,
`K4A_INDEX_TOPIC`, `K4A_INDEX_MAX_FOLLOWED`, `K4A_INDEX_EVICT_AFTER`,
`K4A_INDEX_TOPIC_MAX_BYTES`, `K4A_INDEX_MAX_TOTAL_BYTES`, the three
`K4A_INDEX_DRAIN_STALL_*` values (`config.drainStall.*`),
`K4A_INDEX_LISTEN` (from `service.port`), and `K4A_INDEX_DATA_DIR`
(`/data`). **`K4A_INDEX_LOG_LEVEL` is not templated** — it runs at its
default; add it to the deployment template if you need to change it.

`k4a-mcp` is much smaller: `--index-addr` / `K4A_MCP_INDEX_ADDR` (default
`k4a-index:9500`, the in-cluster ClusterIP hop — plaintext on purpose, the
TLS edge is for external clients), `--listen` / `K4A_MCP_LISTEN` (`:9501`),
`--path` / `K4A_MCP_PATH` (`/mcp`), `--log-level` / `K4A_MCP_LOG_LEVEL`.

## Networking

Both charts create a `ClusterIP` Service only. To reach them from developer
machines, put an **internal** LoadBalancer or Ingress in front, reachable over
your private network / VPN, with DNS such as `k4a-index.example.com` and
`k4a-mcp.example.com`:

- **k4a-index** serves plaintext gRPC (h2c) on 9500. Terminate TLS at the
  edge on `:443` and make sure the listener **negotiates ALPN `h2`** (on an
  AWS NLB, `alpnPolicy: HTTP2Preferred`; on an Ingress controller, its gRPC
  backend-protocol setting). Without it the TLS handshake fails and every
  client silently falls back to a broker scan.
- **k4a-mcp** serves MCP streamable-HTTP on 9501 at `/mcp`, plus a plain
  `/health`. Terminate TLS at the edge; the MCP client URL is then
  `https://<k4a-mcp-host>/mcp`.

If you restrict ingress with a `NetworkPolicy`, note that a load balancer
targeting pod IPs reaches the pod from addresses that are **not pods** (e.g.
the load balancer's own network interfaces). A rule that only uses
`namespaceSelector`/`podSelector` drops that source and fails target health
checks; admit the load balancer's subnets with an `ipBlock` instead. If you
also restrict egress, the daemon needs the brokers plus, for IAM auth, STS
and the node-local credential agent.

## Operating notes

- **Restarting is cheap.** Storage is an `emptyDir` on purpose: the index
  is fully reconstructable from the broker, so losing it costs about a
  minute of re-warm and nothing else. Never reach for a PVC here.
- **Silence is the failure mode, not restarts.** The drain-stall watchdog
  exists because a wedged drain serves *nothing*, silently, for as long as
  nobody notices. If the daemon exits with a goroutine dump on stderr, that
  dump is the artifact — the distroless image has no shell and no pprof,
  so it's the only root-cause evidence you'll get.
- **Coverage is honest by contract.** A client only gets an index-served
  answer for a window the daemon actually covers; anything else falls back
  to a client-side scan. So a cold, lagging, or trimmed index degrades to
  "slower", never to "wrong".
- **A dead daemon is not an outage.** Every client path has a
  "endpoint unreachable → local behavior" branch. A k4a pointed at a dead
  endpoint must be indistinguishable from a k4a with none.
