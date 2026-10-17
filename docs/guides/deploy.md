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

Each image also carries a **gated `/bin/sh`** ([`cmd/sh`](../../cmd/sh),
from [`github.com/blairham/sh/gated`](https://github.com/blairham/sh/blob/main/gated/README.md)):
a shell that cannot exec, signal or write anything, and reads only `/proc` and
`/sys/fs/cgroup` (`/proc/*/environ` is refused). It is there so `kubectl exec`
has something to read the container with:

```sh
# ps, with no ps
kubectl exec <pod> -- /bin/sh -c 'for d in /proc/[0-9]*; do read -r c < "$d/comm"; echo "${d#/proc/} $c"; done'
# memory ceiling, from cgroup v2
kubectl exec <pod> -- /bin/sh -c 'read -r m < /sys/fs/cgroup/memory.max; echo $m'
```

Refusals are summarized on stderr, because an empty answer and a refused one
look the same on stdout. The gate holds only while the image has no other
userland and the pod runs no `exec` probes or lifecycle hooks; the charts use
gRPC, HTTP and TCP probes only, so keep it that way when overriding them.

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
| `kafka.auth` | `KAFKA_AUTH` | `plaintext`, `tls`, `scram` (SHA-512), `scram-sha-256`, `plain`, `oauthbearer`, `mtls`, `iam`. Default `iam`; `""` infers it from the credentials supplied. |
| `aws.region` | `AWS_REGION` | Region for IAM auth. Rendered only when set; the daemon defaults to `us-east-1`. |
| `kafka.insecure` | `--insecure` | Skip TLS certificate verification. Passed as an argument (the flag has no env). Testing only; refused with `mtls`. |
| `kafka.noTLS` | `--no-tls` | Run a SASL method without TLS (`SASL_PLAINTEXT`); the password or token crosses the network in cleartext and the daemon logs a warning. Passed as an argument. Development brokers only. |
| `kafka.credentials.existingSecret` (`.usernameKey`, `.passwordKey`) | `KAFKA_USERNAME`, `KAFKA_PASSWORD` | SASL username/password (`scram`, `scram-sha-256`, `plain`) from an existing Secret. Keys default to `username` / `password`. |
| `kafka.oauth.tokenUrl`, `.clientId`, `.scopes` | `KAFKA_OAUTH_TOKEN_URL`, `KAFKA_OAUTH_CLIENT_ID`, `KAFKA_OAUTH_SCOPES` | OAUTHBEARER client-credentials grant. The token URL must be `https`. |
| `kafka.oauth.existingSecret` (`.clientSecretKey`, `.tokenKey`) | `KAFKA_OAUTH_CLIENT_SECRET`, `KAFKA_OAUTH_TOKEN` | The client secret for the grant, or instead a static token, from an existing Secret. Set one of the two keys. |
| `kafka.tls.existingSecret` (`.caKey`, `.certKey`, `.keyKey`) | `KAFKA_TLS_CA`, `KAFKA_TLS_CERT`, `KAFKA_TLS_KEY` | Secret mounted read-only at `/etc/k4a/tls`; each non-empty key becomes the matching file path. Keys default to `ca.crt` / `tls.crt` / `tls.key`. `mtls` needs all three. |
| `serviceAccount.name` / `.annotations` | — | The pod's identity (empty name = the release fullname). For `iam` on EKS, bind it to an IAM role with EKS Pod Identity, or annotate it with the role ARN for IRSA. |

With `iam` the daemon picks up the pod's AWS credentials, so no SASL secret is
involved; the SASL methods, `tls` and `mtls` take their material from the
`existingSecret` values above, which the chart never creates.

### Release naming

Resources are named the standard Helm way: `<release>-<chart>`, or just the
release name when it already contains the chart name, so `helm install
k4a-index …` still yields `k4a-index`. Two releases of a chart in one
namespace no longer collide. `fullnameOverride` pins the name outright.
`k4a-mcp`'s default `config.indexAddr` assumes a `k4a-index` release named
`k4a-index`; otherwise point it at `<release>-k4a-index:9500`.

### Pod hardening and optional policy objects

Both charts run the container with a read-only root filesystem, no privilege
escalation, all capabilities dropped, `runAsNonRoot`, and the `RuntimeDefault`
seccomp profile (`securityContext` / `podSecurityContext` in values). The only
writable paths are emptyDirs: the index at `/data` (`k4a-index`) and `/tmp`.
`terminationGracePeriodSeconds` defaults to 30.

Both charts also carry a `NetworkPolicy` (`networkPolicy.enabled`, ingress to
the service port from `networkPolicy.ingressFrom`, egress restricted only if
`networkPolicy.egress` is set) and a `PodDisruptionBudget`
(`podDisruptionBudget.enabled`, `minAvailable` or `maxUnavailable`). Both are
off by default. At `replicaCount: 1`, a PDB with `minAvailable: 1` blocks node
drains, so raise the replica count first.

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

**Do not add sensitive topics without a per-caller policy.** Without
`listener.authz` (see [Authorizing callers](#authorizing-callers)), reaching
the daemon grants broad read over everything it follows, and the index
persists decrypted payloads to disk. Orders, fills, and PII-bearing topics
go on the lists only together with a policy that grants them to the callers
who may read them.

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
| `--followset-topic` | `K4A_INDEX_FOLLOWSET_TOPIC` | *(empty)* | Compacted topic that holds the follow-set for every pod and replica (chart: `config.followSetTopic`). Created compacted if absent. Needs read, write and describe on it, and create unless you create it. Empty keeps only the local file. |
| `--max-followed` | `K4A_INDEX_MAX_FOLLOWED` | `256` | Hard cap on concurrently-followed topics; at the cap a new follow evicts the coldest. |
| `--backfill-window` | `K4A_INDEX_BACKFILL_WINDOW` | `24h` | History a newly followed topic indexes before its live tail. `0` keeps the old start (the most recent ~100 records). |
| `--backfill-max-records` | `K4A_INDEX_BACKFILL_MAX_RECORDS` | `1000000` | Cap on that history across the topic's partitions (split evenly); also how far behind a restarted index may be and still resume. `0` = no cap. |
| `--evict-after` | `K4A_INDEX_EVICT_AFTER` | `168h` | Drop a topic not searched within this idle window (LRU). Pinned topics are exempt. |
| `--topic-max-bytes` | `K4A_INDEX_TOPIC_MAX_BYTES` | `1GiB` | Per-topic index byte cap; oldest records are trimmed inline once exceeded. `0` disables. |
| `--max-total-bytes` | `K4A_INDEX_MAX_TOTAL_BYTES` | `0` | Disk budget for the whole index root, measured on disk. Over budget the daemon deletes orphaned index dirs, then evicts the coldest non-pinned topics. Set it below the volume's size limit. `0` disables. |
| `--drain-stall-timeout` | `K4A_INDEX_DRAIN_STALL_TIMEOUT` | `5m` | If a topic's indexing drain makes no progress while records are queued, dump all goroutine stacks to stderr and exit. `0` disables. |
| `--drain-stall-merge-grace` | `K4A_INDEX_DRAIN_STALL_MERGE_GRACE` | `30m` | Tolerate a stall this long while a scorch segment merge is in flight, so the watchdog doesn't kill a working merge and livelock on restart. `0` disables the grace. |
| `--drain-stall-wipe-after` | `K4A_INDEX_DRAIN_STALL_WIPE_AFTER` | `3` | On the Nth consecutive stall exit for the same topic index, quarantine that index so the next boot re-warms fresh. `0` disables. |
| `--shutdown-timeout` | `K4A_INDEX_SHUTDOWN_TIMEOUT` | `15s` | On SIGTERM, how long in-flight RPCs may run before they are cut off, so a stalled Search cannot hold shutdown open past the pod's grace period (which would skip the final index flush). Keep it under `terminationGracePeriodSeconds` (Kubernetes default 30s). `0` stops them at once. |
| `-l`, `--listen` | `K4A_INDEX_LISTEN` | `:9500` | gRPC listen address. |
| `--tls-cert`, `--tls-key` | `K4A_INDEX_TLS_CERT`, `K4A_INDEX_TLS_KEY` | *(empty: plaintext)* | Serve gRPC over TLS. The certificate is re-read when its file changes. |
| `--tls-client-ca` | `K4A_INDEX_TLS_CLIENT_CA` | *(empty)* | Require a client certificate signed by this CA on every RPC except health checks (mTLS). Needs `--tls-cert`. |
| `--token-file` | `K4A_INDEX_TOKEN_FILE` (or the token itself in `K4A_INDEX_TOKEN`) | *(empty: no token)* | Bearer token every RPC except health checks must present. Read once at startup. |
| `--max-search-limit` | `K4A_INDEX_MAX_SEARCH_LIMIT` | `10000` | Most matches one Search returns; a larger or missing request limit is lowered to it and the answer reports capped. |
| `--admin-listen` | `K4A_INDEX_ADMIN_LISTEN` | *(empty: off; chart `:9502`)* | Plain-HTTP admin port: `/metrics` (Prometheus), `/healthz`, `/readyz`. Asks no credential; carries topic names and counters, never record content. |
| `--data-dir` | `K4A_INDEX_DATA_DIR` | k4a's `~/.k4a` | Index storage root. |
| `--log-level` | `K4A_INDEX_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

Connection flags mirror the headless CLI (`--context`/`--config`, or
`-b/--brokers` + `--auth`/`--region`/`--profile`/TLS files), with env
equivalents `KAFKA_BROKERS`, `KAFKA_AUTH`, `AWS_REGION`, `AWS_PROFILE`,
`KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TLS_CA`, `KAFKA_TLS_CERT`,
`KAFKA_TLS_KEY`, and for OAUTHBEARER `KAFKA_OAUTH_TOKEN`,
`KAFKA_OAUTH_TOKEN_FILE`, `KAFKA_OAUTH_TOKEN_URL`, `KAFKA_OAUTH_CLIENT_ID`,
`KAFKA_OAUTH_CLIENT_SECRET`, `KAFKA_OAUTH_SCOPES`. `--insecure` and
`--no-tls` have no env form. A `--brokers` connection without `--auth` infers
the method as the CLI does: credentials mean `scram`, OAuth settings
`oauthbearer`, a client cert or key `mtls`, and nothing `plaintext`. The
chart always renders `KAFKA_AUTH` (default `iam`), so set `kafka.auth: ""` to
get the inference there.

The chart wires `KAFKA_BROKERS` (`kafka.brokers`), `KAFKA_AUTH`
(`kafka.auth`), `AWS_REGION` (`aws.region`, when set), `K4A_INDEX_ALLOW`,
`K4A_INDEX_TOPIC`, `K4A_INDEX_MAX_FOLLOWED`, `K4A_INDEX_EVICT_AFTER`,
`K4A_INDEX_TOPIC_MAX_BYTES`, `K4A_INDEX_MAX_TOTAL_BYTES`, the three
`K4A_INDEX_DRAIN_STALL_*` values (`config.drainStall.*`),
`K4A_INDEX_MAX_SEARCH_LIMIT` (`config.maxSearchLimit`),
`K4A_INDEX_BACKFILL_WINDOW` / `K4A_INDEX_BACKFILL_MAX_RECORDS`
(`config.backfill.window` / `.maxRecords`), the listener's
`K4A_INDEX_TLS_*` and `K4A_INDEX_TOKEN_FILE` (`listener.*`, below),
`K4A_INDEX_LISTEN` (from `service.port`), `K4A_INDEX_DATA_DIR`
(`/data`), `K4A_INDEX_LOG_LEVEL` (`logLevel`), and the Kafka credential and
TLS values above.

`k4a-mcp` is much smaller: `--index-addr` / `K4A_MCP_INDEX_ADDR` (default
`k4a-index:9500`, the in-cluster ClusterIP hop — plaintext by default),
`--listen` / `K4A_MCP_LISTEN` (`:9501`), `--path` / `K4A_MCP_PATH` (`/mcp`),
`--log-level` / `K4A_MCP_LOG_LEVEL` (chart value `logLevel`), plus the
credential flags below.

| Flag | Env | What it does |
|---|---|---|
| `--tls-cert`, `--tls-key` | `K4A_MCP_TLS_CERT`, `K4A_MCP_TLS_KEY` | Serve HTTPS; the certificate is re-read when its file changes. |
| `--tls-client-ca` | `K4A_MCP_TLS_CLIENT_CA` | Require a client certificate from this CA on every request but `/health`. |
| `--token-file` | `K4A_MCP_TOKEN_FILE` (or `K4A_MCP_TOKEN`) | Bearer token every request but `/health` must present. |
| `--index-tls` | `K4A_MCP_INDEX_TLS` | Dial k4a-index over TLS (implied by `--index-ca`/`--index-cert`). |
| `--index-ca`, `--index-cert`, `--index-key` | `K4A_MCP_INDEX_CA`, `_CERT`, `_KEY` | Verify k4a-index's certificate; present a client certificate to it. |
| `--index-token-file` | `K4A_MCP_INDEX_TOKEN_FILE` (or `K4A_MCP_INDEX_TOKEN`) | Token for k4a-index. Refused at startup without TLS to the index. |

## Authenticating callers

Both `k4a-index` and `k4a-mcp` can ask callers for a credential. **Nothing
is required by default**, so upgrading an install changes nothing on the
wire; each instead logs a warning at startup that its listener is plaintext
and unauthenticated. What it authenticates is "a caller holding this
deployment's credential"; without a policy (next section) every
authenticated caller reads every followed topic.

`k4a-index` chart values:

| Value | Renders | Notes |
|---|---|---|
| `listener.tls.existingSecret` (`.certKey`, `.keyKey`) | `K4A_INDEX_TLS_CERT`, `K4A_INDEX_TLS_KEY` | A `kubernetes.io/tls` Secret mounted at `/etc/k4a/listener-tls`. Keys default to `tls.crt` / `tls.key`. A renewed certificate is served without a restart. |
| `listener.tls.clientCAKey` | `K4A_INDEX_TLS_CLIENT_CA` | A CA key in the same Secret (e.g. `ca.crt`); clients must then present a certificate it signed. |
| `listener.token.existingSecret` (`.key`) | `K4A_INDEX_TOKEN_FILE` | Secret mounted at `/etc/k4a/listener-token`; key defaults to `token`. Restart the pod after rotating it. |

Health checks (and the TCP probes) stay open whatever is configured.

**Upgrade path** for a running plaintext install:

1. Issue the daemon a certificate (cert-manager, or your PKI) for the names
   clients dial, and set `listener.tls.existingSecret`. Clients drop
   `index-insecure` and, for a private CA, set `index-ca`. A k4a still
   configured with `index-insecure: true` now fails its handshake and
   searches locally, with a warning — slower, never wrong.
2. Create a token Secret, set `listener.token.existingSecret`, and hand the
   token to clients (`K4A_INDEX_TOKEN`, `--index-token-file`, or a context's
   `index-token: $VAR`). Clients refuse to send a token over plaintext, so
   do step 1 first, or keep TLS on a load balancer in front and dial that.
3. Optionally require client certificates with `listener.tls.clientCAKey`;
   clients set `index-cert`/`index-key`.

Doing all three at once is fine for a new install; on a live one, each step
breaks only the clients not yet moved, and those degrade to a scan.
In-cluster `k4a-mcp` is one of those clients: move it with its `index.*`
values (below) in the same step.

`k4a-mcp` chart values have the same `listener.*` shape (Secrets mounted at
`/etc/k4a/listener-tls` and `/etc/k4a/listener-token`; the HTTP probes
switch to HTTPS when `listener.tls` is set), plus what it presents to
`k4a-index`:

| Value | Renders | Notes |
|---|---|---|
| `index.tls.enabled` | `K4A_MCP_INDEX_TLS` | Dial `k4a-index` over TLS. |
| `index.tls.existingSecret` (`.caKey`, `.certKey`, `.keyKey`) | `K4A_MCP_INDEX_CA`, `_CERT`, `_KEY` | Mounted at `/etc/k4a/index-tls`; empty keys are skipped. `caKey` defaults to `ca.crt`; cert and key are for mTLS. |
| `index.token.existingSecret` (`.key`) | `K4A_MCP_INDEX_TOKEN_FILE` | Mounted at `/etc/k4a/index-token`. The chart refuses to render it without `index.tls.enabled`. |

`config.indexAddr` must then be a name on `k4a-index`'s certificate (the
Service name, e.g. `k4a-index.<namespace>.svc`).

MCP clients send the token as `Authorization: Bearer <token>`; for Claude
Code, see [`mcp-setup.md`](mcp-setup.md).

## Authorizing callers

Both charts take a per-caller authorization policy under `listener.authz`
(`--authz-policy`; the format and semantics are in
[`design/shared-index-service.md`](../design/shared-index-service.md#per-caller-authorization-phase-5-118)).
**Unset, nothing changes**: every authenticated caller reaches every topic
the daemon follows. Once set it is default-deny — a caller the policy does
not name is refused, and a named caller reaches only the topics it grants.

| Value | Renders | Notes |
|---|---|---|
| `listener.authz.policy` | ConfigMap `<fullname>-authz`, `K4A_INDEX_AUTHZ_POLICY` / `K4A_MCP_AUTHZ_POLICY` | The policy inline. Changing it rolls the pods (it is read at startup). |
| `listener.authz.existingConfigMap` (`.key`) | `K4A_INDEX_AUTHZ_POLICY` / `K4A_MCP_AUTHZ_POLICY` | A ConfigMap you manage, mounted at `/etc/k4a/authz`; key defaults to `policy.yaml`. Restart the pods after changing it. Set this or `policy`, not both. |

```yaml
listener:
  tls:
    existingSecret: k4a-index-tls
    clientCAKey: ca.crt        # certificate entries need a client CA
  authz:
    policy:
      callers:
        - name: payments-oncall
          cert: {cn: payments-oncall}
          search: ["payments.*"]
          follow: ["payments.*"]
        - name: k4a-mcp        # the daemon sees k4a-mcp, not the people behind it
          cert: {cn: k4a-mcp}
          search: ["telemetry.*"]
          follow: ["telemetry.*"]
        - name: ci
          tokenSHA256: <printf %s "$TOKEN" | sha256sum>
          search: ["telemetry.*"]
```

Token entries hold the token's SHA-256, never the token, and replace
`listener.token` (the daemon refuses both). Hand each caller its own token;
clients send it exactly as they send the shared one. A refused k4a client
scans with its own Kafka credentials instead, so a too-narrow policy is
slower, never wrong. Behind `k4a-mcp`, give `k4a-mcp`'s identity the union
of what its callers may reach in the daemon's policy, and the per-person
grants in `k4a-mcp`'s own.

## Networking

Both charts create a `ClusterIP` Service only. To reach them from developer
machines, put an **internal** LoadBalancer or Ingress in front, reachable over
your private network / VPN, with DNS such as `k4a-index.example.com` and
`k4a-mcp.example.com`:

- **k4a-index** serves plaintext gRPC (h2c) on 9500 unless `listener.tls`
  is set. Either serve TLS from the pod, or terminate TLS at the edge on
  `:443` and make sure the listener **negotiates ALPN `h2`** (on an AWS NLB,
  `alpnPolicy: HTTP2Preferred`; on an Ingress controller, its gRPC
  backend-protocol setting). Without it the TLS handshake fails and every
  client silently falls back to a broker scan.
- **k4a-mcp** serves MCP streamable-HTTP on 9501 at `/mcp`, plus a plain
  `/health`. Serve TLS from the pod (`listener.tls`) or terminate it at the
  edge; the MCP client URL is then `https://<k4a-mcp-host>/mcp`.

If you restrict ingress with a `NetworkPolicy`, note that a load balancer
targeting pod IPs reaches the pod from addresses that are **not pods** (e.g.
the load balancer's own network interfaces). A rule that only uses
`namespaceSelector`/`podSelector` drops that source and fails target health
checks; admit the load balancer's subnets with an `ipBlock` instead. If you
also restrict egress, the daemon needs the brokers plus, for IAM auth, STS
and the node-local credential agent.

## Metrics and probes

`k4a-index` serves the standard `grpc.health.v1` service on its gRPC port
(open whatever credentials the listener asks for). It reports `SERVING`
once the listener is up, after the pinned pre-follow set is open, and
`NOT_SERVING` from SIGTERM on, so endpoints drop before in-flight RPCs
drain. The chart's probes are gRPC probes against it; a TLS listener is
probed through the admin port's `/readyz` instead, since the kubelet's gRPC
probe has no TLS (or by TCP when `metrics.enabled` is false). Readiness
does not wait for topics to warm (a cold topic answers with no coverage and
the client scans) and does not track Kafka reachability (a warm index stays
useful while the brokers are away); a wedged indexing drain is the
drain-stall watchdog's job, which exits the process.

With `metrics.enabled` (default on in the chart, port `metrics.port`
9502) the admin port also serves Prometheus metrics:

| Metric | Type | What |
|---|---|---|
| `k4a_index_followed_topics` | gauge | Topics followed now. |
| `k4a_index_topic_indexed_records_total{topic}` | counter | Records handed to the indexer since the topic was followed. |
| `k4a_index_topic_dropped_records_total{topic}` | counter | Records dropped on a full buffer, each one a coverage gap. Alert on any increase. |
| `k4a_index_topic_buffered_records{topic}` | gauge | Records waiting to be indexed. |
| `k4a_index_topic_lag_records{topic}` | gauge | Broker records past the newest indexed offset (partitions with indexed records), sampled every 15s. |
| `k4a_index_topic_newest_record_timestamp_seconds{topic}` | gauge | Newest indexed record; `time() -` this is staleness. |
| `k4a_index_topic_stored_bytes{topic}` | gauge | Bytes the index holds (what `--topic-max-bytes` caps). |
| `k4a_index_topic_pinned{topic}` | gauge | 1 for a `--topic` pin. |
| `k4a_index_searches_total{outcome}` | counter | `served`, `warming`, `denied` (not in `--allow`), `forbidden` (outside the caller's `--authz-policy` grant), `error`. |
| `k4a_index_search_duration_seconds` | histogram | Latency of searches served from an index. |
| `k4a_index_evictions_total{reason}` | counter | `cap`, `idle`, `disk_budget`, `tail_exit`. |

plus the Go runtime and process collectors. Per-topic series disappear
when the topic leaves the follow-set. `metrics.serviceMonitor.enabled`
renders a prometheus-operator `ServiceMonitor`, and
`networkPolicy.metricsFrom` limits who may reach the port when the
NetworkPolicy is on. There is still no pprof endpoint.

## Operating notes

- **Restarting is cheap.** Storage is an `emptyDir` on purpose: the index
  is fully reconstructable from the broker, so losing it costs a re-warm
  (a backfill of `--backfill-window` per topic) and nothing else. Never
  reach for a PVC here. A container restart within the pod keeps the
  volume: the daemon re-follows its saved follow-set
  (`/data/k4a-index-followset.json`) and each index catches up from where
  it stopped. A new pod (a reschedule, a drain, an upgrade) starts on an
  empty volume, so it re-follows only `config.prefollow` unless
  `config.followSetTopic` is set: then it reads the follow-set from that
  compacted topic and re-follows what the deployment followed, backfilling
  each topic, and replicas follow what any one of them starts. Grant the
  daemon's identity read, write and describe on the topic first (on MSK IAM
  `kafka-cluster:ReadData`, `WriteData`, `DescribeTopic`; add `CreateTopic`
  to let the daemon create it, or create it yourself with
  `cleanup.policy=compact`). Without the grant the daemon logs an error and
  keeps the local file only.
- **Silence is the failure mode, not restarts.** The drain-stall watchdog
  exists because a wedged drain serves *nothing*, silently, for as long as
  nobody notices. If the daemon exits with a goroutine dump on stderr, that
  dump is the artifact — the distroless image has no general shell and no pprof,
  so it's the only root-cause evidence you'll get. Before it fires,
  `k4a_index_topic_buffered_records` climbing while
  `k4a_index_topic_indexed_records_total` stands still is the same wedge
  seen from the outside.
- **Coverage is honest by contract.** A client only gets an index-served
  answer for a window the daemon actually covers; anything else falls back
  to a client-side scan. So a cold, lagging, or trimmed index degrades to
  "slower", never to "wrong".
- **A dead daemon is not an outage.** Every client path has a
  "endpoint unreachable → local behavior" branch. A k4a pointed at a dead
  endpoint must be indistinguishable from a k4a with none.
