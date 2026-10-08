# Configure k4a & connect Claude Code to the shared index

This guide covers two things:

1. **Configuring k4a** — the `~/.k4a/config.yaml` contexts every k4a
   surface (TUI, CLI, MCP) connects through.
2. **Pointing Claude Code (or any MCP client) at `k4a-mcp`** — the shared,
   always-on index served over the Model Context Protocol, so an agent can
   search your Kafka topics as a tool call.

There are **two ways** to give an agent k4a's search, and they trade off
exactly the way the [shared-index design](../design/shared-index-service.md)
describes:

| | `k4a mcp` (local stdio) | `k4a-mcp` (shared, networked) |
|---|---|---|
| Runs | on your laptop, per agent | once, centrally, always on |
| Backing | direct Kafka **scan** each call | the shared warm **index** |
| First query | seconds (scan) | milliseconds (already warm) |
| Kafka auth | **your** creds (e.g. AWS IAM via SSO/assume) | the daemon's own identity (e.g. EKS Pod Identity or IRSA) |
| Needs | active creds for the context | network reach to the daemon (private network / VPN) |
| Setup | `claude mcp add` a command | `claude mcp add` an HTTP URL |

Use the shared server when it's deployed and you can reach it; fall back to
local stdio for a cluster it doesn't follow, or when you're off-network.

The examples below use **MSK IAM auth**, where there is no SASL password or
key anywhere. The difference between the two paths is *whose* identity
connects: the local stdio server uses **your** creds (resolved from the
context's `profile` / `region`, so you need an active SSO/`assume` session),
while the shared daemon uses **its own** identity in-cluster (you manage no
creds for that path — you only need to reach it on the network).

---

## 1. Configure k4a

k4a reads a kubeconfig-style file at `~/.k4a/config.yaml`. Each **context**
names a cluster + how to authenticate. See
[`design/contexts.md`](../design/contexts.md) for the full resolution rules
(SSM broker lookup, `$VAR` expansion, precedence).

```yaml
current-context: staging
contexts:
  staging:
    ssm-brokers: /my-cluster/msk-brokers   # or: brokers: b1:9098,b2:9098
    auth: iam
    region: us-east-1
    profile: my-staging-profile
  production:
    brokers: broker1:9098,broker2:9098
    auth: iam
    region: us-east-1
    profile: my-production-profile
```

`auth: iam` is the MSK IAM method: k4a signs the connection with your AWS
identity resolved from `profile`/`region` (or the ambient
`AWS_PROFILE`/`AWS_REGION`). No password or key is stored. Before using any
IAM context locally, make sure you have live creds —
your SSO login (or `assume <profile>`) — or the connection fails with an auth
error. (Other methods — `plaintext`, `tls`, `scram`, `mtls` — exist for
brokers that don't use MSK IAM.)

List and switch contexts:

```bash
k4a contexts                 # list configured contexts
k4a use-context staging      # set current-context
k4a --context production …   # one-off override on any subcommand
```

Every headless subcommand (`search`, `mcp`, `produce`, …) takes the same
connection flags (`--context`, `--brokers`, `--auth`, `--region`,
`--profile`, …), so anything you can put in a context you can also pass ad
hoc. `k4a --help` and `k4a search --help` list them.

### Point k4a's own search at the shared index (optional)

Beyond the MCP server, **k4a's own search** (`k4a search`, the TUI's `s`,
the local `k4a mcp`) can query the shared `k4a-index` daemon directly. Add
`index-endpoint` to the context:

```yaml
contexts:
  staging:
    ssm-brokers: /my-cluster/msk-brokers
    auth: iam
    region: us-east-1
    profile: my-staging-profile
    # Shared index (gRPC). A time-bounded search the daemon covers is served
    # from the warm index in milliseconds; anything else transparently falls
    # back to a broker scan (and warms the topic for next time). The daemon is
    # an accelerator, never a hard dependency.
    index-endpoint: k4a-index.example.com:9500
    index-insecure: true   # plaintext gRPC (network-gated); see the TLS note below
```

Or per-invocation: `k4a search … --index-endpoint host:port [--index-insecure]`.

When a query is served from the index the search header reads
**`indexed (shared) · N matches · 9ms`** instead of `scanned · … · 9.4s`.
Coverage is honest: if the index doesn't span your whole `--since/--until`
window (or the topic isn't followed), k4a scans — it never serves partial or
stale results as complete.

> **TLS note.** The daemon itself serves plaintext gRPC on `:9500`. If it is
> exposed that way on a private network, use `:9500` +
> `index-insecure: true`.
>
> If it sits behind a TLS-terminating load balancer or Ingress on `:443`
> (see [`guides/deploy.md`](deploy.md#networking)), that listener must
> negotiate ALPN `h2` for gRPC-over-TLS clients to connect. Drop
> `index-insecure` and point at `:443` to use TLS.

---

## 2a. Connect Claude Code to the shared `k4a-mcp` (recommended)

`k4a-mcp` speaks MCP over **streamable-HTTP**. Point Claude Code at its
internal URL. You must be able to reach it — it's network-gated
(private network / VPN), same as the index's gRPC port. The v1 security
model is exactly that network gate plus the daemon's non-sensitive topic
allowlist; see the
[design doc](../design/shared-index-service.md#security--the-shared-index-surface).
The URLs below are placeholders — substitute wherever you deployed it.

**User scope** (available in every project on your machine):

```bash
claude mcp add --transport http --scope user \
  k4a-index https://k4a-mcp.example.com/mcp
```

**Project scope** (checked in, shared with everyone working in the repo) — a `.mcp.json` at the
repo root; see [`.mcp.json.example`](../../.mcp.json.example) in this repo:

```json
{
  "mcpServers": {
    "k4a-index": {
      "type": "http",
      "url": "https://k4a-mcp.example.com/mcp"
    }
  }
}
```

Verify it's connected:

```bash
claude mcp list          # shows k4a-index and whether it connected
```

In a session, `/mcp` lists the server and its tools. The two tools:

- **`search`** — search a followed topic. Returns matches **plus a coverage
  frame** and `served_from_index`. When `served_from_index` is `false` the
  topic isn't warm yet, so ask the agent to fall back to a direct scan — but if
  the topic is on the daemon's allowlist, that first search also **registers it**
  (warm-on-first-search), so the *next* search of it is served from the index.
- **`coverage`** — report what the index holds for a topic (per-partition
  offset/time ranges, gaps) without running a query.

Example prompts once connected:

> "Using the k4a-index search tool, did any record on
> `example.ingest.prices.v1` mention `4100000017` in the last 24h?"

> "Check k4a-index coverage for `example.ingest.prices.v1` before
> searching — is the index authoritative for the last hour?"

Removing it: `claude mcp remove k4a-index`.

## 2b. Local stdio fallback (`k4a mcp`)

For a cluster `k4a-mcp` doesn't follow, or when you're off-network, run the
in-process MCP server. It scans Kafka directly using your configured
context — no shared daemon involved:

```bash
claude mcp add --scope user k4a-local -- k4a mcp --context staging
```

or a `.mcp.json` entry:

```json
{
  "mcpServers": {
    "k4a-local": {
      "command": "k4a",
      "args": ["mcp", "--context", "staging"]
    }
  }
}
```

This is the single-user, cold-scan path — correct everywhere, just not
pre-warmed. Same `search` tool schema as the shared server, so prompts
transfer unchanged. Because it connects to MSK as **you**, the context must
be `auth: iam` and you must have a live AWS session (`assume`/SSO) when the
agent calls the tool — otherwise the scan fails with an IAM auth error.

---

## Troubleshooting

- **`claude mcp list` shows k4a-index failed to connect** — you likely
  can't reach the internal host. Confirm your VPN / network path is up and
  `curl -sS https://k4a-mcp.example.com/health` returns
  `200`.
- **`served_from_index: false` on every search** — either the topic is off the
  daemon's follow allowlist (default-deny; it will never be followed — see the
  [design doc](../design/shared-index-service.md#security--the-shared-index-surface)),
  or it's allowlisted but you're only searching a window older than when it was
  first registered. An allowlisted topic warms on its first search; if the second
  search is *still* `false`, it's denied — use the local stdio server for it.
- **Results look stale** — the coverage frame is authoritative; if
  `offset_hi` lags the broker head, the index is still catching up. The
  agent should scan the uncovered tail. This is by design — the index never
  reports stale data as current.
- **Local `k4a mcp` fails with an auth/timeout error** — your AWS session
  for the context's IAM identity has probably expired. Re-`assume` the
  profile / re-run your SSO login and retry. (The shared server can't hit
  this — it uses its own in-cluster identity, not your creds.)
