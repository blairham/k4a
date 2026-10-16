# Design: Archive search (S3 sink + Athena)

**Status:** proposed — this is the design pass #117 asked for before code.
It is the expanded form of Tier 6 in [`local-index.md`](local-index.md).
**Depends on:** [`auth.md`](auth.md) (AWS profile handling),
[`deep-search.md`](deep-search.md) (the search semantics this must not fork),
[`local-index.md`](local-index.md) (the candidate-filter rule for recall).
**Code (planned):** `internal/archive/` (planning, SQL, estimate, execution
behind interfaces), `internal/command/search_archive.go` (the `--archive`
path of `k4a search`), `internal/config` (the per-context `archive:` block).

## Purpose

Kafka keeps days, sometimes hours. Many organizations already copy their
topics to S3 with a sink connector (Confluent's S3 sink, or an equivalent)
and register a table over it in the Glue catalog for Athena. "Did this order
ID ever go through `orders`?" for last quarter is then answerable — but only
by someone who knows the table, its partition layout and its SQL dialect.

Archive search lets `k4a search` ask that archive the same question it asks
Kafka, with the same pattern semantics, without making the user write SQL,
and without letting a typo turn into a full-table scan that someone pays for.

## Scope

In v1:

- `k4a search --archive PATTERN -t TOPIC --since 90d` runs one Athena query
  over a table the user has configured for that topic, and prints matches in
  the existing `text` / `json` / `summary` formats.
- It does not connect to Kafka at all. An archive search works with the
  cluster unreachable, and its results never claim Kafka coverage.

Not in v1 (each is a deliberate follow-up, not an oversight):

- **No TUI or MCP surface.** Both would need the confirm-before-run step to
  be answered by someone. A TUI dialog is straightforward follow-up work; an
  MCP tool that spends money on an agent's say-so needs its own decision.
- **No `k4a-index` involvement.** The daemon stays Kafka-only.
- **No automatic sink discovery.** The original Tier 6 sketch detected S3
  sink connectors through the Kafka Connect REST API. A connector's config
  says where it writes, not which Glue table reads it or how that table is
  partitioned, so discovery would still need the user to fill in the half
  that matters. Explicit configuration first; discovery can pre-fill it later.
- **No hybrid Kafka + archive answer.** See *Coverage semantics*.
- **k4a creates nothing in AWS.** No sink, no Glue table, no workgroup, no
  bucket. It reads a table that already exists.

## Configuration

Archive settings live in a per-context `archive:` block, because the
archive belongs to the same cluster as the context, and the context already
carries the AWS profile that usually reaches it.

```yaml
contexts:
  production:
    brokers: b-1.prod:9098
    auth: iam
    profile: Production/ReadOnlyAccess
    archive:
      # Optional. Defaults to the context's profile and region (after
      # -p/--profile and -r/--region overlay them). Set them when the
      # archive lives in another account, e.g. a data-lake account.
      profile: DataLake/AthenaReader
      region: us-east-1
      workgroup: k4a                       # default "primary"
      output-location: s3://athena-results/k4a/   # omit if the workgroup enforces one
      catalog: AwsDataCatalog              # default
      confirm-bytes: 1GiB                  # ask before a query estimated above this
      max-bytes: 50GiB                     # refuse above this; stop a query that crosses it
      topics:
        orders:
          database: kafka_archive
          table: orders
          columns:
            value: value                   # required, varchar
            timestamp: ts                  # required
            timestamp-type: epoch-millis   # epoch-millis (default) or timestamp
            key: key                       # optional, varchar
            headers: headers               # optional, varchar
            partition: kafka_partition     # optional, integer
            offset: kafka_offset           # optional, bigint
          time-zone: UTC                   # the sink partitioner's zone (default UTC)
          time-partitions:                 # at least one; see Partition pruning
            - column: dt
              format: "{yyyy}-{MM}-{dd}-{HH}"
          # Optional: lets k4a estimate bytes before running. Same tokens.
          location: s3://kafka-archive/topics/orders/dt={yyyy}-{MM}-{dd}-{HH}/
```

Rules, all checked when the block is loaded for a search rather than when
the whole file is read (a broken `archive:` block must not stop the TUI from
opening):

- **Identifiers are identifiers.** `database`, `table`, every column name
  and every partition column must match `[A-Za-z_][A-Za-z0-9_]*`. They are
  double-quoted in the SQL. There is no expression form: a column that is a
  struct, or a value that needs `json_format(...)`, gets a view, and the view
  is what k4a is pointed at. This keeps user config out of the SQL grammar.
- **Sizes** take `B`, `KB`/`MB`/`GB`/`TB` (powers of 1000) and
  `KiB`/`MiB`/`GiB`/`TiB` (powers of 1024).
- **`time-partitions` is required.** A table k4a cannot prune by time is a
  table every search scans in full, which is precisely the expensive mistake
  this design exists to prevent. There is no opt-out in v1.
- `$VAR` expansion follows the existing whole-value rule (`contexts.md`)
  for `profile`, `region`, `workgroup` and `output-location`.

## Auth

The archive reuses `internal/awscreds.Load(ctx, profile, region)` — the same
in-process shared-config loading that IAM auth and the SSM broker lookup use
— so an SSO session or a `credential_process` (granted, aws-vault) works
without a wrapper, and an ambient session works as the default chain.

Profile and region resolve, most specific first:

1. `archive.profile` / `archive.region` in the context.
2. The connection's resolved profile/region: `-p`/`--profile`, `-r`/`--region`
   (and `AWS_PROFILE` / `AWS_REGION` through them), over the context's
   `profile:` / `region:`.
3. Empty, which `awscreds.Load` resolves through the SDK's chain and then
   `awscreds.DefaultRegion`.

The archive's own setting wins over the flag because it is the more specific
statement: `-p` is documented as the cluster's profile, and an archive in a
data-lake account is exactly the case where the two differ.

An expired or missing SSO session surfaces from the first AWS call. k4a
wraps it with `awscreds.LoginHint` naming the profile actually used, the same
way the SSM lookup does.

Permissions the profile needs: `athena:StartQueryExecution`,
`athena:GetQueryExecution`, `athena:GetQueryResults`,
`athena:StopQueryExecution` on the workgroup; `glue:GetTable` /
`glue:GetPartitions` on the table (Athena calls these on the caller's
behalf); `s3:GetObject` / `s3:ListBucket` on the archive; write on the
output location; and `s3:ListBucket` on the archive prefix if `location` is
set for estimates.

## Query shape

One `SELECT` per search, built from the configuration and the parsed search
request:

```sql
SELECT "value", "key", "ts", "kafka_partition", "kafka_offset"
FROM "AwsDataCatalog"."kafka_archive"."orders"
WHERE "dt" IN ('2026-07-18-14', '2026-07-18-15', ...)      -- partition pruning
  AND "ts" BETWEEN 1784383200000 AND 1792160000000          -- exact time bounds
  AND (strpos(lower("value"), 'order-41000') > 0
       OR strpos(lower("key"), 'order-41000') > 0)          -- candidate filter
ORDER BY "ts" DESC
LIMIT 500
```

String literals are built by k4a and single-quote-doubled; nothing from the
config file reaches the SQL except validated identifiers. The SQL is printed
with `--explain`, which runs nothing.

### Pattern semantics: candidate filter, then the real regex

The scan and the index match grep-style with a Go RE2 pattern. Athena's
regex dialect is not RE2, and the two differ in ways that drop matches
silently. So the rule from `local-index.md` applies unchanged: **the query
is a candidate filter that must never miss, and precision comes from
re-applying the exact Go regex to each returned row.**

The candidate filter is a **mandatory literal**: a string every match of the
pattern provably contains, found by walking the parsed `regexp/syntax` tree
(a literal; the longest required literal of a concatenation, including runs
formed by adjacent literal children; the body of a capture, `+` or `{n,}`
with n ≥ 1). Alternation, optional parts and character classes contribute
nothing. The literal must be at least 3 characters.

Case-insensitive patterns (the default) push down `strpos(lower(col),
lower(lit))`. That is recall-safe only for characters whose Unicode
simple-fold orbit is entirely ASCII: Go's `(?i)k` also matches U+212A KELVIN
SIGN and `(?i)s` matches U+017F LONG S, which `lower()` never turns into `k`
or `s`. The literal is split at any such rune and the longest remaining run
is used, so the fuzz-found Kelvin/long-s class of gap cannot reappear here.

**A pattern with no usable literal is refused**, with the reason, rather than
run as "every row in the window". Returning every row would bill the full
window and pull it back through `GetQueryResults` 1000 rows at a time. The
user can add a literal or narrow the time range and use a Kafka scan.

`--path` (JSON-path predicate) is refused with `--archive` in v1: it has no
pushdown, so it would reduce to the same unbounded case.

`--scope` selects which configured columns the literal is ORed across and
the regex is re-applied to. A scope that names a field the archive does not
have (`key` with no `key` column) searches the fields it does have and says
so in the coverage report, instead of refusing the default `key+value` on
every value-only archive.

`--partitions` becomes `"kafka_partition" IN (...)` when a partition column
is configured, and is refused otherwise.

### Limits and ordering

`ORDER BY ts DESC LIMIT cap` keeps results newest-first like every other
source. `cap` is `--cap`, else the deep-search default. The limit applies to
candidates, and some candidates fail the exact regex, so a result whose
candidate count reached the limit is reported `capped` even when fewer
matches survived. Ordering does not change bytes scanned; Athena bills the
partitions read regardless.

Sinks are commonly at-least-once, so with both partition and offset columns
configured, rows are de-duplicated on `(partition, offset)` and the count of
dropped duplicates is reported. Without them, duplicates are passed through
and the report says de-duplication was not possible.

## Cost controls

Athena bills by bytes scanned (list price $5/TB, 10 MB minimum per query).
Three layers, cheapest first:

1. **Partition pruning (always).** Every query constrains the time-partition
   columns to exactly the partition values that intersect `[since, until]`
   (below). `--since` is required with `--archive`; `--until` defaults to now.
   There is no unbounded archive search.
2. **Estimate and confirm before running.** When `location` is configured,
   k4a lists the S3 prefixes of the pruned partitions (`ListObjectsV2`, a few
   at a time) and sums object sizes. That is an upper bound for row formats
   (JSON, Avro) and an overestimate for columnar ones (Parquet, ORC), which
   is the safe direction. Then:
   - estimate > `max-bytes`: refuse before running, naming both numbers
     (`--max-bytes` raises the cap for one run);
   - estimate > `confirm-bytes`, or no estimate (no `location`): ask on a
     terminal, showing the estimate and its list-price cost; refuse a
     non-terminal stdin without `--yes` — the same contract as the
     destructive commands' `guardMutation`;
   - otherwise run.
3. **A bytes-scanned watchdog while running.** k4a polls
   `GetQueryExecution` (backing off from 500 ms to 2 s) and calls
   `StopQueryExecution` as soon as `Statistics.DataScannedInBytes` passes
   `max-bytes`. This is best effort: statistics lag the scan, so a query can
   overshoot by one polling interval's worth. The hard limit belongs to AWS:
   the design recommends a dedicated workgroup with
   `BytesScannedCutoffPerQuery` set, and `max-bytes` should sit at or under it.

Every result reports the bytes actually scanned, the estimate it ran under,
and the query execution ID, so a surprising bill can be traced to a query.

Results are never cached: the Tier 2 cache keys on Kafka high watermarks,
which say nothing about an archive, and a stale archive answer is worse
than a slow one.

### Partition pruning

`time-partitions` is a list of `{column, format, type}`. A format is literal
text plus the tokens `{yyyy}`, `{MM}`, `{dd}`, `{HH}` (zero-padded); `type`
is `string` (default, values quoted) or `integer` (values unquoted, the
format must then be a single token). Two common layouts:

```yaml
# One compound column — prunes exactly
time-partitions:
  - {column: dt, format: "{yyyy}-{MM}-{dd}-{HH}"}

# Confluent TimeBasedPartitioner's Hive-style path
time-partitions:
  - {column: year,  format: "{yyyy}"}
  - {column: month, format: "{MM}"}
  - {column: day,   format: "{dd}"}
  - {column: hour,  format: "{HH}"}
```

The values to select are found by sampling: the instant `since`, every
whole UTC hour inside the range, and `until`, each formatted in
`time-zone`. Any one-hour local partition that intersects the range
contains one of those samples, so this is recall-complete for every zone,
including half-hour offsets and DST transitions, without calendar logic.
Duplicate tuples collapse.

A single column becomes `"dt" IN (...)`. Several columns become a nested
OR of ANDs grouped by the leading columns (`year = '2026' AND month = '10'
AND (day = '16' AND hour IN (...) OR ...)`). That is exact, but Athena may
prune it only to the per-column cross product, so the compound-column layout
prunes better; the doc for users should say so. The exact `ts BETWEEN`
predicate trims the partial partitions at either end.

## Coverage semantics

Every source k4a has reports what it covered, and an archive must too,
because its window and Kafka's overlap only partially and neither is a
superset of the other:

```
            archive (S3)                        Kafka retention
 ├──────────────────────────────────────┤
                              ├──────────────────────────────────┤
 older ──────────────────────────────────────────────────────── now
                              ↑ Kafka's oldest      ↑ sink lag: newest records
                                retained record       not yet flushed to S3
```

- The **archive is behind**: a sink flushes on size or time, so the newest
  minutes to hours exist only in Kafka.
- The **archive can be ahead of Kafka's past**: retention and compaction
  delete records from Kafka that the archive kept. Offsets in archive results
  can be ones a consumer can no longer read.

So an archive result reports, in the `summary` document (as a new
`archive` object beside `stats`; `stats.source` is `archive`) and on stderr
for `text`/`json`:

- `requested`: the `[since, until]` asked for;
- `partitions`: how many partition values were selected, and the first and
  last;
- `fields`: the columns searched, and any requested by `--scope` that the
  archive does not have;
- `candidate_literal`: what was pushed down (it is also what lands in
  Athena's query history — see *Security*);
- `estimated_bytes`, `scanned_bytes`, `query_execution_id`;
- `duplicates_dropped`, or that de-duplication was not possible;
- `capped` as above, and a fixed note that records newer than the sink's
  flush lag may exist only in Kafka, and that archived records may no longer
  be in Kafka.

What it does **not** report is Kafka's retention boundary, because an archive
search does not touch Kafka. That is the reason for no hybrid in v1: a
correct hybrid needs Kafka's oldest retained timestamp per partition, the
sink's flush watermark, and partition/offset columns to de-duplicate the
overlap — and when any of those is missing it has to fall back to one source
and say so. It is a reasonable Tier 6b once v1 shows how archives are
actually laid out, and the coverage fields above are the vocabulary it would
use. Until then the CLI states the boundary rather than hiding it: archive
for history, plain `k4a search` for the recent window.

## Security

- Athena stores **query text** in the workgroup's history and **results**
  (matched records) under the output location. The pushed-down literal and
  the matched records therefore persist in the user's AWS account, under
  that account's access controls — not under `~/.k4a`. The help text and
  user docs say this, because "search for this customer's email" leaves the
  email in query history.
- k4a writes nothing under `~/.k4a` for an archive search: no cache, no
  index, no session state.
- Credentials never leave the SDK. Nothing about the AWS session is logged.
- `SECURITY.md` gains a paragraph covering the above when the code lands.

## Testability

The AWS calls sit behind two narrow interfaces in `internal/archive`, each
the subset of the SDK client's method set the code calls, so the real
`*athena.Client` and `*s3.Client` satisfy them and tests pass fakes:

```go
type AthenaAPI interface {
    StartQueryExecution(ctx, *athena.StartQueryExecutionInput, ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
    GetQueryExecution(ctx, *athena.GetQueryExecutionInput, ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
    GetQueryResults(ctx, *athena.GetQueryResultsInput, ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
    StopQueryExecution(ctx, *athena.StopQueryExecutionInput, ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
}

type S3API interface {
    ListObjectsV2(ctx, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}
```

Partition enumeration, SQL generation, literal extraction and size parsing
are pure functions and tested as such (the literal extractor also gets a
property test against the Go regex: every string the regex matches contains
the literal). Execution — polling, the watchdog stop, pagination, header
row, de-duplication, the confirm/refuse branches — runs against the fakes.
The polling interval is a field, so tests do not sleep. No test constructs a
real SDK client or reads `~/.aws`; anything that loads AWS config uses the
`isolateAWS` pattern from `internal/awscreds` (`t.TempDir` + `t.Setenv`).

New dependencies: `github.com/aws/aws-sdk-go-v2/service/athena` and
`.../service/s3`, both in the aws-sdk group Dependabot already moves. No
hosted service, no new daemon.

## Open questions

- **Default sizes.** 1 GiB to confirm and 50 GiB to refuse (≈ $0.005 and
  ≈ $0.25 at list price) are deliberately low for a first release; raise
  them once real archives show what a typical search costs.
- **Value encodings.** v1 assumes the archived value is text (JSON, or a
  string column a view produced). Avro-in-Parquet archives of
  schema-registry topics need the view to render them; native support would
  ride on #115's decoded-text work.
- **Sink discovery** from the Kafka Connect REST API, to pre-fill
  `location` and `time-partitions` from a connector's `path.format`.
