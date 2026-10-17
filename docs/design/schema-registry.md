# Design: Schema Registry decoding

**Status:** living document. Decoding is wired into Messages, Message Detail,
`k4a consume`, and search: the deep-search scan, the local index and
`k4a-index` all match decoded text, gated on the registry's identity (#115).
**Code:** `internal/schemareg/`, `internal/command/schemareg.go`,
`internal/tui/schemareg.go`, `internal/tui/views/messages.go`,
`internal/tui/views/message_detail.go`; for search, `internal/kafka/decoder.go`,
`internal/kafka/deep_search.go`, `internal/index/` (`index.go`, `async.go`,
`query.go`), `internal/indexwire/`, `cmd/k4a-index/registry.go`

## Purpose

Most production topics are not JSON. A record written by a Confluent
serializer is a five-byte header followed by Avro, Protobuf or JSON-Schema
encoded bytes, and k4a showed those as a `hex:` dump. This subsystem turns
them back into JSON using the schemas a Confluent-compatible Schema Registry
holds, so Messages, Message Detail and `k4a consume` show what the producer
meant.

It started as a display layer and is now also what search matches. Decoding
never changes what k4a reads from Kafka, never writes to the registry, and a
payload it cannot decode is shown, and searched, exactly as it was before,
with a short reason beside it.

## Wire format

```
byte 0       magic byte, always 0
bytes 1-4    schema ID, big-endian int32
[Protobuf]   message-index path: zigzag varint count, then that many
             zigzag varint indexes; the single byte 0 means [0]
rest         the encoded message
```

`ParseHeader` accepts a payload only when it is at least five bytes, starts
with 0 and names a positive ID: the registry allocates IDs from 1, so a
leading zero followed by a zero or negative ID is a payload that merely starts
with a NUL. Plain text and JSON never start with 0, so the check costs nothing
for topics that do not use a registry: `ErrNotFramed` comes back without any
network call.

### Schema ID in a record header (Confluent Platform 8)

A Confluent Platform 8 serializer configured with `HeaderSchemaIdSerializer`
leaves the payload unprefixed and writes the schema reference to a record
header, `__key_schema_id` or `__value_schema_id`:

```
byte 0       1 (a GUID follows), or 0 (a 4-byte ID follows, as above)
bytes 1-16   the schema GUID, big-endian (UUID most then least significant bits)
[Protobuf]   the message-index path, as above; nothing written means [0]
```

The names and layout are Confluent's own (`SchemaId`, `DualSchemaIdDeserializer`
in confluentinc/schema-registry). `recordToConsumed` keeps the **last** such
header (a null value meaning none), and the record's bytes whatever they start
with, since there is no prefix to recognize them by. Precedence follows
Confluent's deserializer: a parseable header wins over the payload prefix; one
that does not parse falls back to the prefix, and only when the payload is not
framed either does the field get a reason ("neither a GUID nor an ID"). A GUID
is fetched with `GET /schemas/guids/{guid}` and cached beside numeric IDs; its
numeric ID (for the subject/version label) comes from the response or
`/schemas/guids/{guid}/ids`, preferring the default context, and failing that
the card shows `guid ...` alone. `ParseSchemaIDHeader` is in `FuzzParseWire`.

`ParseMessageIndexes` caps the path at 64 entries and each index at 2^20, so a
hostile count cannot allocate before the bytes behind it are checked. Both
parsers take bytes straight off a broker; `FuzzParseWire` checks they never
panic, always return a suffix of their input and agree with franz-go's own
`sr.ConfluentHeader` decoder.

## Registry client

franz-go's `pkg/sr`: the same project as the Kafka client, with no dependencies
of its own beyond the standard library, and an in-memory fake registry
(`srfake`) the tests run against.

- **One lookup per schema ID.** `Registry` keeps every compiled schema in
  memory for the life of the process. Concurrent decodes of a new ID share
  one request. The fetch runs under its own timeout (two times the 10 s HTTP
  timeout), so a caller whose context ends does not leave a canceled result
  in the cache for everyone else.
- **Failures are cached too.** A schema the registry says does not exist
  (404) is remembered for good; any other failure for 30 s. Without that, a
  topic of records naming a missing schema would cost one request per row,
  and with it a registry that was briefly down recovers on its own.
- **Bounded.** At 4096 entries the cache drops a finished entry, preferring a
  failed one, so a topic of garbage whose leading zero byte makes every
  record look like a new schema ID cannot grow it without limit.
- **Subject and version** come from `GET /schemas/ids/{id}/versions`. When an
  ID is registered under several subjects, the TopicNameStrategy subject
  (`<topic>-key` or `<topic>-value`) wins. Failure here costs only the label.

## Formats

`hamba/avro/v2`, the more common Go Avro library, was tried first and
dropped: govulncheck reports three unfixed vulnerabilities in it (GO-2026-5046,
-5047, -5048: CPU exhaustion, integer overflow and unbounded allocation in the
decoder), and decoding untrusted record bytes is exactly the path they sit on.

| Format | Library | Notes |
|---|---|---|
| Avro | `linkedin/goavro/v2` | goavro cannot load named types from elsewhere, so a schema with references compiles as the union `[ref..., main]` (references depth first) and a payload decodes as that union by prefixing the main branch's index. Values render in Avro's JSON encoding (a union as `{"type": value}`), decimals as exact decimal strings and non-finite floats as strings. goavro preallocates an array block from its declared count, so its ceiling is lowered from 2^31 items to 2^20: one hostile record could otherwise allocate 32 GiB. |
| Protobuf | `bufbuild/protocompile` + `google.golang.org/protobuf` | The registry returns `.proto` source; references become files named by their import path, compiled with protoc's standard imports, stubs of Confluent's built-in `confluent/meta.proto` and `confluent/type/decimal.proto`, and the sixteen `google/type/*.proto` files Confluent also treats as built in (`ProtobufSchema`), copied unmodified from googleapis at `886464f` so field numbers match real producers; its serializers never register any of them. A `google.protobuf.Any` is expanded when the schema's files (or the well-known `google.protobuf.*` types) define its type URL, recursively; one that names a type outside them, or whose bytes do not parse as it, renders as `{"@type": url, "value": base64}` instead of failing the record. The message-index path picks the message; `dynamicpb` decodes it and `protojson` renders it, compacted because protojson randomizes whitespace. |
| JSON Schema | none | The payload is already JSON; it is compacted and checked to be JSON. The schema constrains producers and adds nothing to how the value reads, so it is not applied. |

Reference chains are capped at 32 levels and each subject version is fetched
once per compile, so a cycle cannot recurse forever.

Not supported: a GUID in the payload prefix (magic byte 1), which
Confluent's serializers do not write; resolving an `Any` type URL through the
registry rather than the schema's own imports.

## Configuration

A context's nested `schema-registry:` block (`url`, `username`, `password`,
`token`, `ca`; secrets in the whole-value `$VAR` form) names the registry.
`--schema-registry-url`, `-username`, `-password`, `-token` and `-ca` on `k4a`,
`k4a consume`, `k4a search`, `k4a mcp` and `k4a-index`, each with a
`K4A_SCHEMA_REGISTRY_*` environment variable,
override it field by field: flag, then environment, then config, the order
every other connection setting follows. Setting a token drops the context's
username and password and vice versa, so an override never leaves both for
the client to refuse. A `--brokers` connection has no context and takes the
flags alone. In the TUI, a context switch builds the next context's registry
with the same flags over it; a registry that cannot be built (a bad URL, an
unreadable CA) flashes an error and costs decoding, not the switch. The
headless commands and `k4a-index` refuse to start with one instead. The
`k4a-index` chart renders `schemaRegistry.url` and credentials from an
existing Secret as those variables.

## Display

`kafka.ConsumedMessage` keeps a copy of the key and value bytes only when they
could be framed (at least five bytes, leading zero, or a schema-ID header,
whose value it keeps too as `KeySchemaID` / `ValueSchemaID`): a record's slices pin its
whole fetch buffer, and nothing else needs them. The copies, and the
`KeyDecoding` / `ValueDecoding` that record what the decoder did, are tagged
`json:"-"`, so the search cache's on-disk shape does not change.

`Registry.DecodeMessage` replaces `Key` / `Value` with the decoded JSON and
keeps the old rendering in `Decoding.Raw`; a framed field it cannot decode
keeps its rendering and gets `Decoding.Reason`. A nil `*Registry` is decoding
off, so no caller checks.

- **Messages** decodes on a goroutine between the consumer channel and the
  view (`decodeStream`), never on the UI loop. Each record waits at most 3 s
  for a schema the registry has not answered yet; past that it shows raw with
  "did not answer in time", and because the lookup carries on detached, the
  next record with that schema decodes. The `/` filter matches what is shown.
- **Message Detail** names each field's schema (`subject vN (id N, FORMAT)`)
  or why it was not decoded, in the metadata card.
- **`x`** toggles raw and decoded on both views, and drilling in keeps the
  Messages view's mode. Copy (`c`/`K`/`C`) and export (`e`) take the mode
  on screen: the flash says decoded or raw, and each exported record carries
  a `decoding` block with `shown`, the schemas and any decode error. It was free on every view; the hint bar shows it only
  with a registry configured, where it does something.
- **`k4a consume --decode auto|raw`**: `auto` (the default) decodes when a
  registry is configured. `--verbose` adds `key-schema="..."` /
  `value-schema="..."`. Each distinct failure is reported once on stderr (at
  most 20), so stdout stays the messages.

## Search and the indexes

A search with a registry configured matches a framed key or value as its
decoded JSON, the text Messages shows, on every path that can answer it: the
deep-search scan, the search cache, the local index and `k4a-index`. The
index is an accelerator whose answers must equal a scan's (`AGENTS.md`,
*coverage parity*), so decoding is not added to one side alone: the scan and
the indexer take the same decoder, and the coverage contract carries which
decoder built an index, so a client never trusts one that decoded
differently.

**The decoder.** `kafka.RecordDecoder` (`internal/kafka/decoder.go`) is the
seam; `*schemareg.Registry` implements it (`DecodeRecord`, `Identity`), and
the interface lives in `kafka` because `schemareg` imports it.
`kafka.SearchParams.Decoder` carries it. The TUI sets it once, outermost
(`withDecoder` in `internal/tui/actions.go`), from the connection's registry,
so the cache key, both coverage checks and the scan read the same one;
`k4a search` and `k4a mcp` build it from the context's block and
`--schema-registry-*`, with `--decode raw` to match bytes instead, and
`k4a-index` from the same flags and `K4A_SCHEMA_REGISTRY_*`.

**Identity.** `Registry.Identity` is `schemareg/<v>:` and the first 8 bytes
of a SHA-256 over the rendering version and the registry URL, lowercased,
without credentials, query, fragment or a trailing slash. A client and a
daemon pointed at the same registry agree; neither learns the other's URL
from coverage, and nothing secret reaches state on disk. Two URLs for one
registry (an internal and an external hostname) read as different
decoders, which costs a scan, never a wrong answer. `decodingVersion`
names the rendering: a change that makes a payload decode to different text
bumps it, and every index built with the old rendering stops matching.

**Scan.** A record that could be framed is decoded with the scan's context
(no 3 s display deadline: a search waits for the registry) and the decoded
key or value is matched; everything else is matched on its bytes, as before.
A field the registry could not answer for (unreachable, an error status, the
caller's deadline) is matched on its bytes and marks the result `Truncated`,
so it is neither cached nor shown as complete. A schema the registry says
does not exist, or a payload its schema cannot decode, fails the same way
every time and is not a truncation. Matches are emitted decoded, with
`KeyDecoding` / `ValueDecoding`, so the Search view and Message Detail show
them as Messages does.

**Index.** `index.OpenDecoding` fixes an indexer's decoder for its life, and
`IndexBatch` decodes framed records before storing them, outside the
indexer's mutex since a schema lookup is a network call. The bookkeeper's
`decoder` field records the identity; an index on disk built with another
decoder (or none) is renamed aside and rebuilt, as on a schema bump. A record
the decoder could not settle is stored as consumed and recorded as a gap
(`decode_unavailable`) before its batch commits, so no published coverage
spans it without the gap beside it. Gaps carry no time and are never
cleared, so a registry outage during indexing leaves that topic scanning
until its index is cleared or rebuilt; that is the safe direction.
`SchemaVersion` 3 retires every index built before the field existed.

**Coverage.** `Bookkeeper.CanServe` declines when `QueryParams.Decoder`
differs from the bookkeeper's, with the reason ("index decoded with schema
registry ..., search with no schema registry"). `indexwire` round-trips the
field as `Coverage.decoder` (and the client's as `SearchRequest.decoder`),
so the shared and local decisions stay identical. A daemon predating the
field reports none, which a decoding client declines. The reverse is the one
gap: a client predating the field ignores it, so an old raw client would
trust a decoding daemon. Upgrade clients before giving a daemon a registry.

**Cache.** `searchcache.Key` includes the identity (omitted when empty, so a
raw search's key hashes as it did). Decoding metadata is `json:"-"`, so a
replayed or index-served match carries the decoded text but not the raw
rendering or the schema label: `x` has nothing to toggle to there.

**Still out of scope.** Headers are never decoded. A field-aware query
(`field:value`, Tier 5 in [`local-index.md`](local-index.md)) is #116.

## Credentials and transport

- A context block or flags supply the URL and either HTTP basic credentials
  or a bearer token, never both. Secrets take the whole-value `$VAR` form
  every other context secret uses (`docs/design/contexts.md`).
- A URL with embedded credentials is refused: they would appear in every
  error that names it. Errors k4a builds show the URL only through
  `RedactURL`, and registry errors are reduced to the status code and the
  registry's message, never the request URL.
- Credentials are sent only over https, or http to a loopback host (a local
  dev stack), the rule the OAuth token URL follows. An http registry without
  credentials is allowed.
- TLS trusts the system roots plus an optional CA file, at TLS 1.2 or later.
- Nothing is logged, and nothing the registry returns is written to disk: the
  schema cache is in memory only.
