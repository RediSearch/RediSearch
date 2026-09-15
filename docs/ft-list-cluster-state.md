# FT._LIST WITHCLUSTERSTATE

```text
FT._LIST [WITHCLUSTERSTATE]
```

`FT._LIST` without arguments returns the serving shard's local index names, as
before. The case-insensitive `WITHCLUSTERSTATE` token requests cross-shard
diagnostics. Unknown tokens and extra arguments return errors.

## Reply

The diagnostic reply is an array with one entry per index observed in the shard
reports. RESP3 entries are maps with two keys: `index` and `status`. A consistent
index has the string `"ok"` as its status. Every other status is a map containing
`warning` and, when available, shard lists:

```json
[
  {"index": "idx1", "status": "ok"},
  {
    "index": "idx2",
    "status": {
      "warning": "Inconsistent index state: index is missing from 1 of 3 reporting shards. Drop the index and recreate it so that all shards agree.",
      "missing_from_shards": ["<node-id>"]
    }
  }
]
```

RESP2 encodes each map, including the nested status map, as an alternating
key/value array:

```text
[
  ["index", "idx1", "status", "ok"],
  ["index", "idx2", "status",
    ["warning", "...", "missing_from_shards", ["<node-id>"]]]
]
```

| Status-map field | Meaning |
| --- | --- |
| `warning` | Explains confirmed inconsistency, uncertainty, or both. Always starts with `Inconsistent index state`. |
| `missing_from_shards` | Node IDs of reporting shards that do not list this index. |
| `unreachable_shards` | Expected node IDs with no usable identified report. Omitted if any shard rejected the request, because error replies cannot be attributed to node IDs. |

Shard lists are omitted when empty. Treat them as unordered collections of node
IDs, not numeric shard ordinals. Entry order is not a client contract.

There are no `"inconsistent"` or `"unknown"` status strings, and no top-level
`warning` field. Clients can distinguish `"ok"` from a diagnostic map directly;
the warning text distinguishes proven inconsistency from uncertainty.

Schema differences are described by a count in the warning. The reply does not
include `schema_mismatch_shards`, fingerprints, or groups of shards by schema.
It does not select an authoritative schema or label a particular schema as wrong.
This is the implemented contract for MOD-17813, superseding the ticket's original
example of string statuses and schema-mismatch attribution.

## Interpreting diagnostics

Missing-index diagnostics require a usable report from the shard. A shard that
does not answer is not evidence that the index is absent there. Rejections,
incompatible fingerprint recipes or encoding versions, and null fingerprints from
older peers produce uncertainty. Proven inconsistency remains visible
even when other shards could not be assessed.

Fingerprints are compared only among shards using the same fingerprint recipe and
index encoding version. The schema count is the largest distinct count within any
such comparable group; it is not a total across incompatible groups. Fingerprints
cover schema-defining state, including synonyms, but exclude documents, statistics,
and aliases. `"ok"` does not establish equality of indexed documents or aliases.

The result is a diagnostic snapshot, not an atomic view of the cluster. An
`FT.CREATE`, `FT.DROPINDEX`, `FT.ALTER`, or synonym update still propagating between
shards can produce temporary disagreement. **Wait for ongoing operations to finish
and rerun the command before acting on a drop-and-recreate recommendation.** The
command does not repair or modify indexes.

An incomplete fanout may omit indexes held only by shards that did not report.
If no observed index can carry a warning and reports are incomplete, the command
returns an error instead of an apparently authoritative empty list. A complete
set of empty shard reports returns `[]`. No usable reports also produces an error.

## Execution

Standalone and a single-shard cluster answer locally with `"ok"` for every local
index. Multi-shard diagnostics require a ready cluster and a blocking context;
they cannot run inside `MULTI`/`EXEC` or Lua. The no-token form remains local and
does not require a blocking context.

The internal `_FT._LIST WITHCLUSTERSTATE` payload is separate from the public reply:

```text
[node_id, fingerprint_recipe, index_encoding_version,
 [[index_name, fingerprint_or_null], ...]]
```

It uses the same array structure in RESP2 and RESP3. Older shards may reject this
new internal command; those replies are reported as uncertainty, not as schema
divergence. The Enterprise package excludes the internal command from its public
command list.

### Fingerprint implementation

The fingerprint streams schema values directly into SHA-1 and returns its leading
64 bits. It does not register a module type, invoke RDB save callbacks, or build a
temporary serialization. Numbers use a fixed byte order and strings have explicit
boundaries; pointers, struct padding, and live index state are never hashed.
Stopwords use sorted traversal and synonym membership hashes are order-independent.

Recipe `4` identifies this direct hash; earlier recipes `2` and `3` hashed RDB
bytes. Both use the same internal payload shape, so mixed recipes produce
uncertainty rather than a false schema mismatch. Current producers always return
an integer fingerprint, while the reducer still accepts older null fingerprints.
`rdbcompression` no longer affects fingerprints or comparability. The node ID may
be empty in standalone mode or before topology arrives.

The coordinator owns the target-node snapshot in the list command's private
request data. A generic callback captures it on the IO thread immediately before
fanout. The reducer uses an exact-name dictionary whose keys borrow the shard
replies, retaining their lengths without copying names. It validates complete
peer payloads before accumulating them; malformed payloads are incomplete reports.
