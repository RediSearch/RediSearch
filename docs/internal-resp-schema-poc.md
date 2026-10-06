# Internal RESP field schema POC

This opt-in experiment applies only to distributed `FT.AGGREGATE` shard replies.
Public replies, SEARCH, HYBRID, reducers, numeric encoding and persistence are unchanged.

Enable `CONFIG SET search-internal-resp-schema yes` on every node of a compatible,
homogeneous deployment, or add `search-internal-resp-schema yes` to each Redis
configuration file. The default is `no`. Mixed versions are unsupported when enabled;
there is no capability negotiation. The coordinator requests this format with the
internal `_RESP_SCHEMA` argument; client commands cannot request it. Enablement is
snapshotted when a request is created, before worker dispatch. A configuration
change applies to new requests; existing requests and their cursors retain their format.

Within existing count, cursor, profile, warning and error envelopes, the internal
results payload is `["__resp_schema_v1", rows, schema]`. RESP2 `rows` keeps its
leading total count. RESP3 counts stay in the existing metadata map.

`schema` contains field names once per chunk. Each row is `[presence, values]`.
Values use the existing RESP serialization, including numeric markers, JSON
selection, nested values and ADDSCORES fields.
A null presence means every column of that row's schema prefix is present.
Otherwise presence is an ASCII `0`/`1` string; values contain only the present
columns, in schema order. Missing values are never represented by explicit nulls.

The schema is written after rows. LOAD * may append fields as the streaming pipeline
reads documents; earlier rows refer to a shorter prefix and omit later columns.
This uses the sealed lookup's append-only invariant and needs neither row buffering
nor a legacy fallback for changing schemas. Buffered FAIL and RETURN-STRICT replies
use the same representation with the final lookup schema.

The coordinator validates row widths, masks and value counts before exposing any
row, then resolves lookup keys once per chunk using temporary name indexes. Wide
LOAD * chunks require a presence mask for each sparse row, so this POC does not
promise a speedup for schemas with many fields unique to individual documents.
The mapping belongs to that chunk and
is freed when its reply is released. The legacy decoder remains available, including
for errors and empty replies produced before normal execution. Manual internal
requests asking for metadata slots (raw IDs, scores, payloads, sort keys or required
fields) retain the legacy format; distributed aggregate does not request these slots.

Focused regression coverage lives in `tests/pytests/test_resp_schema.py`. Raw internal
reply assertions verify that the format is used, including schema growth during
streaming. Public replies are compared against the disabled configuration in both
protocols, including cursor reads, counts, sparse fields, buffered timeout policies,
scores and JSON formats.
