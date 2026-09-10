# Design

`src/spec.c` parses the options into `HNSWParams.quantType` and
`TieredHNSWParams.QuantNormalizationSetSize`. `quantParams` remains null: VecSim
learns the mean from the indexed vectors. Both FLOAT32 and FLOAT16 support L2,
IP, and cosine, with or without training. The HNSW backend exists from index
construction and remains empty while the full-precision frontend accumulates
the training sample.

Compression and training options are limited to in-memory indexes. Disk indexes
reject these options because their provider has a separate compression lifecycle
and does not implement the tiered training threshold. The common parameter
validator rejects compressed disk configurations during creation and RDB loading.

SQ8 dimensions are capped at `UINT32_MAX / UINT8_MAX` because VecSim stores the
sum of quantized bytes in a 32-bit accumulator. Enforce the bound before size
estimation on both creation and RDB load. Uncompressed HNSW keeps its existing
dimension validation.

`FT.INFO` reads the stored field configuration. It does not describe whether
training has completed; a small compressed index may still use flat storage.

## Migration without workers

Accumulation uses the frontend regardless of the worker setting. At the training
threshold, VecSim checks the current write mode: asynchronous mode submits the
pending insertion jobs, while write-in-place mode executes them synchronously.
This follows SVS behavior and supports disabling workers during accumulation.
The threshold-crossing write can take longer because it migrates the entire
training set before returning.

The HLD's section 3.2 describes queue submission at the transition but omits the
zero-worker case. This fallback completes that behavior and also permits RDB
rebuilds with both `WORKERS 0` and `MIN_OPERATION_WORKERS 0`.

## Resize limits

Tiered SQ8 uses one block size for the full-precision frontend and compressed
HNSW backend. Choose that size using the larger of the two element estimates.
The backend estimate receives the tiered parameters so it includes mean
normalization when enabled. Apply this validation both to creation and RDB load;
loading under a smaller memory limit recomputes the block size or rejects an
element that cannot fit. Uncompressed HNSW retains its existing estimate.

## Persistence

Index encoding version 28 adds two unsigned values after HNSW epsilon:
compression type and training threshold. `VecSim_RdbLoad_v5` reads both values;
the v4 reader retains the old layout and supplies no-compression defaults. The
new values are validated before narrowing to their destination types. Truncated
or unsupported configurations fail loading through the existing error path.

As with existing in-memory search indexes, reload rebuilds SQ8 from the original
HASH/JSON vectors. The learned mean, running sum, and HNSW graph are not persisted.
The training sample can differ because replay order and the live document set can
differ. Approximate distances and rankings need not be identical across reloads.
If deletions leave fewer vectors than the threshold, a previously trained index
returns to flat accumulation until enough new vectors arrive. Existing vectors
remain searchable throughout. The original zero-threshold setting is preserved.

Persisting the learned state would require a separate VecSim API and an index-state
persistence design, beyond the two parameter fields specified by the HLD. This
change follows the existing rebuild model; the reload consequences above are
explicitly covered for maintainer review.

## Compatibility

Older RDBs continue to load, with `compression: NO_COMPRESSION`. Older binaries
cannot read encoding version 28. FLAT and SVS parameter layouts are unchanged.
There is no runtime change to an existing field's compression configuration.
Converting a pre-tiered HNSW field clears the reused parameter storage before
initializing the tiered wrapper, so saving it in the current format preserves
the zero training threshold. Legacy fixtures are loaded, saved, and loaded again.

The encoding-version change also applies to schema propagation during slot
migration (`Indexes_Propagate`) and replication/RDB transfers. It affects every
schema written by this build, including indexes without SQ8 fields. There is no
destination-version negotiation in the module. For the release adopting this
format, destinations must support encoding v28 before they receive schemas or
RDB data from upgraded nodes. New-to-old replication and slot migration are not
supported during a mixed-version upgrade. A downgrade requires a compatible
pre-upgrade backup; saving with the new binary does not produce an old-format
backup. The release rollout must enforce this ordering.
