# Design

`src/spec.c` parses the options into `HNSWParams.quantType` and
`TieredHNSWParams.QuantNormalizationSetSize`. `quantParams` remains null: VecSim
learns the mean from the indexed vectors. Validation runs before index creation,
including rejection of FLOAT16 L2 with a nonzero training threshold, which the
current VecSim factory does not support.

`FT.INFO` reads the stored field configuration. It does not describe whether
training has completed; a small compressed index may still use flat storage.

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
