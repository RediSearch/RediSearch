# HNSW SQ8

## Creation and reporting

- HNSW accepts case-insensitive `COMPRESSION SQ8` for FLOAT32 and FLOAT16.
- Without compression, `FT.INFO` reports `compression: NO_COMPRESSION` and omits
  `training_threshold`.
- With compression, `FT.INFO` reports `compression: SQ8` and the configured
  `training_threshold`, even while the index is still accumulating vectors.
- An omitted threshold defaults to 10240. Explicit zero disables mean
  normalization. The maximum is 102400, inclusive.
- The threshold may appear before compression. A threshold without compression,
  an unsupported compression/type, a negative or non-integer threshold, or a value
  above the maximum is rejected.
- FLOAT16 L2 requires an explicit zero threshold until VecSim supports mean
  normalization for that combination.
- SQ8 accepts dimensions up to 16,843,009, inclusive. Larger dimensions are
  rejected during creation and RDB loading.
- The resize limit bounds the shared block size using both full-precision
  frontend and compressed backend element estimates, including when the training
  threshold is zero. Reject creation if one element cannot fit.

## Worker settings

- `WORKERS 0` remains supported. Vectors accumulate in the frontend until the
  training threshold is reached, then migrate synchronously before the write
  returns. No insertion jobs are left waiting for unavailable workers.
- With workers enabled, migration uses background jobs.
- The worker setting at the transition applies, including when workers were
  disabled during accumulation.
- RDB rebuilds support disabling both regular and temporary loading workers
  with `WORKERS 0 MIN_OPERATION_WORKERS 0`, before and after training.

## Save and reload

- RDB stores compression configuration and original source vectors. Rebuilding
  recomputes training state and does not guarantee identical approximate scores
  or rankings.
- A save during accumulation reloads with all remaining source vectors searchable
  in the flat frontend. New vectors can complete training and migrate the index.
- A save after training reloads with all remaining source vectors searchable. If
  their count reaches the threshold, training and migration complete again.
- If a trained index has fallen below the threshold after deletions, reload
  returns it to accumulation. Compression configuration remains unchanged.
- A zero threshold stays zero on reload and bypasses accumulation.
- Legacy RDB versions load with no compression and remain loadable after saving
  in the current format. Invalid compression settings and missing new-format
  parameter fields are rejected.
- Reload recomputes the block size using the current memory limits for both
  tiers. Reject loading if a full-precision frontend element cannot fit.
