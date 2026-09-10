# Implementation checklist

- [x] Parse and validate HNSW compression and training options.
- [x] Report configuration in `FT.INFO`, including uncompressed indexes.
- [x] Version RDB parameters and retain legacy readers.
- [x] Update the local dependency to VectorSimilarity #1029's current head.
- [x] Cover parsing, reporting, invalid/truncated RDB parameters, and reloads
      during accumulation and after training.
- [x] Build and run unit, focused, and full standalone behavioral tests on
      `dorer-intel`; results are in [verification.md](verification.md).
- [x] Align with the final head of the #1034 → #1035 → #1029 stack and validate
      the build, units, and behavioral tests on `arm-r8g.xlarge`.
- [x] Update the SVS GC fixture for the dependency's retained spare block.
- [x] Resolve or account for the previous full-suite failures; the expiration
      assertion remains unresolved, as recorded in [verification.md](verification.md).
- [x] Obtain an independent review.
- [x] Enforce the frontend resize limit during creation and RDB loading.
- [x] Resolve workerless SQ8 migration in VecSim and verify it in RediSearch.
- [x] Merge current `master`, keep its SVS GC fixture, and refresh the provisional
      VecSim pin to `0e71fad4`.
- [x] Reproduce and fix legacy HNSW save/reload rejection after pre-tiered RDB conversion.
- [x] Confirm SQ8 byte-sum overflow and reject unsupported dimensions on creation and reload.
- [x] Refresh the provisional VecSim pin to the reviewed stack head `fb2c5b04`.
- [x] Enable FLOAT16 L2 training and expect an empty backend during accumulation.
- [x] Reject SQ8 disk fields during creation and RDB loading.
- [x] Reject zero dimensions and invalid metrics in SQ8 RDB parameters.
- [x] Verify scores and ranking for KNN, both hybrid policies, range queries,
      and multi-value JSON through training and reload.
- [x] Document the encoding-v28 boundary for replication and slot migration.
- [ ] Replace the provisional dependency pin with the merged VecSim commit.
- [ ] Obtain maintainer review and green CI before merge.
