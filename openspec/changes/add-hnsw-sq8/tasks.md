# Implementation checklist

- [x] Parse and validate HNSW compression and training options.
- [x] Report configuration in `FT.INFO`, including uncompressed indexes.
- [x] Version RDB parameters and retain legacy readers.
- [x] Update the local dependency to VectorSimilarity #1029's current head.
- [x] Cover parsing, reporting, invalid/truncated RDB parameters, and reloads
      during accumulation and after training.
- [ ] Complete builds and unit/behavioral verification on `dorer-intel`.
- [ ] Obtain independent review and resolve findings.
- [ ] Replace the provisional dependency pin with the merged VecSim commit.
- [ ] Obtain maintainer review and green CI before merge.
