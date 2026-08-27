# Tasks: result-processor drain API

Each top-level task maps to one PR in the agreed stack. A processor PR includes
its focused unit and concurrency tests rather than deferring them to the end.

Every processor PR uses the processor's existing implementation language. The
already-Rust counter remains in Rust; no additional C-to-Rust processor
migration is part of this stack.

## 1. Shared API and contract

- [x] Add the C Drain vtable entry, public dispatcher, and documented concurrency contract
- [x] Add a dedicated RESULT/EOF/ERROR Drain status and reject timeout/pending statuses
- [x] Provide null-Drain default propagation during the stack without using `Next` pointer identity
- [x] Keep Drain off shared `resultLimit`; pass a private budget from the caller
- [x] Keep the API aligned with QueryRequest blocked-client ownership and lifetime work; add no independent query-level handoff
- [x] Add Drain to the Rust ResultProcessor trait and upstream-chain abstraction
- [x] Replace whole-processor mutable borrowing in concurrent Rust FFI entry points with a sound shared-access model
- [x] Prevent the Rust Drain path from using single-thread-only `QueryProcessingCtx` mutation helpers
- [x] Mechanically adapt the existing Rust counter to shared trait access so the API PR compiles
- [x] Keep the Rust ResultProcessor header layout compatible with C and add compile-time layout checks
- [ ] Add synthetic propagation, serialization, handoff, idempotence, and exactly-once cleanup tests
- [x] Add pure Rust concurrent `Next`/Drain contract tests

## 2. Source and loader processors

- [x] Implement and test `RP_INDEX`
- [ ] Implement and test `RP_NETWORK`
- [ ] Implement and test `RP_LOADER`
- [ ] Implement and test `RP_SAFE_LOADER`
- [ ] Implement and test `RP_DISK_ASYNC_LOADER`, including the paired provider change

## 3. Streaming transformation processors

- [ ] Implement and test `RP_SCORER`
- [ ] Implement and test `RP_METRICS`
- [ ] Implement and test `RP_KEY_NAME_LOADER`
- [ ] Implement and test `RP_VECTOR_NORMALIZER`
- [ ] Implement and test `RP_HIGHLIGHTER`
- [ ] Implement and test `RP_PROJECTOR`
- [ ] Implement and test `RP_FILTER`
- [ ] Implement and test `RP_PROFILE`
- [ ] Implement and test the Rust `RP_COUNTER`
- [ ] Implement and test `RP_PAGER_LIMITER`

## 4. Buffering and fan-in processors

- [ ] Implement and test `RP_SORTER`, moving its timeout accumulation-to-yield transition into Drain
- [ ] Implement and test `RP_MAX_SCORE_NORMALIZER`
- [ ] Implement and test `RP_GROUP`
- [ ] Implement and test `RP_DEPLETER`
- [ ] Implement and test `RP_SAFE_DEPLETER`
- [ ] Implement and test `RP_HYBRID_MERGER`

## 5. Debug processors

- [ ] Implement and test `RP_TIMEOUT`
- [ ] Implement and test `RP_CRASH`
- [ ] Implement and test `RP_CRASH_IN_RUST`
- [ ] Implement and test `RP_PAUSE`

## 6. Controlled-path integration

- [ ] Invoke Drain from an existing synchronized timeout cleanup or fault-injection path
- [ ] Prove AREQ and HybridRequest pipeline support
- [ ] Verify that timeout replies and cursor behavior remain unchanged

## 7. Final verification

- [ ] Run C/C++ ResultProcessor unit tests
- [ ] Run Rust result-processor and FFI tests
- [ ] Run aggregate and hybrid Python tests in standalone and cluster modes
- [ ] Run ASAN tests for stateful and asynchronous processors
- [ ] Run the full build, formatting checks, and lint
- [ ] Update the result-processor-drain delta spec to match the approved contract
