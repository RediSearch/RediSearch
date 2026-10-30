# Rust geo filter and aggregate port

## Scope

Extend `src/redisearch_rs/geo`, expose it through the existing `geo_ffi` crate,
and replace the C filter and aggregate implementations. Keep the existing
geohash and coordinate parser as the sources of truth. No new crate is needed.

- [x] Add distance units, filter construction/validation/radius evaluation.
- [x] Add legacy parsing with cursor conversion supplied by an adapter.
- [x] Add aggregate distance dispatch, parsing, rounding, and error behavior.
- [x] Cover public APIs with Rust integration tests.
- [x] Run targeted geo/geo_ffi tests, then the full Rust suite once.
- [x] Generate the C filter layouts and API in `geo_ffi.h`.
- [x] Bridge legacy cursors without replacing their numeric grammar or ownership.
- [x] Replace `geo_index.c` and leave `geo_index.h` as a compatibility include.
- [x] Retain only registry/result plumbing in `aggregate/functions/geo.c`.
- [x] Exercise C ABI boundaries, targeted Python tests, and final full suites.

## Core/adapter boundaries

- `GeoFilter` is a geometry/query specification, not an ABI-compatible C struct.
  Its field index uses the existing `rqe_core` type and invalid sentinel.
  The C `numericFilters` runtime cache stays with the iterator adapter: its array
  and elements have different allocator origins and must continue to use
  `GeoFilter_FreeNumericFilters`. Rust filter values need no explicit free;
  legacy field bytes are owned and released by `Drop`.
- Legacy numeric conversion is deliberately supplied through `LegacyGeoArgs`.
  Redis string cursors use `RedisModule_StringToDouble`; char/SDS cursors use
  `strtod`, with different whitespace, hex, NaN, infinity, and empty behavior.
  Replacing either with Rust float parsing would silently change behavior.
  The adapter uses the cursor's conversion-error text, propagates offset and
  the sticky empty flag even on errors, and maps errors to `QueryError` ParseArgs.
  Failed C parsing exposes a partially initialized output struct; Rust returns
  no filter on failure. Any adapter retaining that C contract must initialize
  the C output separately. Dialect checks and validation remain caller-owned.
- Validation preserves comparison-based NaN acceptance and the unit, coordinate,
  radius error order. Errors map to the C syntax error path. Radius evaluation
  still asserts finite coordinates, as the existing Rust-backed C helper does.
  This pre-existing validation/evaluation mismatch is not fixed by this port.
- Aggregate argument views distinguish numeric conversion from field decoding.
  The RSValue adapter follows references for field decoding and
  references plus trios for numeric conversion. String field parse errors update
  QueryError; other conversion failures only return NaN. Both are expression
  success. Arity is checked by the function registry before evaluation.
- Unit bytes preserve C-string NUL truncation, and the bounded unit parser rejects
  buffers at least 16 bytes long before looking for NUL.
- `geo_ffi` owns the generated C layouts, including the iterator cache pointer.
  Unit representation remains an integer so unknown C values stay representable.
  Bindgen consumes the same staged header for direct and transitive includes;
  otherwise `#pragma once` cannot prevent duplicate generated struct definitions.
  Existing Rust bindgen unit names remain compatibility aliases.

## Validation

Targeted iterations use `cargo nextest run -p geo -p geo_ffi` with the workspace
manifest. Final runs cover the full Rust workspace and C/C++ and Python suites.

Core-only checks, before integration:

- `./build.sh DEBUG=1`: passed, with generated headers unchanged.
- Targeted nextest: 119 passed, including 26 new tests.
- Workspace nextest: 3763 passed, none skipped.
- Targeted Clippy with all targets and warnings denied: passed.
- Targeted rustdoc with warnings denied: passed.

The initial targeted run caught two incorrect distance fixtures (exact coordinates
versus quantized cell centers); the corrected exact-coordinate result was also
checked with an independent haversine calculation. Build prerequisites needed
recovery from a shared sccache server retaining a deleted temporary directory.
An isolated socket and short stable temporary path avoided both that error and
macOS's socket-path length limit. Workspace tests initially stopped before test
execution because the default C bundle lookup selects release artifacts; setting
`BINDIR` to the debug build directory enabled the one complete workspace test run.

## Integration verification

- Pinned submodules initialized recursively; no dependency content is vendored.
  This fork inherited no jj conflicts. Copied dependency directories lacking
  Git metadata were moved aside before normal submodule initialization.
- Shared sccache used for all build/Cargo commands, without restarting it.
- Debug build and regenerated headers: passed.
- Targeted Rust: 119 passed, none skipped.
- Targeted C++: 11 passed, including two additional ABI tests for partial legacy
  output/cursor grammar and aggregate reference/trio views.
- `TEST=test_geo`: 14 passed.
- Full Rust workspace, one complete run: 3763 passed, none skipped.
- Full C/C++ suite, one complete run: 1391 passed, one existing skipped test.
- Workspace `make lint`: passed (Clippy and rustdoc, debug and release).
- `make fmt CHECK=1` and clang-format checks for the changed C/C++ integration
  files: passed.
- Full Python, one complete run: runner reported 2694 passed and 10 failed.
  Eight failures were command-probe Unix sockets exceeding macOS's path limit;
  all eight passed in a targeted `TEST=test_himport` rerun with `TMPDIR=/tmp`.
  The remaining failures match the pre-port baseline:
  `test_expire:testSortableFieldWithExpirationAndRegularField` and
  `test:test_with_tls` (server lacks TLS support).
  An earlier full-suite command stopped before tests because RedisJSON's
  repeat build could not locate Python; the completed run reused the exact
  RedisJSON module built and exercised by the targeted geo run.
- No ASan, Miri, or benchmarks were run for the integration.

## Reviewable change boundaries

The pure core and its tests are independent of the ABI handoff. The FFI files
separate filter lifecycle, cursor adaptation, and aggregate adaptation. The
generated header must accompany the corresponding Rust changes. A three-stage
port → FFI → swap stack can stage the FFI exports under temporary distinct
symbols before transferring the original C names at the swap; otherwise the
original-symbol exports and C removal must remain atomic to avoid duplicate
linker symbols. No history was rewritten or published during this integration.
