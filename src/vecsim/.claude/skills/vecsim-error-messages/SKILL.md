---
name: vecsim-error-messages
description: >-
  Use when adding, changing, or reviewing a rejection branch in VecSimIndex_ResolveParams's
  _ResolveParams_* helpers and the caller needs to know *why* a query param was rejected, not
  just that it was. Covers the SetResolveErr mechanism, how to phrase messages consistently,
  and how RediSearch currently consumes them (as of MOD-11888).
---

# VecSim descriptive error messages (query-time)

`VecSimIndex_ResolveParams` used to return only a `VecSimResolveCode` with no specifics — callers
got a generic canned message regardless of which parameter or constraint actually failed.
MOD-11888 added a way to surface the specific reason for this path.

Index-creation-time errors (`VecSimIndex_New` returning `NULL`) were considered under the same
ticket but descoped — that path still has no descriptive-error mechanism; don't assume one exists
there.

## `VecSimIndex_ResolveParams`

Lives in `src/VecSim/vec_sim.cpp`. Each `_ResolveParams_<Name>` static helper (EFRuntime, Epsilon,
SearchWS, SearchBC, UseSearchHistory, BatchSize, Rerank, HybridPolicy) takes a trailing
`const char **err_msg` and returns via the `SetResolveErr(err_msg, code, fmt, ...)` helper instead
of a bare `return VecSimParamResolverErr_X;`. `SetResolveErr` printf-formats into a thread-local
buffer and points `*err_msg` at it; passing `nullptr` for `err_msg` (as most existing tests do) is
always safe — every write is gated on `err_msg != nullptr`.

When adding a new rejection branch here:
- Always go through `SetResolveErr`, never return a bare code.
- Name the parameter using the existing `VecSimCommonStrings::*_STRING` constant (e.g.
  `VecSimCommonStrings::EPSILON_STRING`), not a hardcoded literal — it's the same string already
  used to match the param name in the dispatch `if`/`else if` chain above.
- State the *constraint*, not just the rejection: `"%s is only valid for HNSW or SVS indexes"`
  reads better than `"invalid parameter"`. If the message needs the offending value or name
  (unknown-param, invalid hybrid policy value), interpolate it — `rparam.name`/`rparam.value` are
  guaranteed null-terminated (the same fields are already passed straight to `strcasecmp`/
  `strtoll` elsewhere in this function).
- The message pointer is thread-local and lives only until the next `VecSimIndex_ResolveParams`
  call on the same thread — callers must read it before making another resolve call.

## Downstream: RediSearch

RediSearch (`src/vector_index.c`, `VecSim_ResolveQueryParams`) currently maps `VecSimResolveCode`
to one of its own fixed `QueryErrorCode`s and always uses that code's canned default message —
it does not yet forward VecSim's new `err_msg` text into the client-facing error. Wiring that up
(and updating RediSearch's error-message tests to match) is the next step of MOD-11888 and is
tracked separately; don't assume it's done just because the VecSim side is.
