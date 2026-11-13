---
name: vecsim-error-messages
description: >-
  Use when adding, changing, or reviewing a rejection branch in VecSimIndex_ResolveParams's
  _ResolveParams_* helpers and the caller needs to know *why* a query param was rejected, not
  just that it was. Covers the SetResolveErr mechanism, how to phrase messages consistently,
  and how RediSearch currently consumes them.
---

# VecSim descriptive error messages (query-time)

`VecSimIndex_ResolveParams` used to return only a `VecSimResolveCode` with no specifics — callers
got a generic canned message regardless of which parameter or constraint actually failed.
The API now surfaces the specific reason for this path.

Index-creation-time errors (`VecSimIndex_New` returning `NULL`) were considered separately and are outside this mechanism — that path still has no descriptive-error mechanism; don't assume one exists
there.

## `VecSimIndex_ResolveParams`

Lives in `src/vecsim/src/VecSim/vec_sim.cpp`. Each `_ResolveParams_<Name>` static helper (EFRuntime, Epsilon,
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

RediSearch (`src/vector_index.c`, `VecSim_ResolveQueryParams`) maps the return code
to its own `QueryErrorCode` and forwards the detail with `QueryError_SetWithUserDataFmt`.
The detail can contain user-supplied names and values: keep it in the user-data
argument so obfuscated errors do not disclose it. Do not concatenate it into the
safe default message. Test both the detailed client error and its obfuscated form
when changing this boundary.
