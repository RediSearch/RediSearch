/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef TERM_STREAM_CODEC_H__
#define TERM_STREAM_CODEC_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Yields the next (term, score, payload, num_docs) tuple from a term-stream
 * source. Returns true on a real tuple, false when the stream is exhausted.
 *
 * Out-parameter lifetime: `*term` and `*payload` are owned by the callback's
 * context and remain valid only until the next call to this function OR until
 * the matching enum-dispose step on the source's side, whichever comes first.
 * Matches the lifetime contract of the C `TrieIterator` (which owns one
 * `runesToStr` scratch buffer per step).
 *
 * Buffer shape: the codec writes the trailing NUL byte to the wire to preserve
 * the legacy on-disk format. Implementations MUST guarantee `(*term)[*term_len]
 * == '\0'` so the codec can write `*term_len + 1` bytes. Same requirement
 * applies to `*payload` when `*payload_len > 0`.
 *
 * `*payload` may be set to NULL with `*payload_len == 0` when the source has
 * no payload to emit; the codec writes the empty-payload sentinel ("", 1) in
 * that case. */
typedef bool (*TermEnumFn)(void *ctx, const char **term, size_t *term_len, double *score,
                           const char **payload, size_t *payload_len, size_t *num_docs);

/* Installs one (term, score, payload, num_docs) tuple into the sink's
 * backing container. Returns 0 on success, non-zero on a sink-level error
 * (e.g. payload overflow). On non-zero the codec logs via
 * RedisModule_LogIOError and aborts the load.
 *
 * The pointers `term` / `payload` are valid only for the duration of the
 * call. If the sink needs to retain the bytes it must copy them. */
typedef int (*TermSinkFn)(void *ctx, const char *term, size_t term_len, double score,
                          const char *payload, size_t payload_len, size_t num_docs);

/* Save `count` term tuples to `rdb` by repeatedly calling `next(ctx, ...)`.
 * The caller is responsible for matching `count` to the number of tuples the
 * callback will yield; mismatch is the caller's bookkeeping, not the codec's.
 *
 * `save_payloads` and `save_num_docs` gate the optional trailing fields,
 * mirroring the legacy TrieType_GenericSave flags. */
void TermStream_RdbSave(RedisModuleIO *rdb, uint64_t count, TermEnumFn next, void *ctx,
                        bool save_payloads, bool save_num_docs);

/* Load term tuples from `rdb` by repeatedly calling `install(ctx, ...)` for
 * each entry read off the stream. Returns 0 on success, non-zero on I/O or
 * sink error. The caller owns the backing container created by its sink;
 * the codec does not own backend state and does not free it on error. */
int TermStream_RdbLoad(RedisModuleIO *rdb, TermSinkFn install, void *ctx, bool load_payloads,
                       bool load_num_docs);

#ifdef __cplusplus
}
#endif
#endif
