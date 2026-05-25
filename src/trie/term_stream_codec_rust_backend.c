/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/*
 * Rust-backed TermDictionary path for `sp->terms` RDB save/load. Bridges
 * the callback-shaped C codec (`term_stream_codec.{c,h}`) to the FFI
 * surface in `src/redisearch_rs/c_entrypoint/term_dict_ffi/`.
 *
 * Whole-file gate: only compiles when `USE_RUST_TERM_DICT` is defined by
 * the top-level cmake option. With the option OFF (default) this
 * translation unit is empty — the C `Trie *` path in `trie.c` is the only
 * RDB consumer. With the option ON, callers (`spec.c`) opt into the Rust
 * path by calling `TrieType_GenericSave_TermDict` /
 * `TrieType_GenericLoad_TermDict` directly.
 *
 * The `TrieType` module type registration in `trie.c:507` is untouched in
 * either configuration — `FT.SUGADD` / `FT.SUGGET` keys keep using the C
 * `Trie *` backend regardless of this option.
 */

#ifdef USE_RUST_TERM_DICT

#include "term_stream_codec_rust_backend.h"

#include "redismodule.h"
#include "term_dict_ffi.h"
#include "term_stream_codec.h"

typedef struct {
  const TermDict *dict;
  TermDictIter *it;
} TermDictEnumCtx;

typedef struct {
  TermDict *dict;
} TermDictSinkCtx;

static bool term_dict_enum(void *ctx, const char **term, size_t *term_len, double *score,
                           const char **payload, size_t *payload_len, size_t *num_docs) {
  TermDictEnumCtx *s = ctx;
  if (!s->it) {
    return false;
  }
  if (!TermDict_IterNext(s->it, term, term_len, score, num_docs)) {
    return false;
  }
  // TermEntry has no payload field; the codec writes the empty-payload
  // sentinel ("", 1) when save_payloads is on and *payload is NULL.
  *payload = NULL;
  *payload_len = 0;
  return true;
}

static int term_dict_sink(void *ctx, const char *term, size_t term_len, double score,
                          const char *payload, size_t payload_len, size_t num_docs) {
  TermDictSinkCtx *s = ctx;
  (void)payload;
  (void)payload_len;
  return TermDict_InsertRaw(s->dict, term, term_len, score, num_docs) ? 0 : -1;
}

void TrieType_GenericSave_TermDict(RedisModuleIO *rdb, TermDict *dict, bool save_payloads,
                                   bool save_num_docs) {
  TermDictEnumCtx ctx = {.dict = dict, .it = TermDict_IterNew(dict)};
  TermStream_RdbSave(rdb, (uint64_t)TermDict_Len(dict), term_dict_enum, &ctx, save_payloads,
                     save_num_docs);
  TermDict_IterFree(ctx.it);
}

TermDict *TrieType_GenericLoad_TermDict(RedisModuleIO *rdb, bool load_payloads,
                                        bool load_num_docs) {
  TermDictSinkCtx ctx = {.dict = TermDict_New()};
  int rc = TermStream_RdbLoad(rdb, term_dict_sink, &ctx, load_payloads, load_num_docs);
  if (rc != 0) {
    TermDict_Free(ctx.dict);
    return NULL;
  }
  return ctx.dict;
}

#endif /* USE_RUST_TERM_DICT */
