/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef TERM_STREAM_CODEC_RUST_BACKEND_H__
#define TERM_STREAM_CODEC_RUST_BACKEND_H__

#ifdef USE_RUST_TERM_DICT

#include <stdbool.h>

#include "redismodule.h"
#include "term_dict_ffi.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Save the contents of `dict` to `rdb` in the on-disk format produced by
 * the C `TrieType_GenericSave`. Iterates via TermDict_IterNew /
 * TermDict_IterNext / TermDict_IterFree under the hood. */
void TrieType_GenericSave_TermDict(RedisModuleIO *rdb, TermDict *dict, bool save_payloads,
                                   bool save_num_docs);

/* Load tuples off `rdb` (in the format produced by TrieType_GenericSave)
 * into a fresh TermDict. Returns NULL on I/O or sink error. */
TermDict *TrieType_GenericLoad_TermDict(RedisModuleIO *rdb, bool load_payloads,
                                        bool load_num_docs);

#ifdef __cplusplus
}
#endif

#endif /* USE_RUST_TERM_DICT */
#endif /* TERM_STREAM_CODEC_RUST_BACKEND_H__ */
