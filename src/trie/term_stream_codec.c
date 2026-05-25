/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "term_stream_codec.h"

#include "rdb.h"
#include "trie/trie.h"

void TermStream_RdbSave(RedisModuleIO *rdb, uint64_t count, TermEnumFn next, void *ctx,
                        bool save_payloads, bool save_num_docs) {
  RedisModule_SaveUnsigned(rdb, count);

  const char *term = NULL;
  size_t term_len = 0;
  double score = 0;
  const char *payload = NULL;
  size_t payload_len = 0;
  size_t num_docs = 0;

  while (next(ctx, &term, &term_len, &score, &payload, &payload_len, &num_docs)) {
    RedisModule_SaveStringBuffer(rdb, term, term_len + 1);
    RedisModule_SaveDouble(rdb, score);

    if (save_payloads) {
      // save an extra space for the null terminator to make the payload null terminated on load
      if (payload != NULL && payload_len > 0) {
        RedisModule_SaveStringBuffer(rdb, payload, payload_len + 1);
      } else {
        // If there's no payload - we save an empty string
        RedisModule_SaveStringBuffer(rdb, "", 1);
      }
    }
    if (save_num_docs) {
      RedisModule_SaveUnsigned(rdb, num_docs);
    }
  }
}

int TermStream_RdbLoad(RedisModuleIO *rdb, TermSinkFn install, void *ctx, bool load_payloads,
                       bool load_num_docs) {
  char *str = NULL;
  char *payload_data = NULL;
  uint64_t elements = LoadUnsigned_IOError(rdb, goto cleanup);

  while (elements--) {
    size_t len;
    size_t payload_len = 0;
    str = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
    double score = LoadDouble_IOError(rdb, goto cleanup);
    if (load_payloads) {
      payload_data = LoadStringBuffer_IOError(rdb, &payload_len, goto cleanup);
      // load an extra space for the null terminator
      payload_len--;
    }
    size_t num_docs = 0;
    if (load_num_docs) {
      num_docs = LoadUnsigned_IOError(rdb, goto cleanup);
    }
    int rc =
        install(ctx, str, len - 1, score, payload_len ? payload_data : NULL, payload_len, num_docs);
    RedisModule_Free(str);
    str = NULL;
    if (payload_data != NULL) {
      RedisModule_Free(payload_data);
      payload_data = NULL;
    }
    if (rc != 0) {
      RedisModule_LogIOError(rdb, "warning",
                             "RDB Load: Failed to insert trie entry (payload overflow)");
      goto cleanup;
    }
  }
  return 0;

cleanup:
  if (str) {
    RedisModule_Free(str);
  }
  if (payload_data) {
    RedisModule_Free(payload_data);
  }
  return -1;
}
