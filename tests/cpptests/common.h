/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "redismock/redismock.h"
#include "redismock/util.h"
#include "spec.h"
#include "document.h"

#ifdef __cplusplus
#include <chrono>
#include <functional>
#include <sstream>
#endif

#define __ignore__(X) \
    do { \
        int rc = (X); \
        if (rc == -1) \
            ; \
    } while(0)

#define get_spec(x) ((IndexSpec*)__RefManager_Get_Object(x))

#ifdef __cplusplus
extern "C" {
#endif
#include "rwlock.h"
#ifdef __cplusplus
}
#endif

namespace RS {

static void donecb(RSAddDocumentCtx *aCtx, RedisModuleCtx *, void *) {
  // printf("Finished indexing document. Status: %s\n", QueryError_GetUserError(&aCtx->status));
}

template <typename... Ts>
bool addDocument(RedisModuleCtx *ctx, RSIndex *index, const char *docid, Ts... args) {
  RWLOCK_ACQUIRE_WRITE();
  RMCK::ArgvList argv(ctx, args...);
  AddDocumentOptions options = {0};
  options.numFieldElems = argv.size();
  options.fieldsArray = argv;
  options.donecb = donecb;
  options.keyStr = RedisModule_CreateString(ctx, docid, strlen(docid));
  options.score = 1.0;
  options.options = DOCUMENT_ADD_REPLACE;

  QueryError status = QueryError_Default();
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(index));
  int rv = RS_AddDocument(&sctx, RMCK::RString(docid), &options, &status);
  RedisModule_FreeString(ctx, options.keyStr);
  RWLOCK_RELEASE();
  return rv == REDISMODULE_OK;
}

bool deleteDocument(RedisModuleCtx *ctx, RSIndex *index, const char *docid);

template <typename... Ts>
IndexSpec *createIndex(RedisModuleCtx *ctx, const char *name, Ts... args) {
  RMCK::ArgvList argv("FT.CREATE", name, args...);
  QueryError err{QueryErrorCode(0)};
  IndexSpec *sp = IndexSpec_CreateNew(ctx, argv, argv.size(), &err);
  if (!sp) {
    abort();
  }
  return sp;
}

std::vector<std::string> search(RSIndex *index, RSQueryNode *qn);
std::vector<std::string> search(RSIndex *index, const char *s);

/**
 * @brief Wait for a condition to become true with a timeout.
 *
 * This function polls the condition at regular intervals until it becomes true
 * or the timeout expires.
 *
 * @tparam Condition A callable that returns bool (e.g., lambda, function pointer)
 * @param condition The condition to wait for (should return true when satisfied)
 * @param timeout_s Timeout in seconds (default: 30s)
 * @param poll_interval_us Polling interval in microseconds (default: 100us)
 * @return true if condition became true before timeout, false if timeout expired
 *
 * Example usage:
 *   bool success = WaitForCondition([&]() { return counter == 0; }, 300);
 *   ASSERT_TRUE(success) << "Timeout waiting for counter to reach 0";
 *
 */
template<typename Condition>
bool WaitForCondition(Condition condition,
                      int timeout_s = 30,
                      int poll_interval_us = 100) {
  auto start = std::chrono::steady_clock::now();
  auto timeout = std::chrono::seconds(timeout_s);

  while (!condition()) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > timeout) {
      return false; // Timeout
    }
    usleep(poll_interval_us);
  }
  return true; // Success
}

}  // namespace RS
