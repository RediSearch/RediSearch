/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redisearch.h"

#include <stdbool.h>

#ifdef __cplusplus
#define NODISCARD [[nodiscard]]
#define NOEXCEPT noexcept
#else
#define NODISCARD __attribute__((__warn_unused_result__))
#define NOEXCEPT __attribute__((__nothrow__))
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define GEO_VARIANTS(X) X(Cartesian) X(Geographic)

#define X(variant)                                                                               \
  struct RTDoc_##variant;                                                                        \
  NODISCARD struct RTDoc_##variant *RTDoc_##variant##_From_WKT(                                  \
      const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);                     \
  NODISCARD struct RTDoc_##variant *RTDoc_##variant##_Copy(struct RTDoc_##variant const *other); \
  void RTDoc_##variant##_Free(struct RTDoc_##variant *doc) NOEXCEPT;                             \
  NODISCARD t_docId RTDoc_##variant##_GetID(struct RTDoc_##variant const *doc) NOEXCEPT;         \
  NODISCARD bool RTDoc_##variant##_IsEqual(struct RTDoc_##variant const *lhs,                    \
                                           struct RTDoc_##variant const *rhs);                   \
  void RTDoc_##variant##_Print(struct RTDoc_##variant const *doc);                               \
  NODISCARD RedisModuleString *RTDoc_##variant##_ToString(struct RTDoc_##variant const *doc);

GEO_VARIANTS(X)
#undef X

#ifdef __cplusplus
}
#endif
