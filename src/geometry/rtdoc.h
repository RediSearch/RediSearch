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

struct RTDoc;

NODISCARD struct RTDoc *From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
NODISCARD struct RTDoc *RTDoc_Copy(struct RTDoc const *other);
void RTDoc_Free(struct RTDoc *doc) NOEXCEPT;
NODISCARD t_docId RTDoc_GetID(struct RTDoc const *doc) NOEXCEPT;
NODISCARD bool RTDoc_IsEqual(struct RTDoc const *lhs, struct RTDoc const *rhs);

void RTDoc_Print(struct RTDoc const *doc);
NODISCARD RedisModuleString *RTDoc_ToString(struct RTDoc const *doc);

#ifdef __cplusplus
}
#endif
