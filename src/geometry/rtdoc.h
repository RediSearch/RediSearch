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

struct RTDoc_Cartesian;
struct RTDoc_Geographic;

NODISCARD struct RTDoc_Cartesian *RTDoc_Cartesian_From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
NODISCARD struct RTDoc_Geographic *RTDoc_Geographic_From_WKT(const char *wkt, size_t len, t_docId id, RedisModuleString **err_msg);
NODISCARD struct RTDoc_Cartesian *RTDoc_Cartesian_Copy(struct RTDoc_Cartesian const *other);
NODISCARD struct RTDoc_Geographic *RTDoc_Geographic_Copy(struct RTDoc_Geographic const *other);
void RTDoc_Cartesian_Free(struct RTDoc_Cartesian *doc) NOEXCEPT;
void RTDoc_Geographic_Free(struct RTDoc_Geographic *doc) NOEXCEPT;
NODISCARD t_docId RTDoc_Cartesian_GetID(struct RTDoc_Cartesian const *doc) NOEXCEPT;
NODISCARD t_docId RTDoc_Geographic_GetID(struct RTDoc_Geographic const *doc) NOEXCEPT;
NODISCARD bool RTDoc_Cartesian_IsEqual(struct RTDoc_Cartesian const *lhs, struct RTDoc_Cartesian const *rhs);
NODISCARD bool RTDoc_Geographic_IsEqual(struct RTDoc_Geographic const *lhs, struct RTDoc_Geographic const *rhs);

void RTDoc_Cartesian_Print(struct RTDoc_Cartesian const *doc);
void RTDoc_Geographic_Print(struct RTDoc_Geographic const *doc);
NODISCARD RedisModuleString *RTDoc_Cartesian_ToString(struct RTDoc_Cartesian const *doc);
NODISCARD RedisModuleString *RTDoc_Geographic_ToString(struct RTDoc_Geographic const *doc);

#ifdef __cplusplus
}
#endif
