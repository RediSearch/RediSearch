#pragma once

#include <stdbool.h>
#include "../redismodule.h"

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
typedef size_t docID_t;

NODISCARD struct RTDoc *From_WKT(const char *wkt, size_t len, docID_t id);
NODISCARD struct RTDoc *RTDoc_Copy(struct RTDoc const *other);
void RTDoc_Free(struct RTDoc *doc) NOEXCEPT;
NODISCARD docID_t RTDoc_GetID(struct RTDoc const *doc) NOEXCEPT;
NODISCARD bool RTDoc_IsEqual(struct RTDoc const *lhs, struct RTDoc const *rhs);

void RTDoc_Print(struct RTDoc const *doc);
NODISCARD RedisModuleString *RTDoc_ToString(struct RTDoc const *doc);

#ifdef __cplusplus
}
#endif
