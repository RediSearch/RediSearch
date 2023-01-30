#pragma once

#include <stdbool.h>
#include "point.h"
#include "polygon.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTDoc;
typedef size_t docID_t;

NODISCARD struct RTDoc *RTDoc_New(struct Polygon const *polygon, docID_t id);
NODISCARD struct RTDoc *RTDoc_Copy(struct RTDoc const *other);
void RTDoc_Free(struct RTDoc *doc) NOEXCEPT;
NODISCARD docID_t RTDoc_GetID(struct RTDoc const *doc) NOEXCEPT;
NODISCARD struct Polygon *RTDoc_GetPolygon(struct RTDoc const *doc);
NODISCARD struct Point *RTDoc_MinCorner(struct RTDoc const *RTDoc);
NODISCARD struct Point *RTDoc_MaxCorner(struct RTDoc const *RTDoc);
NODISCARD bool RTDoc_IsEqual(struct RTDoc const *lhs, struct RTDoc const *rhs);

void RTDoc_Print(struct RTDoc const *doc);

#ifdef __cplusplus
}
#endif

