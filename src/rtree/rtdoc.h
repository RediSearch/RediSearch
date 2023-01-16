#pragma once

#include <stdbool.h>
#include "point.h"
#include "polygon.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RTDoc;

struct RTDoc *RTDoc_New(struct Polygon const *polygon);
struct RTDoc *RTDoc_Copy(struct RTDoc const *other);
void RTDoc_Free(struct RTDoc *RTDoc);
struct Point const *RTDoc_MinCorner(struct RTDoc const *RTDoc);
struct Point const *RTDoc_MaxCorner(struct RTDoc const *RTDoc);
bool RTDoc_IsEqual(struct RTDoc const *lhs, struct RTDoc const *rhs);

#ifdef __cplusplus
}
#endif

