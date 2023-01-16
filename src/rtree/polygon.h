#pragma once

#include <stdbool.h>
#include "point.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Polygon;

struct Polygon *Polygon_NewByCoords(int num_points, ...);
struct Polygon *Polygon_NewByPoints(int num_points, ...);
struct Polygon *Polygon_Copy(struct Polygon const *other);
void Polygon_Free(struct Polygon *polygon);
bool Polygon_IsEqual(struct Polygon const *lhs, struct Polygon const *rhs);

#ifdef __cplusplus
}
#endif

