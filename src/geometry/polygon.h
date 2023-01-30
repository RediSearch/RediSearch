#pragma once

#include <stdbool.h>
#include "point.h"

#ifdef __cplusplus
extern "C" {
#endif

struct Polygon;

NODISCARD struct Polygon *Polygon_NewByCoords(int num_points, ...);
NODISCARD struct Polygon *Polygon_NewByPoints(int num_points, ...);
NODISCARD struct Polygon *Polygon_Copy(struct Polygon const *other);
void Polygon_Free(struct Polygon *polygon) NOEXCEPT;
NODISCARD bool Polygon_IsEqual(struct Polygon const *lhs, struct Polygon const *rhs);

void Polygon_Print(struct Polygon const *poly);

#ifdef __cplusplus
}
#endif

