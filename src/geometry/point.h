#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

struct Point;

struct Point *Point_New(double x, double y);
struct Point *Point_Copy(struct Point const *other);
void Point_Free(struct Point *point);
bool Point_IsEqual(struct Point const *lhs, struct Point const *rhs);

#ifdef __cplusplus
}
#endif

