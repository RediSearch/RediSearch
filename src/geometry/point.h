#pragma once

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

struct Point;

NODISCARD struct Point *Point_New(double x, double y);
NODISCARD struct Point *Point_Copy(struct Point const *other);
void Point_Free(struct Point *point) NOEXCEPT;
NODISCARD bool Point_IsEqual(struct Point const *lhs, struct Point const *rhs);

#ifdef __cplusplus
}
#endif

