
#include "point.hpp"

[[nodiscard]] Point *Point_New(double x, double y) {
	return new Point{x, y};
}
[[nodiscard]] Point *Point_Copy(Point const *other) {
	return new Point{other->point_};
}

void Point_Free(Point *point) noexcept {
	delete point;
}

[[nodiscard]] bool Point_IsEqual(Point const *lhs, Point const *rhs) {
	return *lhs == *rhs;
}
