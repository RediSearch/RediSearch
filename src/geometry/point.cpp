
#include "point.hpp"

Point *Point_New(double x, double y) {
	return new Point{x, y};
}
Point *Point_Copy(Point const *other) {
	return new Point{other->point_};
}

void Point_Free(Point *point) {
	delete point;
}

bool Point_IsEqual(Point const *lhs, Point const *rhs) {
	return *lhs == *rhs;
}
