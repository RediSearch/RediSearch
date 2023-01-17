
#include "rtdoc.hpp"

RTDoc *RTDoc_New(Polygon const *polygon) {
	return new RTDoc{polygon->poly_};
}

RTDoc *RTDoc_Copy(RTDoc const *other) {
	return new RTDoc{*other};
}

void RTDoc_Free(RTDoc *doc) {
	delete doc;
}

Point const *RTDoc_MinCorner(RTDoc const *doc) {
	return new Point{doc->rect_.min_corner()};
}

Point const *RTDoc_MaxCorner(RTDoc const *doc) {
	return new Point{doc->rect_.max_corner()};
}

bool RTDoc_IsEqual(RTDoc const *lhs, RTDoc const *rhs) {
	return *lhs == *rhs;
}

void RTDoc_Print(RTDoc const *doc) {
	auto p = Polygon{doc->poly_};
	Polygon_Print(&p);
}
