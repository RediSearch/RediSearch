
#include "rtdoc.hpp"

[[nodiscard]] RTDoc *RTDoc_New(Polygon const *polygon, docID_t id) {
	return new RTDoc{polygon->poly_, id};
}

[[nodiscard]] RTDoc *RTDoc_Copy(RTDoc const *other) {
	return new RTDoc{*other};
}

void RTDoc_Free(RTDoc *doc) noexcept {
	delete doc;
}

[[nodiscard]] docID_t RTDoc_GetID(RTDoc const *doc) noexcept {
	return doc->id_;
}

[[nodiscard]] Polygon *RTDoc_GetPolygon(RTDoc const *doc) {
	return new Polygon{doc->poly_};
}

[[nodiscard]] Point *RTDoc_MinCorner(RTDoc const *doc) {
	return new Point{doc->rect_.min_corner()};
}

[[nodiscard]] Point *RTDoc_MaxCorner(RTDoc const *doc) {
	return new Point{doc->rect_.max_corner()};
}

[[nodiscard]] bool RTDoc_IsEqual(RTDoc const *lhs, RTDoc const *rhs) {
	return *lhs == *rhs;
}

#include <iostream>
void RTDoc_Print(RTDoc const *doc) {
	std::cout << bg::wkt(doc->poly_) << "\n";
}
