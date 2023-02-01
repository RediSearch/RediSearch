
#include "rtdoc.hpp"

[[nodiscard]] RTDoc *RTDoc_Copy(RTDoc const *other) {
	return new RTDoc{*other};
}

void RTDoc_Free(RTDoc *doc) noexcept {
	delete doc;
}

[[nodiscard]] docID_t RTDoc_GetID(RTDoc const *doc) noexcept {
	return doc->id_;
}

[[nodiscard]] bool RTDoc_IsEqual(RTDoc const *lhs, RTDoc const *rhs) {
	return *lhs == *rhs;
}

#include <iostream>
void RTDoc_Print(RTDoc const *doc) {
	std::cout << bg::wkt(doc->poly_) << "\n";
}
