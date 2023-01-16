
#include <vector>
#include <utility>
#include "rtree.hpp"

RTree *RTree_New() {
	return new RTree{};
}

void RTree_Free(RTree *rtree) {
	delete rtree;
}

void RTree_Insert(RTree *rtree, RTDoc const *doc) {
	rtree->rtree_.insert(*doc);
}

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
	return rtree->rtree_.remove(*doc);
}

size_t RTree_Query_Contains(RTree const *rtree, Point const *point, RTDoc **results) {
	return rtree->query(bgi::contains(point->point_), results);
}

void RTree_Query_Free(RTDoc *query) {
	delete[] query;
}

RTDoc *RTree_Bounds(RTree const *rtree) {
	return new RTDoc{rtree->rtree_.bounds()};
}

size_t RTree_Size(RTree const *rtree) {
	return rtree->rtree_.size();
}

bool RTree_IsEmpty(RTree const *rtree) {
	return rtree->rtree_.empty();
}

void RTree_Clear(RTree *rtree) {
	rtree->rtree_.clear();
}
