
#include <vector>
#include "rtdoc.hpp"
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

#include <iostream>
RTree_QueryIterator *RTree_Query_Contains(RTree const *rtree, Polygon const *query_poly, size_t *num_results) {
	auto results = rtree->query(bgi::contains(RTDoc::to_rect(query_poly->poly_)), num_results);
	std::erase_if(results, [&](auto const& doc) {
		return !bg::within(query_poly->poly_, doc.poly_);;
	});
	// results.resize(std::distance(results.begin(), end));
	*num_results = results.size();
	return new RTree_QueryIterator{std::move(results)};
}

void RTree_QIter_Free(RTree_QueryIterator *iter) {
	delete iter;
}

RTDoc *RTree_QIter_Next(RTree_QueryIterator *iter) {
	return iter->index_ < iter->iter_.size() ? &iter->iter_[iter->index_++] : nullptr;
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
