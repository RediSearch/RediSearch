
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

QueryIterator *RTree_Query_Contains(RTree const *rtree, RTDoc const *query) {
	auto results = rtree->query(bgi::contains(query->rect_));
	std::erase_if(results, [&](auto const& doc) {
		return !bg::within(query->poly_, doc.poly_);
	});
	return new QueryIterator{std::move(results)};
}
QueryIterator *RTree_Query_Within(RTree const *rtree, RTDoc const *query) {
	auto results = rtree->query(bgi::within(query->rect_));
	std::erase_if(results, [&](auto const& doc) {
		return !bg::within(doc.poly_, query->poly_);
	});
	return new QueryIterator{std::move(results)};
}

void QIter_Free(QueryIterator *iter) {
	delete iter;
}

RTDoc *QIter_Next(QueryIterator *iter) {
	return iter->index_ < iter->iter_.size() ? &iter->iter_[iter->index_++] : nullptr;
}

size_t QIter_Remaining(QueryIterator const *iter) {
	return iter->iter_.size() - iter->index_;
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



size_t RTree_MemUsage(struct RTree const *rtree) {
	return rtree->rtree_.get_allocator().report();
}
