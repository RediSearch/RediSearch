
#include "rtree.hpp"

[[nodiscard]] RTree *RTree_New() {
	return new RTree{};
}

void RTree_Free(RTree *rtree) noexcept {
	delete rtree;
}

void RTree_Insert(RTree *rtree, RTDoc const *doc) {
	rtree->rtree_.insert(*doc);
}

bool RTree_Remove(RTree *rtree, RTDoc const *doc) {
	return rtree->rtree_.remove(*doc);
}

[[nodiscard]] QueryIterator *RTree_Query_Contains(RTree const *rtree, RTDoc const *query) {
	auto results = rtree->query(bgi::contains(query->rect_));
	std::erase_if(results, [&](auto const& doc) {
		return !bg::within(query->poly_, doc.poly_);
	});
	return new QueryIterator{std::move(results)};
}
[[nodiscard]] QueryIterator *RTree_Query_Within(RTree const *rtree, RTDoc const *query) {
	auto results = rtree->query(bgi::within(query->rect_));
	std::erase_if(results, [&](auto const& doc) {
		return !bg::within(doc.poly_, query->poly_);
	});
	return new QueryIterator{std::move(results)};
}

[[nodiscard]] RTDoc *RTree_Bounds(RTree const *rtree) {
	return new RTDoc{rtree->rtree_.bounds()};
}

[[nodiscard]] size_t RTree_Size(RTree const *rtree) noexcept {
	return rtree->rtree_.size();
}

[[nodiscard]] bool RTree_IsEmpty(RTree const *rtree) noexcept {
	return rtree->rtree_.empty();
}

void RTree_Clear(RTree *rtree) noexcept {
	rtree->rtree_.clear();
}



[[nodiscard]] size_t RTree_MemUsage(RTree const *rtree) {
	return rtree->rtree_.get_allocator().report();
}
