
#include <ranges>
#include "query_iterator.hpp"

void QIter_Free(GeometryQueryIterator *iter) noexcept {
	delete iter;
}

RTDoc const *QIter_Next(GeometryQueryIterator *iter) {
	return iter->index_ < iter->iter_.size() ? &iter->iter_[iter->index_++] : nullptr;
}

[[nodiscard]] size_t QIter_Remaining(GeometryQueryIterator const *iter) {
	return iter->iter_.size() - iter->index_;
}

void QIter_Sort(GeometryQueryIterator *iter) {
	std::ranges::sort(iter->iter_, [](auto const& doc1, auto const& doc2) {
		return doc1.id_ < doc2.id_;
	});
}
