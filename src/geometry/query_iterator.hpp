#pragma once

#include <vector>
#include <ranges>
#include <algorithm>
#include "rtdoc.hpp"
#include "query_iterator.h"

struct GeometryQueryIterator {
	using container = std::vector<t_docId, rm_allocator<t_docId>>;
	IndexIterator base_;
	container iter_;
	size_t index_;

	explicit GeometryQueryIterator() = default;
	explicit GeometryQueryIterator(std::vector<RTDoc, rm_allocator<RTDoc>>&& docs)
		: base_{init()}
		, iter_{std::ranges::sort(std::ranges::transform(docs, [](auto && doc){ return doc.id(); }))}
		, index_{0}
	{}

	explicit GeometryQueryIterator(const GeometryQueryIterator&) = delete;
	explicit GeometryQueryIterator(GeometryQueryIterator&&) = default;
	GeometryQueryIterator& operator=(const GeometryQueryIterator&) = delete;
	GeometryQueryIterator& operator=(GeometryQueryIterator&&) = default;
	~GeometryQueryIterator() { IndexResult_Free(base_.current); }

	IndexIterator *base() { return &base_; }


	t_docId current() const {
		return has_next() ? iter_[index_] : 0;
	}
	int has_next() const {
		return index_ < iter_.size();
	}
	size_t len() const {
		return iter_.size();
	}
	void abort() {
		base_.isValid = 0;
	}
	void rewind() {
		base_.current->docId = 0;
		index_ = 0;
	}

	void init() {
		return IndexIterator {
			.ctx = this;
			.mode = MODE_SORTED;
			.type = /* TODO: new iterator type */;
			.NumEstimated = ;
			.GetCriteriaTester = ;
			.Read = ;
			.SkipTo = ;
			.LastDocId = QIter_LastDocId;
			.HasNext = QIter_HasNext;
			.Free = QIter_Free;
			.Len = QIter_Len;
			.Abort = QIter_Abort;
			.Rewind = QIter_Rewind;
		};
	}

	using Self = GeometryQueryIterator;
  [[nodiscard]] void* operator new(std::size_t) { return rm_allocator<Self>().allocate(1); }
  void operator delete(void *p) noexcept { rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1); }
};

t_docId QIter_LastDocId(void *ctx) {
	return static_cast<GeometryQueryIterator const*>(ctx)->current();
}
int QIter_HasNext(void *ctx) {
	return static_cast<GeometryQueryIterator const*>(ctx)->has_next();
}
void QIter_Free(IdexIterator *self) {
	auto it = static_cast<GeometryQueryIterator*>(self->ctx);
	delete it;
}
size_t QIter_Len(void *ctx) {
  return static_cast<GeometryQueryIterator const*>(ctx)->len();
}
void QIter_Abort(void *ctx) {
  static_cast<GeometryQueryIterator*>(ctx)->abort();
}
void QIter_Rewind(void *ctx) {
	static_cast<GeometryQueryIterator*>(ctx)->rewind();
}


/*
void QIter_Free(GeometryQueryIterator *iter) noexcept {
	delete iter;
}

RTDoc const *QIter_Next(GeometryQueryIterator *iter) {
	return iter->index_ < iter->iter_.size() ? &iter->iter_[iter->index_++] : nullptr;
}

size_t QIter_Remaining(GeometryQueryIterator const *iter) {
	return iter->iter_.size() - iter->index_;
}

void QIter_Sort(GeometryQueryIterator *iter) {
	std::ranges::sort(iter->iter_, [](auto const& doc1, auto const& doc2) {
		return doc1.id_ < doc2.id_;
	});
}
*/