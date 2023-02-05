#pragma once

#include <vector>
#include <algorithm>
#include "rtdoc.hpp"
#include "query_iterator.h"

struct QueryIterator {
	using container = std::vector<RTDoc, rm_allocator<RTDoc>>;
	container iter_;
	size_t index_;

	explicit QueryIterator() = default;
	explicit QueryIterator(container&& iter) : iter_{std::move(iter)}, index_{0} {}

	using Self = QueryIterator;
  [[nodiscard]] void* operator new(std::size_t sz) { return rm_allocator<Self>().allocate(sz); }
  void operator delete(void *p) noexcept { rm_allocator<Self>().deallocate(static_cast<Self*>(p), sizeof(Self)); }
};
