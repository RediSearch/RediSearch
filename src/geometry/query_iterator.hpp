#pragma once

#define BOOST_ALLOW_DEPRECATED_HEADERS
#include <boost/geometry.hpp>
#undef BOOST_ALLOW_DEPRECATED_HEADERS
#include <vector>
#include <algorithm>
#include "allocator.hpp"
#include "rtdoc.hpp"
#include "query_iterator.h"

struct GeometryQueryIterator {
	using container = std::vector<RTDoc, rm_allocator<RTDoc>>;
	container iter_;
	size_t index_;

	explicit GeometryQueryIterator(container&& iter) : iter_{std::move(iter)}, index_{0} {}

  [[nodiscard]] void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) { rm_free(p); }
};
