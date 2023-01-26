#pragma once

#include <boost/geometry.hpp>
#include <algorithm>
#include "allocator.hpp"
#include "rtdoc.hpp"
#include "rtree.h"

namespace bg = boost::geometry;
namespace bgi = bg::index;

struct QueryIterator {
	using container = std::vector<RTDoc, rm_allocator<RTDoc>>;
	container iter_;
	size_t index_;

	QueryIterator(container&& iter) : iter_{std::move(iter)}, index_{0} {}

  void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) { rm_free(p); }
};

struct RTree {
	using parameter_type = bgi::quadratic<16>;
	using rtree_internal = bgi::rtree<RTDoc, parameter_type, RTDoc_Indexable, RTDoc_EqualTo, rm_allocator<RTDoc>>;

	rtree_internal rtree_;

    RTree() : rtree_{} {}
    RTree(rtree_internal const& rt) : rtree_{rt} {}
	
	template <typename Predicate>
	QueryIterator::container query(Predicate p) const {
		QueryIterator::container result{};
		rtree_.query(p, std::back_inserter(result));
		return result;
	}
	
  void* operator new(std::size_t sz) { return rm_malloc(sz); }
  void operator delete(void *p) { rm_free(p); }
};
