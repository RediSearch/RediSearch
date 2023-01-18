#pragma once

#include <boost/geometry.hpp>
#include <algorithm>
#include "rtdoc.hpp"
#include "rtree.h"

namespace bg = boost::geometry;
namespace bgi = bg::index;

struct RTree {
	using parameter_type = bgi::quadratic<16>;
	using rtree_internal = bgi::rtree<RTDoc, parameter_type, RTDoc_Indexable, RTDoc_EqualTo
			// , Redis_Allocator
		>;

	rtree_internal rtree_;

    RTree() : rtree_{} {}
    RTree(rtree_internal const& rt) : rtree_{rt} {}
	
	template <typename Predicate>
	std::vector<RTDoc> query(Predicate p) const {
		std::vector<RTDoc> result{};
		rtree_.query(p, std::back_inserter(result));
		return result;
	}
};

struct QueryIterator {
	std::vector<RTDoc> iter_;
	size_t index_;

	QueryIterator(std::vector<RTDoc>&& iter) : iter_{std::move(iter)}, index_{0} {}
};
