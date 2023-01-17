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
	
	template <typename Predicate>
	std::vector<RTDoc> query(Predicate p, size_t *num_results) const {
		std::vector<RTDoc> result{};
		*num_results = rtree_.query(p, std::back_inserter(result));
		return result;
	}
};

struct RTree_QueryIterator {
	std::vector<RTDoc> iter_;
	size_t index_;

	RTree_QueryIterator(std::vector<RTDoc>&& iter) : iter_{std::move(iter)}, index_{0} {}
};
