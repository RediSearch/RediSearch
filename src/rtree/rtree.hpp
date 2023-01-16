#pragma once

#include <boost/geometry.hpp>
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
	size_t query(Predicate p, RTDoc **results) const {
		std::vector<RTDoc> result{};
		size_t length = rtree_.query(p, std::back_inserter(result));
		
		*results = new RTDoc[length];
		std::copy(result.begin(), result.end(), *results);
		return length;
	}
};
