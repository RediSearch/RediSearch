/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "rtdoc.hpp"
#include "query_iterator.hpp"
#include "rtree.h"

namespace bg = boost::geometry;
namespace bgi = bg::index;

struct RTree {
  using parameter_type = bgi::quadratic<16>;
  using rtree_internal =
      bgi::rtree<RTDoc, parameter_type, RTDoc_Indexable, RTDoc_EqualTo, rm_allocator<RTDoc>>;

  rtree_internal rtree_;

  explicit RTree() = default;
  explicit RTree(rtree_internal const& rt) noexcept : rtree_{rt} {
  }

  void insert(const RTDoc& doc) {
    rtree_.insert(doc);
  }

  bool remove(const RTDoc& doc) {
    return rtree_.remove(doc);
  }

  [[nodiscard]] size_t size() const noexcept {
    return rtree_.size();
  }

  [[nodiscard]] bool is_empty() const noexcept {
    return rtree_.empty();
  }

  void clear() noexcept {
    rtree_.clear();
  }

  [[nodiscard]] size_t report() const noexcept {
    return rtree_.get_allocator().report();
  }

  using ResultsVec = std::vector<RTDoc, rm_allocator<RTDoc>>;

	[[nodiscard]] ResultsVec query(RTDoc const& queryDoc, QueryType queryType) const {
		switch (queryType) {
		 case QueryType::CONTAINS:
			return contains(queryDoc);
		 case QueryType::WITHIN:
			return within(queryDoc);
		 default:
			return {};
		}
	}

	[[nodiscard]] ResultsVec contains(RTDoc const& queryDoc) const {
		auto results = query(bgi::contains(queryDoc.rect_));
		std::erase_if(results, [&](auto const& doc) {
			return !bg::within(queryDoc.poly_, doc.poly_);
		});
		return results;
	}
	[[nodiscard]] ResultsVec within(RTDoc const& queryDoc) const {
		auto results = query(bgi::within(queryDoc.rect_));
		std::erase_if(results, [&](auto const& doc) {
			return !bg::within(doc.poly_, queryDoc.poly_);
		});
		return results;
	}

  template <typename Predicate>
  [[nodiscard]] ResultsVec query(Predicate p) const {
    ResultsVec results{};
    rtree_.query(p, std::back_inserter(results));
    return results;
  }

  using Self = RTree;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
