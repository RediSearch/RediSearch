#pragma once

#include "../index.h"
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

	[[nodiscard]] auto query(RTDoc const& queryDoc, QueryType queryType) const {
		switch (queryType) {
		 case QueryType::CONTAINS:
			return contains(queryDoc);
		 case QueryType::WITHIN:
			return within(queryDoc);
		 default:
			return {};
		}
	}

	[[nodiscard]] auto contains(RTDoc const& queryDoc) const {
		auto results = query(bgi::contains(queryDoc.rect_));
		std::erase_if(results, [&](auto const& doc) {
			return !bg::within(queryDoc.poly_, doc.poly_);
		});
		return results;
	}
	[[nodiscard]] auto within(RTDoc const& queryDoc) const {
		auto results = query(bgi::within(queryDoc.rect_));
		std::erase_if(results, [&](auto const& doc) {
			return !bg::within(doc.poly_, queryDoc.poly_);
		});
		return results;
	}

  template <typename Predicate>
  [[nodiscard]] auto query(Predicate p) const {
    std::vector<RTDoc, rm_allocator<RTDoc>> result{};
    rtree_.query(p, std::back_inserter(result));
    return result;
  }

  using Self = RTree;
  [[nodiscard]] void* operator new(std::size_t) {
    return rm_allocator<Self>().allocate(1);
  }
  void operator delete(void* p) noexcept {
    rm_allocator<Self>().deallocate(static_cast<Self*>(p), 1);
  }
};
