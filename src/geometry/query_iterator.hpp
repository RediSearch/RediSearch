/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "../iterators/iterator_api.h"
#include "allocator/tracking_allocator.hpp"

#include <vector>     // std::vector
#include <ranges>     // ranges::input_range, ranges::begin, ranges::end
#include <algorithm>  // ranges::sort

namespace RediSearch {
namespace GeoShape {
struct CPPQueryIterator {
  using alloc_type = RediSearch::Allocator::TrackingAllocator<t_docId>;
  using container_type = std::vector<t_docId, alloc_type>;

  QueryIterator base_;
  container_type iter_;
  std::size_t index_;
  const RedisSearchCtx *sctx_;
  const FieldFilterContext filterCtx_;

  explicit CPPQueryIterator() = delete;

  // Projection will be necessary to implement `distance` in the future
  template <typename R, typename Proj = std::identity>
    requires std::ranges::input_range<R> &&
                 std::convertible_to<std::ranges::range_reference_t<R>, t_docId>
  explicit CPPQueryIterator(const RedisSearchCtx *sctx, const FieldFilterContext* filterCtx, R &&range, std::size_t &alloc, Proj proj = {})
      : base_{init_base()},
        iter_{std::ranges::begin(range), std::ranges::end(range), alloc_type{alloc}},
        index_{0}, sctx_(sctx), filterCtx_(*filterCtx) {
    std::ranges::sort(iter_, std::ranges::less{}, proj);
  }

  /* rule of 5 */
  explicit CPPQueryIterator(CPPQueryIterator const &) = delete;
  explicit CPPQueryIterator(CPPQueryIterator &&) = delete;
  CPPQueryIterator &operator=(CPPQueryIterator const &) = delete;
  CPPQueryIterator &operator=(CPPQueryIterator &&) = delete;
  ~CPPQueryIterator() noexcept = default;

  auto base() noexcept -> QueryIterator *;

  IteratorStatus read() noexcept;
  IteratorStatus skip_to(t_docId docId);
  t_docId current() const noexcept;
  bool has_next() const noexcept;
  std::size_t len() const noexcept;
  void rewind() noexcept;

  static QueryIterator init_base();
private:
  IteratorStatus read_single() noexcept;
};

}  // namespace GeoShape
}  // namespace RediSearch
