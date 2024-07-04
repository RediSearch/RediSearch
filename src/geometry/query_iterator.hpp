/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../index_iterator.h"
#include "allocator/tracking_allocator.hpp"

#include <vector>     // std::vector
#include <ranges>     // ranges::input_range, ranges::begin, ranges::end
#include <algorithm>  // ranges::sort

struct IndexSpec;
namespace RediSearch {
namespace GeoShape {
struct QueryIterator {
  using alloc_type = RediSearch::Allocator::TrackingAllocator<t_docId>;
  using container_type = std::vector<t_docId, alloc_type>;

  IndexIterator base_;
  container_type iter_;
  std::size_t index_;
  const IndexSpec* indexSpec_;
  const FieldIndexFilterContext* filterCtx_;

  explicit QueryIterator() = delete;

  // Projection will be necessary to implement `distance` in the future
  template <typename R, typename Proj = std::identity>
    requires std::ranges::input_range<R> &&
                 std::convertible_to<std::ranges::range_reference_t<R>, t_docId>
  explicit QueryIterator(const IndexSpec* spec, const FieldIndexFilterContext* filterCtx, R &&range, std::size_t &alloc, Proj proj = {})
      : base_{init_base(this)},
        iter_{std::ranges::begin(range), std::ranges::end(range), alloc_type{alloc}},
        index_{0}, indexSpec_(spec), filterCtx_(filterCtx) {
    std::ranges::sort(iter_, std::ranges::less{}, proj);
  }

  /* rule of 5 */
  explicit QueryIterator(QueryIterator const &) = delete;
  explicit QueryIterator(QueryIterator &&) = delete;
  QueryIterator &operator=(QueryIterator const &) = delete;
  QueryIterator &operator=(QueryIterator &&) = delete;
  ~QueryIterator() noexcept = default;

  auto base() noexcept -> IndexIterator *;

  int read_single(RSIndexResult *&hit) noexcept;
  int read(RSIndexResult *&hit) noexcept;
  int skip_to(t_docId docId, RSIndexResult *&hit);
  t_docId current() const noexcept;
  int has_next() const noexcept;
  std::size_t len() const noexcept;
  void abort() noexcept;
  void rewind() noexcept;
  void set_time(timespec now) noexcept;

  static IndexIterator init_base(QueryIterator *ctx);
};

}  // namespace GeoShape
}  // namespace RediSearch
