/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../index_iterator.h"
#include "allocator/tracking_allocator.hpp"

#include <vector>  // std::vector
#include <ranges>  // ranges::input_range, ranges::begin, ranges::end

namespace RediSearch {
namespace GeoShape {
struct QueryIterator {
  using alloc_type = RediSearch::Allocator::TrackingAllocator<t_docId>;
  using container_type = std::vector<t_docId, alloc_type>;

  IndexIterator base_;
  container_type iter_;
  std::size_t index_;

  explicit QueryIterator() = delete;
  explicit QueryIterator(container_type &&docs);
  template <std::ranges::input_range R>  // the elements of the range must be convertible to t_docId
    requires std::is_convertible_v<t_docId, std::decay_t<decltype(*std::declval<R>().begin())>>
  explicit QueryIterator(R &&range, std::size_t &alloc)
      : QueryIterator(container_type{range.begin(), range.end(), alloc_type{alloc}}) {
  }

  /* rule of 5 */
  QueryIterator(QueryIterator const &) = delete;
  explicit QueryIterator(QueryIterator &&) = default;
  QueryIterator &operator=(QueryIterator const &) = delete;
  QueryIterator &operator=(QueryIterator &&) = default;
  ~QueryIterator() noexcept;

  auto base() noexcept -> IndexIterator *;

  int read(RSIndexResult *&hit) noexcept;
  int skip_to(t_docId docId, RSIndexResult *&hit);
  t_docId current() const noexcept;
  int has_next() const noexcept;
  std::size_t len() const noexcept;
  void abort() noexcept;
  void rewind() noexcept;

  static IndexIterator init_base();

  void *operator new(std::size_t, std::size_t &alloc) noexcept;
  void operator delete(QueryIterator *ptr, std::destroying_delete_t) noexcept;
};

}  // namespace GeoShape
}  // namespace RediSearch
