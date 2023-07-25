/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../../rmalloc.h"
#include "allocator.hpp"

namespace RediSearch {
namespace Allocator {
/**
 * Allocator which updates an external memory tracker with all allocations done using this
 * allocator. No manual memory tracking needs to be done. May not be default constructed.
 */
template <class T>
struct TrackingAllocator {
  using value_type = T;
  std::size_t& allocated_;

  TrackingAllocator() = delete;
  explicit inline constexpr TrackingAllocator(std::size_t& ref) noexcept;
  template <class U>
  explicit inline constexpr TrackingAllocator(TrackingAllocator<U> const& other) noexcept;

  [[nodiscard]] inline auto allocate(std::size_t n) noexcept -> value_type*;
  inline void deallocate(value_type* p, std::size_t n) noexcept;

  [[nodiscard]] inline constexpr std::size_t report() const noexcept;
};

template <class T>
inline constexpr TrackingAllocator<T>::TrackingAllocator(std::size_t& ref) noexcept
    : allocated_{ref} {
}

template <class T>
template <class U>
inline constexpr TrackingAllocator<T>::TrackingAllocator(TrackingAllocator<U> const& other) noexcept
    : allocated_{other.allocated_} {
}

template <class T>
inline auto TrackingAllocator<T>::allocate(std::size_t n) noexcept -> value_type* {
  auto p = Allocator<T>::allocate(n);
  if (p) {
    allocated_ += n * sizeof(value_type);
  }
  return p;
}

template <class T>
inline void TrackingAllocator<T>::deallocate(value_type* p, std::size_t n) noexcept {
  Allocator<T>::deallocate(p, n);
  allocated_ -= n * sizeof(value_type);
}

template <class T>
inline constexpr std::size_t TrackingAllocator<T>::report() const noexcept {
  return allocated_;
}

template <class T, class U>
[[nodiscard]] inline constexpr bool operator==(TrackingAllocator<T> const& a1,
                                               TrackingAllocator<U> const& a2) noexcept {
  return &a1.allocated_ == &a2.allocated_;
}
template <class T, class U>
[[nodiscard]] inline constexpr bool operator!=(TrackingAllocator<T> const& a1,
                                               TrackingAllocator<U> const& a2) noexcept {
  return !(a1 == a2);
}

}  // namespace Allocator
}  // namespace RediSearch
