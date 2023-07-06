/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../../rmalloc.h"

namespace RediSearch {
namespace Allocator {
template <class T>
struct TrackingAllocator {
  using value_type = T;
  std::size_t& allocated_;

  TrackingAllocator() = delete;
  explicit inline TrackingAllocator(std::size_t& ref) noexcept;
  template <class U>
  explicit inline TrackingAllocator(TrackingAllocator<U> const& other) noexcept;

  [[nodiscard]] inline value_type* allocate(std::size_t n) noexcept;
  inline void deallocate(value_type* p, std::size_t n) noexcept;

  [[nodiscard]] inline constexpr std::size_t report() noexcept;
};

template <class T>
inline TrackingAllocator<T>::TrackingAllocator(std::size_t& ref) noexcept : allocated_{ref} {
}

template <class T>
template <class U>
inline TrackingAllocator<T>::TrackingAllocator(TrackingAllocator<U> const& other) noexcept
    : allocated_{other.allocated_} {
}

template <class T>
inline TrackingAllocator<T>::value_type* TrackingAllocator<T>::allocate(std::size_t n) noexcept {
  auto alloc_size = n * sizeof(value_type);
  auto p = static_cast<value_type*>(rm_malloc(alloc_size));
  if (p) {
    allocated_ += alloc_size;
  }
  return p;
}

template <class T>
inline void TrackingAllocator<T>::deallocate(value_type* p, std::size_t n) noexcept {
  rm_free(p);
  allocated_ -= n * sizeof(value_type);
}

template <class T>
inline constexpr std::size_t TrackingAllocator<T>::report() noexcept {
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
