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
 * Allocator which update a local memory tracker with all allocations done using this allocator.
 * Manual memory tracking does need to be done to update an external tracker. May be default
 * constructed.
 */
template <class T>
struct StatefulAllocator {
  using value_type = T;
  std::size_t allocated_ = 0;

  explicit inline constexpr StatefulAllocator() = default;
  template <class U>
  explicit inline constexpr StatefulAllocator(StatefulAllocator<U> const&) noexcept;

  [[nodiscard]] inline auto allocate(std::size_t n) noexcept -> value_type*;
  inline void deallocate(value_type* p, std::size_t n) noexcept;

  [[nodiscard]] inline constexpr std::size_t report() const noexcept;
};

template <class T>
template <class U>
inline constexpr StatefulAllocator<T>::StatefulAllocator(StatefulAllocator<U> const&) noexcept {
}

template <class T>
inline auto StatefulAllocator<T>::allocate(std::size_t n) noexcept -> value_type* {
  auto p = Allocator<T>::allocate(n);
  if (p) {
    allocated_ += n * sizeof(value_type);
  }
  return p;
}

template <class T>
inline void StatefulAllocator<T>::deallocate(value_type* p, std::size_t n) noexcept {
  Allocator<T>::deallocate(p, n);
  allocated_ -= n * sizeof(value_type);
}

template <class T>
inline constexpr std::size_t StatefulAllocator<T>::report() const noexcept {
  return allocated_;
}

template <class T, class U>
[[nodiscard]] inline constexpr bool operator==(StatefulAllocator<T> const&,
                                               StatefulAllocator<U> const&) noexcept {
  return false;
}
template <class T, class U>
[[nodiscard]] inline constexpr bool operator!=(StatefulAllocator<T> const&,
                                               StatefulAllocator<U> const&) noexcept {
  return true;
}

}  // namespace Allocator
}  // namespace RediSearch
