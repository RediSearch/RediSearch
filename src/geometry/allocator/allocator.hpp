/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../../rmalloc.h"
#include <cstddef>

namespace RediSearch {
namespace Allocator {
template <class T>
struct Allocator {
  using value_type = T;

  explicit inline constexpr Allocator() = default;
  template <class U>
  explicit inline constexpr Allocator(Allocator<U> const&) noexcept;

  [[nodiscard]] static inline auto allocate(std::size_t n) noexcept -> value_type*;
  static inline void deallocate(value_type* p, std::size_t n) noexcept;
};

template <class T>
template <class U>
inline constexpr Allocator<T>::Allocator(Allocator<U> const&) noexcept {
}

template <class T>
inline auto Allocator<T>::allocate(std::size_t n) noexcept -> value_type* {
  auto alloc_size = n * sizeof(value_type);
  auto p = static_cast<value_type*>(rm_malloc(alloc_size));
  return p;
}

template <class T>
inline void Allocator<T>::deallocate(value_type* p, std::size_t n) noexcept {
  rm_free(p);
}

template <class T, class U>
[[nodiscard]] inline constexpr bool operator==(Allocator<T> const&, Allocator<U> const&) noexcept {
  return true;
}
template <class T, class U>
[[nodiscard]] inline constexpr bool operator!=(Allocator<T> const&, Allocator<U> const&) noexcept {
  return false;
}

}  // namespace Allocator
}  // namespace RediSearch
