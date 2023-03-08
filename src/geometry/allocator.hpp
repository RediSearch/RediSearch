/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../rmalloc.h"
#include <atomic>

namespace {
std::atomic<size_t> used = 0;
}

template <class T>
struct rm_allocator {
  using value_type = T;

  /**
   * @brief Construct a new RedisModule allocator
   */
  explicit rm_allocator() = default;
  /**
   * @brief Constructs `a` such that `allocator<B>(a) == b` and `allocator<A>(b) == a`.
   * This implies that all allocators related by rebind maintain each other's resources.
   */
  template <class U>
  explicit rm_allocator(rm_allocator<U> const&) noexcept {
  }

  /**
   * @brief Allocates storage suitable for an array object of type T[n] and creates the array,
   *        but does not construct array elements. May throw exceptions.
   *        If n == 0, the return value is unspecified.
   *
   * @param n
   * @return value_type*
   */
  [[nodiscard]] value_type* allocate(std::size_t n) {
    used += n * sizeof(value_type);
    return static_cast<value_type*>(rm_malloc(n * sizeof(value_type)));
  }

  /**
   * @brief Deallocates storage pointed to p, which must be a value returned by a previous call to
   *        allocate that has not been invalidated by an intervening call to deallocate.
   *        n must match the value previously passed to allocate. Does not throw exceptions.
   *
   * @param p
   */
  void deallocate(value_type* p, std::size_t n) noexcept {
    used -= n * sizeof(value_type);
    rm_free(p);
  }

  [[nodiscard]] constexpr static size_t report() noexcept {
    return used;
  }
};

/**
 * @brief `true` only if the storage allocated by the allocator a1 can be deallocated through a2.
 *        Establishes reflexive, symmetric, and transitive relationship. Does not throw exceptions.
 *
 * @param a1
 * @param a2
 * @return bool
 */
template <class T, class U>
[[nodiscard]] constexpr bool operator==(rm_allocator<T> const&, rm_allocator<U> const&) noexcept {
  return true;
}

/**
 * @brief Same as !(a1 == a2).
 *
 * @param a1
 * @param a2
 * @return bool
 */
template <class T, class U>
[[nodiscard]] constexpr bool operator!=(rm_allocator<T> const&, rm_allocator<U> const&) noexcept {
  return false;
}
