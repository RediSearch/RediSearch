#pragma once

#include "../rmalloc.h"

template <class T>
class rm_allocator {
public:
  using value_type = T;

  /**
   * @brief Construct a new RedisModule allocator
   */
  rm_allocator() noexcept {}
  /**
   * @brief Constructs `a` such that `allocator<B>(a) == b` and `allocator<A>(b) == a`.
   * This implies that all allocators related by rebind maintain each other's resources.
   */
  template <class U>
  rm_allocator(rm_allocator<U> const&) noexcept {}

  /**
   * @brief Allocates storage suitable for an array object of type T[n] and creates the array,
   *        but does not construct array elements. May throw exceptions.
   *        If n == 0, the return value is unspecified.
   * 
   * @param n 
   * @return value_type* 
   */
  value_type* allocate(std::size_t n) {
    return static_cast<value_type*>(rm_malloc(n * sizeof(value_type)));
  }

  /**
   * @brief Deallocates storage pointed to p, which must be a value returned by a previous call to
   *        allocate that has not been invalidated by an intervening call to deallocate.
   *        n must match the value previously passed to allocate. Does not throw exceptions. 
   * 
   * @param p 
   */
  void deallocate(value_type* p, std::size_t) noexcept {
    rm_free(p);
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
bool operator==(rm_allocator<T> const& a1, rm_allocator<U> const& a2) noexcept { return true; }

/**
 * @brief Same as !(a1 == a2). 
 * 
 * @param a1 
 * @param a2 
 * @return bool
 */
template <class T, class U>
bool operator!=(rm_allocator<T> const& a1, rm_allocator<U> const& a2) noexcept { return !(a1 == a2); }
