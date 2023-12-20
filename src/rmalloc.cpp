
#include <boost/stacktrace.hpp>                    // boost::stacktrace
#include <cstdint>                                 // std::uintptr_t
#include <boost/unordered/unordered_flat_map.hpp>  // boost::unordered_flat_map
#include <iostream>                                // std::cerr
#include "rmalloc.h"

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */

struct Manager {
  boost::unordered_flat_map<std::uintptr_t, boost::stacktrace::stacktrace> collection;

  Manager() = default;
  Manager(const Manager &) = delete;
  Manager(Manager &&) = delete;
  Manager &operator=(const Manager &) = delete;
  Manager &operator=(Manager &&) = delete;

  ~Manager() {
    for (auto const &[ptr, src] : collection) {
      std::cerr << ptr << " not freed. allocated at:\n\t" << src << '\n';
    }
  }

  void insert(const void *ptr,
              const boost::stacktrace::stacktrace src = boost::stacktrace::stacktrace{}) {
    auto pi = reinterpret_cast<const std::uintptr_t>(ptr);
    collection.insert_or_assign(pi, src);
  }
  void remove(const void *ptr,
              const boost::stacktrace::stacktrace src = boost::stacktrace::stacktrace{}) {
    auto pi = reinterpret_cast<const std::uintptr_t>(ptr);
    if (collection.contains(pi)) {
      collection.erase(pi);
    } else {
      std::cerr << "attempting to free unallocated ptr: " << ptr << " at:\n\t" << src << '\n';
    }
  }
};

inline decltype(auto) manager() {
  static Manager manager{};
  return (manager);
}

extern "C" {

void *rm_malloc(size_t n) {
  auto ptr = RedisModule_Alloc(n);
  manager().insert(ptr);
  return ptr;
}
void *rm_calloc(size_t nelem, size_t elemsz) {
  auto ptr = RedisModule_Calloc(nelem, elemsz);
  manager().insert(ptr);
  return ptr;
}

void *rm_realloc(void *p, size_t n) {
  manager().remove(p);
  if (n == 0) {
    RedisModule_Free(p);
    return nullptr;
  }
  auto q = RedisModule_Realloc(p, n);
  manager().insert(q);
  return q;
}
void rm_free(void *p) {
  manager().remove(p);
  RedisModule_Free(p);
}
char *rm_strdup(const char *s) {
  auto str = RedisModule_Strdup(s);
  manager().insert(str);
  return str;
}

}  // extern "C"

#endif
