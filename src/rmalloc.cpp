
#include <cstdint>                                 // std::uintptr_t
#include <boost/unordered/unordered_flat_map.hpp>  // boost::unordered_flat_map
#include <iostream>                                // std::cerr
#include "rmalloc.h"

#ifdef REDIS_MODULE_TARGET /* Set this when compiling your code as a module */

struct src_location {
  const char *file;
  const char *fn;
  std::size_t line;
};

struct Manager {
  boost::unordered_flat_map<std::uintptr_t, src_location> collection;

  Manager() = default;
  Manager(const Manager &) = delete;
  Manager(Manager &&) = delete;
  Manager &operator=(const Manager &) = delete;
  Manager &operator=(Manager &&) = delete;

  ~Manager() {
    for (auto const [ptr, src] : collection) {
      RedisModule_Log(nullptr, "warning", "%p not freed. allocated at file: %s(%lu) `%s`", ptr,
                      src.file, src.line, src.fn);
    }
  }

  void insert(const void *ptr, const char *file, const char *fn, std::size_t line) {
    const auto pi = reinterpret_cast<const std::uintptr_t>(ptr);
    const auto src = src_location{file, fn, line};
    collection.insert_or_assign(pi, src);
  }
  void remove(const void *ptr, const char *file, const char *fn, std::size_t line) {
    const auto pi = reinterpret_cast<const std::uintptr_t>(ptr);
    const auto src = src_location{file, fn, line};
    if (pi != 0) {
      if (collection.contains(pi)) {
        collection.erase(pi);
      } else {
        RedisModule_Log(nullptr, "warning",
                        "attempting to free unallocated ptr: %p at file: %s(%lu) `%s`", ptr,
                        src.file, src.line, src.fn);
      }
    }
  }
};

inline decltype(auto) manager() {
  static Manager manager{};
  return (manager);
}

extern "C" {

void *rm_malloc_impl(std::size_t n, const char *file, const char *fn, std::size_t line) {
  auto ptr = RedisModule_Alloc(n);
  manager().insert(ptr, file, fn, line);
  return ptr;
}
void *rm_calloc_impl(std::size_t nelem, std::size_t elemsz, const char *file, const char *fn,
                     std::size_t line) {
  auto ptr = RedisModule_Calloc(nelem, elemsz);
  manager().insert(ptr, file, fn, line);
  return ptr;
}

void *rm_realloc_impl(void *p, std::size_t n, const char *file, const char *fn, std::size_t line) {
  manager().remove(p, file, fn, line);
  if (n == 0) {
    RedisModule_Free(p);
    return nullptr;
  }
  auto q = RedisModule_Realloc(p, n);
  manager().insert(q, file, fn, line);
  return q;
}
void rm_free_impl(void *p, const char *file, const char *fn, std::size_t line) {
  manager().remove(p, file, fn, line);
  RedisModule_Free(p);
}
char *rm_strdup_impl(const char *s, const char *file, const char *fn, std::size_t line) {
  auto str = RedisModule_Strdup(s);
  manager().insert(str, file, fn, line);
  return str;
}

}  // extern "C"

#endif
