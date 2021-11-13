#ifndef RMCK_UTIL_H
#define RMCK_UTIL_H

#include "redismock.h"
#include <cstdarg>
#include <cstring>
#include <vector>
#include <string>
namespace RMCK {

class RString {
 public:
  RString(const char *s, size_t n = SIZE_MAX) {
    if (n == SIZE_MAX) {
      n = strlen(s);
    }
    p = RedisModule_CreateString(NULL, s, n);
  }

  RString(const std::string &s) : RString(s.c_str(), s.size()) {
  }

  void clear() {
    if (p) {
      RedisModule_FreeString(NULL, p);
      p = NULL;
    }
  }

  operator RedisModuleString *() {
    return p;
  }

  RedisModuleString *rstring() const {
    return p;
  }

  ~RString() {
    clear();
  }

 private:
  RedisModuleString *p;
};

// Get the refcount of a given string
size_t GetRefcount(const RedisModuleString *s);

/**
 * Set the value of a hash key; creating the hash if it doesn't exist
 * @param ctx the context
 * @param rkey the key of the overall hash
 * @param hkey the key within the hash
 * @param v the value to set for `hkey`
 * @param create if false, will fail if `rkey` does not yet exist
 */
bool hset(RedisModuleCtx *ctx, const char *rkey, const char *hkey, const char *v,
          bool create = true);

/** Clears the database associated with the context */
void flushdb(RedisModuleCtx *);

std::vector<RedisModuleString *> CreateArgv(RedisModuleCtx *, const char *s, ...);
std::vector<RedisModuleString *> CreateArgv(RedisModuleCtx *, const char **s, size_t n);

class ArgvList {
  std::vector<RedisModuleString *> m_list;
  RedisModuleCtx *m_ctx;

 public:
  template <typename... Ts>
  ArgvList(RedisModuleCtx *ctx, Ts... args) : m_ctx(ctx) {
    m_list = CreateArgv(ctx, args..., (const char *)NULL);
  }
  ArgvList(RedisModuleCtx *ctx, const char **s, size_t n) : m_ctx(ctx) {
    m_list = CreateArgv(ctx, s, n);
  }
  ArgvList(ArgvList &) = delete;

  void clear() {
    for (auto ss : m_list) {
      RedisModule_FreeString(m_ctx, ss);
    }
    m_list.clear();
  }

  ~ArgvList() {
    clear();
  }

  operator RedisModuleString **() {
    return &m_list[0];
  }

  size_t size() const {
    return m_list.size();
  }
};

class Context {
 public:
  Context() {
    m_ctx = RedisModule_GetThreadSafeContext(NULL);
  }
  ~Context() {
    RedisModule_FreeThreadSafeContext(m_ctx);
  }

  operator RedisModuleCtx *() {
    return m_ctx;
  }

  Context(const Context &) = delete;

 private:
  RedisModuleCtx *m_ctx;
};

}  // namespace RMCK
#endif
