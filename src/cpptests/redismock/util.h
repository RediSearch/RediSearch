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

}  // namespace RMCK