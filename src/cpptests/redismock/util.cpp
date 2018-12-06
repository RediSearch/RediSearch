#include "util.h"
#include "internal.h"
#include <cstring>

using namespace RMCK;

std::vector<RedisModuleString *> RMCK::CreateArgv(RedisModuleCtx *ctx, const char *s, ...) {
  std::vector<RedisModuleString *> ll;
  ll.push_back(RedisModule_CreateString(ctx, s, strlen(s)));
  va_list ap;
  va_start(ap, s);
  while (true) {
    s = va_arg(ap, const char *);
    if (s == NULL) {
      break;
    } else {
      ll.push_back(RedisModule_CreateString(ctx, s, strlen(s)));
    }
  }
  va_end(ap);
  return ll;
}

std::vector<RedisModuleString *> RMCK::CreateArgv(RedisModuleCtx *ctx, const char **s, size_t n) {
  std::vector<RedisModuleString *> ret;
  for (size_t ii = 0; ii < n; ++ii) {
    RedisModuleString *rstr = new RedisModuleString(s[ii]);
    ret.push_back(rstr);
  }
  return ret;
}
