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

size_t RMCK::GetRefcount(const RedisModuleString *s) {
  return s->refcount;
}

bool RMCK::hset(RedisModuleCtx *ctx, const char *rkey, const char *hkey, const char *value,
                bool create) {
  auto v = ctx->db->get(rkey);
  HashValue *hv = NULL;
  if (!v) {
    if (!create) {
      return false;
    } else {
      hv = new HashValue(rkey);
      ctx->db->set(hv);
      hv->decref();
    }
  } else {
    hv = static_cast<HashValue *>(v);
  }
  hv->add(hkey, value);
  return true;
}

void RMCK::flushdb(RedisModuleCtx *ctx) {
  ctx->db->clear();
}

extern "C" {
static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int err = REDISMODULE_OK;
  if (RedisModule_Init(ctx, "dummy", 0, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    err = REDISMODULE_ERR;
  }

  for (auto db : KVDB::dbs) delete db;
  return err;
}
}

void RMCK::init() {
  RMCK_Bootstrap(my_OnLoad, NULL, 0);
}
