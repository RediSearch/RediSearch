#include "redismodule.h"
#include "module.h"
#include "version.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "aggregate/aggregate.h"

#include "gtest/gtest.h"

typedef enum { COMMAND_AGGREGATE, COMMAND_SEARCH, COMMAND_EXPLAIN } CommandType;
typedef enum { NO_PROFILE, PROFILE_FULL, PROFILE_LIMITED } ProfileMode;



static int parseProfile(AREQ *r, ProfileMode withProfile, RedisModuleString **argv, int argc, QueryError *status) {
  if (withProfile != NO_PROFILE) {

    r->reqflags |= QEXEC_F_PROFILE;
    if (withProfile == PROFILE_LIMITED) {
      r->reqflags |= QEXEC_F_PROFILE_LIMITED;
    }
    // hires_clock_get(&r->initClock);
  }
  return REDISMODULE_OK;
}

static int buildRequest(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int type,
                        QueryError *status, AREQ **r) {

  int rc = REDISMODULE_ERR;
  hires_clock_t parseClock;
  const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
  RedisSearchCtx *sctx = NULL;
  // RedisModuleCtx *thctx = NULL;

  if (type == COMMAND_SEARCH) {
    (*r)->reqflags |= QEXEC_F_IS_SEARCH;
  }
  else if (type == COMMAND_AGGREGATE) {
    (*r)->reqflags |= QEXEC_F_IS_EXTENDED;
  }

  if (AREQ_Compile(*r, argv + 2, argc - 2, status) != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    abort();
  }

//   // Prepare the query.. this is where the context is applied.
//   if ((*r)->reqflags & QEXEC_F_IS_CURSOR) {
//     RedisModuleCtx *newctx = RedisModule_GetThreadSafeContext(NULL);
//     RedisModule_SelectDb(newctx, RedisModule_GetSelectedDb(ctx));
//     ctx = thctx = newctx;  // In case of error!
//   }

  sctx = NewSearchCtxC(ctx, indexname, true);
  if (!sctx) {
    QueryError_SetErrorFmt(status, QUERY_ENOINDEX, "%s: no such index", indexname);
    abort();
  }
  bool is_profile = IsProfile(*r);

  rc = AREQ_ApplyContext(*r, sctx, status);
  // thctx = NULL;
  // ctx is always assigned after ApplyContext
  if (rc != REDISMODULE_OK) {
    RS_LOG_ASSERT(QueryError_HasError(status), "Query has error");
    goto done;
  }

  // if (is_profile) {
  //   hires_clock_get(&parseClock);
  //   (*r)->parseTime += hires_clock_diff_msec(&parseClock, &(*r)->initClock);
  // }

  rc = AREQ_BuildPipeline(*r, 0, status);

  // if (is_profile) {
  //   (*r)->pipelineBuildTime = hires_clock_since_msec(&parseClock);
  // }

done:
  if (rc != REDISMODULE_OK && *r) {
    AREQ_Free(*r);
    *r = NULL;
    // if (thctx) {
    //   RedisModule_FreeThreadSafeContext(thctx);
    // }
  }
  return rc;
}


extern "C" {

static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION, 
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}

}

class MyEnvironment : public ::testing::Environment {
  virtual void SetUp() {
    const char *arguments[] = {"SAFEMODE", "NOGC"};
    // No arguments..
    RMCK_Bootstrap(my_OnLoad, arguments, 2);
  }

  virtual void TearDown() {
    RMCK_Shutdown();
    RediSearch_CleanupModule();
  }
};
class ConcTest : public ::testing::Test {};
TEST_F(ConcTest, simpleTest) {
	RMCK::Context static_ctx;
    RedisModuleCtx *ctx = (RedisModuleCtx *)static_ctx;
	RMCK::ArgvList args(ctx, "FT.CREATE", "idx", "ON", "HASH",
                      "SCHEMA", "t1", "TEXT");
	QueryError qerr = {QueryErrorCode(0)};
  	auto spec = IndexSpec_CreateNew(ctx, args, args.size(), &qerr);
 	ASSERT_TRUE(spec);
    ProfileMode withProfile = NO_PROFILE;
    RMCK::ArgvList argv(ctx, "FT.SEARCH", "idx", "*");
    const char *indexname = RedisModule_StringPtrLen(argv[1], NULL);
    AREQ *r = AREQ_New();
    QueryError status = {.code = QUERY_OK, .detail=NULL};
    if (parseProfile(r, withProfile, argv, argv.size(), &status) != REDISMODULE_OK) {
        printf("Couldn't parse profile: %s\n", QueryError_GetError(&status));
        abort();
    }

    if (buildRequest(ctx, argv, argv.size(), COMMAND_SEARCH, &status, &r) != REDISMODULE_OK) {
        abort();
    }

    SET_DIALECT(r->sctx->spec->used_dialects, r->dialectVersion);
    SET_DIALECT(RSGlobalConfig.used_dialects, r->dialectVersion);

    if (r->reqflags & QEXEC_F_IS_CURSOR) {
        int rc = AREQ_StartCursor(r, ctx, r->sctx->spec->name, &status);
        if (rc != REDISMODULE_OK) {
        abort();
        }
    } else {
        if (IsProfile(r)) {
        RedisModule_ReplyWithArray(ctx, 2);
        }
        AREQ_Execute(r, ctx);
    }

}


int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new MyEnvironment());
  return RUN_ALL_TESTS();
}