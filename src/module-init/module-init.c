#include "redismodule.h"

#include "module.h"
#include "version.h"
#include "config.h"
#include "redisearch_api.h"
#include <assert.h>
#include <ctype.h>
#include "concurrent_ctx.h"
#include "cursor.h"
#include "extension.h"
#include "alias.h"
#include "aggregate/aggregate.h"
#include "ext/default.h"

#ifndef RS_NO_ONLOAD
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}
#endif

/**
 * Check if we can run under the current AOF configuration. Returns true
 * or false
 */
static int validateAofSettings(RedisModuleCtx *ctx) {
  int rc = 1;

  if (RedisModule_GetContextFlags == NULL) {
    RedisModule_Log(ctx, "warning",
                    "Could not determine if AOF is in use. AOF Rewrite will crash!");
    return 1;
  }

  if ((RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_AOF) == 0) {
    // AOF disabled. All is OK, and no further checks needed
    return rc;
  }

  // Can't execute commands on the loading context, so make a new one
  RedisModuleCtx *confCtx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply =
      RedisModule_Call(confCtx, "CONFIG", "cc", "GET", "aof-use-rdb-preamble");
  assert(reply);
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ARRAY);
  assert(RedisModule_CallReplyLength(reply) == 2);
  const char *value =
      RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(reply, 1), NULL);

  // I tried using strcasecmp, but it seems that the yes/no replies have a trailing
  // embedded newline in them
  if (tolower(*value) == 'n') {
    RedisModule_Log(ctx, "warning", "FATAL: aof-use-rdb-preamble required if AOF is used!");
    rc = 0;
  }
  RedisModule_FreeCallReply(reply);
  RedisModule_FreeThreadSafeContext(confCtx);
  return rc;
}

static int initAsModule(RedisModuleCtx *ctx) {
  // Check that redis supports thread safe context. RC3 or below doesn't
  if (RedisModule_GetThreadSafeContext == NULL) {
    RedisModule_Log(ctx, "warning",
                    "***** FATAL: Incompatible version of redis 4.0 detected. *****\n"
                    "\t\t\t\tPlease use Redis 4.0.0 or later from https://redis.io/download\n"
                    "\t\t\t\tRedis will exit now!");
    return REDISMODULE_ERR;
  }

  if (RediSearch_ExportCapi(ctx) != REDISMODULE_OK) {
    RedisModule_Log(ctx, "warning", "Could not initialize low level api");
  } else {
    RedisModule_Log(ctx, "notice", "Low level api version %d initialized successfully",
                    REDISEARCH_CAPI_VERSION);
  }

  if (RedisModule_GetContextFlags == NULL && RSGlobalConfig.concurrentMode) {
    RedisModule_Log(ctx, "warning",
                    "GetContextFlags unsupported (need Redis >= 4.0.6). Commands executed in "
                    "MULTI or LUA will "
                    "malfunction unless 'safe' functions are used or SAFEMODE is enabled.");
  }

  if (!validateAofSettings(ctx)) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

static int initAsLibrary(RedisModuleCtx *ctx) {
  // Disable concurrent mode:
  RSGlobalConfig.concurrentMode = 0;
  return REDISMODULE_OK;
}

int RS_Initialized = 0;
RedisModuleCtx *RSDummyContext = NULL;

int UpdateIndexWithHash(RedisModuleCtx *ctx, IndexSpec *spec, RedisModuleString *key) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, spec);
  Document doc = {0};
  // TODO: get language and score from spec->rule->setting
  Document_Init(&doc, key, 1, DEFAULT_LANGUAGE);
  if (Document_LoadAllFields(&doc, ctx) != REDISMODULE_OK) {
    Document_Free(&doc);
    return RedisModule_ReplyWithError(ctx, "Could not load document");
  }
  QueryError status = {0};
  RSAddDocumentCtx *aCtx = NewAddDocumentCtx(spec, &doc, &status);
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_PARTIAL);
  return REDISMODULE_OK;
}

typedef struct EvalRec {
  RLookup lk;
  RLookupRow row;
  QueryError status;
  ExprEval ee;
  RSValue res;
} EvalRec;

int EvalRec_Init(EvalRec *r, const char *expr) {
  RLookup _lk = {0};
  r->lk = _lk;
  RLookup_Init(&r->lk, NULL);
  RLookupRow _row = {0};
  r->row = _row;
  QueryError _status = {0};
  r->status = _status;

  r->ee.lookup = &r->lk;
  r->ee.srcrow = &r->row;
  r->ee.err = &r->status;
  
  if (!expr) {
  	r->ee.root = NULL;
  } else {
    r->ee.root = ExprAST_Parse(expr, strlen(expr), r->ee.err);
    if (r->ee.root == NULL) {
  	  return REDISMODULE_ERR;
    }
  }
  
  return REDISMODULE_OK;
}

void EvalRec_Destroy(EvalRec *r) {
  if (r->ee.root) {
    ExprAST_Free((RSExpr *) r->ee.root);
  }
}

RLookupKey *EvalRec_Set(EvalRec *r, const char *name, RSValue *val) {
  RLookupKey *lkk = RLookup_GetKey(&r->lk, name, RLOOKUP_F_OCREAT);
  if (lkk != NULL) {
    RLookup_WriteOwnKey(lkk, &r->row, val);
  }
  return lkk;
}

int RLookup_GetHash(RLookup *it, RLookupRow *dst, RedisModuleCtx *ctx, RedisModuleString *key);

int EvalRec_AddHash(EvalRec *r, RedisModuleCtx *ctx, RedisModuleString *key) {
  return RLookup_GetHash(&r->lk, &r->row, ctx, key);
}

int EvalRec_Eval(EvalRec *r) {
  if (ExprAST_GetLookupKeys((RSExpr *) r->ee.root, (RLookup *) r->ee.lookup, r->ee.err) != EXPR_EVAL_OK) {
    return REDISMODULE_ERR;
  }
  return ExprEval_Eval(&r->ee, &r->res);
}

int EvalRec_EvalExpr(EvalRec *r, const char *expr) {
  r->ee.root = ExprAST_Parse(expr, strlen(expr), r->ee.err);
  if (r->ee.root == NULL) {
	  return REDISMODULE_ERR;
  }
  if (ExprAST_GetLookupKeys((RSExpr *) r->ee.root, (RLookup *) r->ee.lookup, r->ee.err) != EXPR_EVAL_OK) {
    return REDISMODULE_ERR;
  }
  return ExprEval_Eval(&r->ee, &r->res);
}

IndexSpec *SchemaIndex_FindByKey(RedisModuleCtx *ctx, RedisModuleString *key) {
  BB;
  
  EvalRec r;
  EvalRec_Init(&r, NULL);
  EvalRec_AddHash(&r, ctx, key);
  RLookupRow_Dump(&r.row);

  for (size_t i = 0; i < array_len(SchemaRules_g); i++) {
    IndexSpec *spec = &SchemaRules_g[i];
    if (EvalRec_EvalExpr(&r, spec->rule->setting.expr) == EXPR_EVAL_OK) {
      int b = RSValue_BoolTest(&r.res);
      if (b) {
        EvalRec_Destroy(&r);
        return spec;
      }
    }
    EvalRec_Destroy(&r);
  }
  return NULL;
}

static int HashNotificationCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
  BB;
  static const char *hset_event = 0, *hmset_event;
  bool hset = false, hmset = false;
  if (event == hset_event) {
    hset = true;
  } else if (event == hmset_event) {
    hmset = true;
  } else {
    if (!strcmp(event, "hset")) {
      hset = true;
      hset_event = event;
    } else if (!strcmp(event, "hmset")) {
      hmset = true;
      hmset_event = event;
    }
  }

  const char *key_cp = RedisModule_StringPtrLen(key, NULL);
  if (hset) {
    RedisModule_Log(ctx, "notice", "key %s: event %s", key_cp, event);
    IndexSpec *spec = SchemaIndex_FindByKey(ctx, key);
    if (spec) {
      UpdateIndexWithHash(ctx, spec, key);
    }
  }

  if (hmset) {
    RedisModule_Log(ctx, "notice", "key %s: event %s", key_cp, event);
  }

  return REDISMODULE_OK;
}

static void Initialize_KeyspaceNotifications(RedisModuleCtx *ctx) {
  RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_HASH, HashNotificationCallback);
  //RedisModule_NotifyKeyspaceEvent(ctx, REDISMODULE_NOTIFY_HASH, , RedisModuleString *key)
}

int RediSearch_Init(RedisModuleCtx *ctx, int mode) {
#define DO_LOG(...)                               \
  if (ctx && (mode != REDISEARCH_INIT_LIBRARY)) { \
    RedisModule_Log(ctx, ##__VA_ARGS__);          \
  }

  // Print version string!
  DO_LOG("notice", "RediSearch version %d.%d.%d (Git=%s)", REDISEARCH_VERSION_MAJOR,
         REDISEARCH_VERSION_MINOR, REDISEARCH_VERSION_PATCH, RS_GetExtraVersion());
  RS_Initialized = 1;

  RSDummyContext = RedisModule_GetThreadSafeContext(NULL);

  if (mode == REDISEARCH_INIT_MODULE && initAsModule(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  } else if (mode == REDISEARCH_INIT_LIBRARY && initAsLibrary(ctx) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }

  sds confstr = RSConfig_GetInfoString(&RSGlobalConfig);
  DO_LOG("notice", confstr);
  sdsfree(confstr);

  // Init extension mechanism
  Extensions_Init();

  Indexes_Init(ctx);

  if (RSGlobalConfig.concurrentMode) {
    ConcurrentSearch_ThreadPoolStart();
  }

  GC_ThreadPoolStart();

  // Init cursors mechanism
  CursorList_Init(&RSCursors);

  IndexAlias_InitGlobal();

  // Register aggregation functions
  RegisterAllFunctions();

  DO_LOG("notice", "Initialized thread pool!");

  /* Load extensions if needed */
  if (RSGlobalConfig.extLoad != NULL) {

    char *errMsg = NULL;
    // Load the extension so TODO: pass with param
    if (Extension_LoadDynamic(RSGlobalConfig.extLoad, &errMsg) == REDISMODULE_ERR) {
      DO_LOG("warning", "Could not load extension %s: %s", RSGlobalConfig.extLoad, errMsg);
      rm_free(errMsg);
      return REDISMODULE_ERR;
    }
    DO_LOG("notice", "Loaded RediSearch extension '%s'", RSGlobalConfig.extLoad);
  }

  // Register the default hard coded extension
  if (Extension_Load("DEFAULT", DefaultExtensionInit) == REDISEARCH_ERR) {
    DO_LOG("warning", "Could not register default extension");
    return REDISMODULE_ERR;
  }

  Initialize_KeyspaceNotifications(ctx);

  return REDISMODULE_OK;
}
