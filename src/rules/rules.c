#include "rules.h"
#include "spec.h"
#include "ruledefs.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "util/arr.h"
#include "module.h"
#include "document.h"
#include "search_ctx.h"

SchemaRules *SchemaRules_g = NULL;

SchemaRules *SchemaRules_Create(void) {
  SchemaRules *rules = rm_calloc(1, sizeof(*rules));
  dllist_init(&rules->rules);
  rules->actions = array_new(MatchAction, 1);
  return rules;
}

static void indexCallback(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx, void *unused) {
  // dummy
}

static int indexDocument(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                         const IndexItemAttrs *attrs, QueryError *e) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  Document d = {0};
  RSAddDocumentCtx *aCtx = NULL;
  Document_Init(&d, item->kstr, attrs->score, attrs->language);
  d.keyobj = item->kobj;
  d.flags |= DOCUMENT_F_NEVEROWN;
  int rv = Document_LoadSchemaFields(&d, &sctx);
  if (rv != REDISMODULE_OK) {
    QueryError_SetError(e, QUERY_ENODOC, "Couldn't load fields");
    goto err;
  }
  if (!d.numFields) {
    QueryError_SetError(e, QUERY_ENOIDXFIELDS, "No indexable fields in document");
    goto err;
  }

  aCtx = NewAddDocumentCtx(sp, &d, e);
  if (!aCtx) {
    goto err;
  }
  aCtx->stateFlags |= ACTX_F_NOBLOCK;  // disable "blocking", i.e. threading
  aCtx->donecb = indexCallback;
  AddDocumentCtx_Submit(aCtx, &sctx, DOCUMENT_ADD_REPLACE | DOCUMENT_ADD_NOSAVE);
  return REDISMODULE_OK;

err:
  Document_Free(&d);
  return REDISMODULE_ERR;
}

static void processKeyItem(RedisModuleCtx *ctx, RuleKeyItem *item, int forceQueue) {
  /**
   * Inspect the key, see which indexes match the key, and then perform the appropriate actions,
   * maybe in a different thread?
   */
  MatchAction *results = NULL;
  size_t nresults = 0;
  SchemaRules_Check(SchemaRules_g, ctx, item, &results, &nresults);
  for (size_t ii = 0; ii < nresults; ++ii) {
    // submit the document for indexing if sync, async otherwise...
    IndexSpec *spec = IndexSpec_Load(ctx, results[ii].index, 1);
    assert(spec);  // todo handle error...
    // check if spec uses synchronous or asynchronous indexing..
    if (forceQueue || (spec->flags & Index_Async)) {
      // submit to queue
    } else {
      QueryError e = {0};
      int rc = indexDocument(ctx, spec, item, &results[ii].attrs, &e);
      if (rc != REDISMODULE_OK) {
        printf("Couldn't index document: %s\n", QueryError_GetError(&e));
        if (e.code == QUERY_ENOIDXFIELDS) {
          rc = REDISMODULE_OK;
        }
        QueryError_ClearError(&e);
      }
      assert(rc == REDISMODULE_OK);
    }
  }
}

static int hashCallback(RedisModuleCtx *ctx, int unused, const char *action,
                        RedisModuleString *key) {
  printf("int:%d, action:%s\n", unused, action);
  RuleKeyItem item = {.kstr = key, .kobj = NULL};
  processKeyItem(ctx, &item, 0);
  if (item.kobj) {
    RedisModule_CloseKey(item.kobj);
  }
  return REDISMODULE_OK;
}

static int delCallback(RedisModuleCtx *ctx, int event, const char *action,
                       RedisModuleString *keyname) {
  int shouldDelete = 0;
  if (event & (REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED)) {
    shouldDelete = 1;
  } else if (event == REDISMODULE_NOTIFY_GENERIC && *action == 'd') {
    shouldDelete = 1;
  }
  if (!shouldDelete) {
    return REDISMODULE_OK;
  }

  // TODO: Handle RENAME

  MatchAction *results = NULL;
  size_t nresults = 0;
  RuleKeyItem rki = {.kstr = keyname};
  SchemaRules_Check(SchemaRules_g, ctx, &rki, &results, &nresults);
  for (size_t ii = 0; ii < nresults; ++ii) {
    // Remove the document from the index
    IndexSpec *sp = IndexSpec_Load(ctx, results[ii].index, 1);
    if (!sp) {
      continue;
    }
    DocTable_DeleteR(&sp->docs, keyname);
  }

  return REDISMODULE_OK;
}

static void scanCallback(RedisModuleCtx *ctx, RedisModuleString *keyname, RedisModuleKey *keyobj,
                         void *privdata) {
  // body here should be similar to keyspace notification callback, except that
  // async is always forced
  RuleKeyItem item = {.kstr = keyname, .kobj = keyobj};
  processKeyItem(ctx, &item, 1);
}

void SchemaRules_ScanAll(const SchemaRules *rules) {
  RedisModuleCtx *ctx = RSDummyContext;
  RedisModuleScanCursor *cursor = RedisModule_ScanCursorCreate();
  RedisModule_Scan(ctx, cursor, scanCallback, NULL);
  RedisModule_ScanCursorDestroy(cursor);
}

void SchemaRules_ReplyAll(const SchemaRules *rules, RedisModuleCtx *ctx) {
}

void SchemaRules_InitGlobal(void) {
  SchemaRules_g = SchemaRules_Create();
  RedisModule_SubscribeToKeyspaceEvents(RSDummyContext, REDISMODULE_NOTIFY_HASH, hashCallback);
  RedisModule_SubscribeToKeyspaceEvents(
      RSDummyContext, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_EXPIRED, delCallback);
}
