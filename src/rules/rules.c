#include "rules.h"
#include "spec.h"
#include "ruledefs.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "module.h"
#include "document.h"
#include "search_ctx.h"

#include <unistd.h>

/* Global list of all indexes created using `WITHRULES` */
static IndexSpec **rindexes_g = NULL;

SchemaRules *SchemaRules_g = NULL;
AsyncIndexQueue *asyncQueue_g = NULL;

SchemaRules *SchemaRules_Create(void) {
  SchemaRules *rules = rm_calloc(1, sizeof(*rules));
  dllist_init(&rules->rules);
  rules->actions = array_new(MatchAction, 1);
  return rules;
}

static void indexCallback(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx, void *unused) {
  // dummy
}

int SchemaRules_IndexDocument(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                              const IndexItemAttrs *attrs, QueryError *e) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  Document d = {0};
  RSAddDocumentCtx *aCtx = NULL;
  Document_Init(&d, item->kstr, attrs->score, attrs->language);
  d.keyobj = item->kobj;
  d.flags |= DOCUMENT_F_NEVEROWN;
  int rv = Document_LoadSchemaFields(&d, &sctx, e);
  if (rv != REDISMODULE_OK) {
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

void SchemaRules_ProcessItem(RedisModuleCtx *ctx, RuleKeyItem *item, int flags) {
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

    if ((flags & RULES_PROCESS_F_NOREINDEX) && DocTable_GetByKeyR(&spec->docs, item->kstr)) {
      // in SCAN mode and document already exists in the index
      continue;
    }

    // check if spec uses synchronous or asynchronous indexing..
    if ((flags & RULES_PROCESS_F_ASYNC) || (spec->flags & Index_Async)) {
      AIQ_Submit(asyncQueue_g, spec, results + ii, item);
    } else {
      QueryError e = {0};
      int rc = SchemaRules_IndexDocument(ctx, spec, item, &results[ii].attrs, &e);
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
  RuleKeyItem item = {.kstr = key, .kobj = NULL};
  SchemaRules_ProcessItem(ctx, &item, 0);
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

  // If it's a delete, broadcast the key to all 'WITHRULES' indexes
  size_t nidx = array_len(rindexes_g);
  for (size_t ii = 0; ii < array_len(rindexes_g); ++ii) {
    IndexSpec *sp = rindexes_g[ii];
    DocTable_DeleteR(&sp->docs, keyname);
    if (sp->flags & Index_Async) {
    }
  }

  // also, if the index is async, remove it from all async queues
  return REDISMODULE_OK;
}

void SchemaRules_ReplyAll(const SchemaRules *rules, RedisModuleCtx *ctx) {
}

void SchemaRules_InitGlobal(RedisModuleCtx *ctx) {
  asyncQueue_g = AIQ_Create(1000, 5);

  SchemaRules_g = SchemaRules_Create();
  RedisModule_SubscribeToKeyspaceEvents(RSDummyContext, REDISMODULE_NOTIFY_HASH, hashCallback);
  RedisModule_SubscribeToKeyspaceEvents(
      RSDummyContext, REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_EXPIRED, delCallback);
}

void SchemaRules_ShutdownGlobal() {
  AIQ_Destroy(asyncQueue_g);
  asyncQueue_g = NULL;
}

void SchemaRules_RegisterIndex(IndexSpec *sp) {
  if (!rindexes_g) {
    rindexes_g = array_new(IndexSpec *, 1);
  }
  rindexes_g = array_append(rindexes_g, sp);
}

void SchemaRules_UnregisterIndex(IndexSpec *sp) {
  if (!rindexes_g) {
    return;
  }

  size_t ix;
  for (ix = 0; ix < array_len(rindexes_g); ++ix) {
    if (rindexes_g[ix] == sp) {
      break;
    }
  }
  if (ix == array_len(rindexes_g)) {
    return;
  }
  rindexes_g = array_del_fast(rindexes_g, ix);
}