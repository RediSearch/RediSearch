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
#include "queue_ts.h"

typedef struct asyncIndexCtx {
  RedisModuleCtx *aiCtx;

  pthread_t aiThread;

  size_t interval;
} asyncIndexCtx;


/* May not be the ideal place for it */
static asyncIndexCtx asyncIndexCtx_g =  { 0 };
static IoQueue *asyncIndexQueue_g;

SchemaRules *SchemaRules_Create(void) {
  SchemaRules *rules = rm_calloc(1, sizeof(*rules));
  dllist_init(&rules->rules);
  return rules;
}

static SchemaIndexAction indexAction_g = {.atype = SCACTION_TYPE_INDEX};

int SchemaRules_AddArgs(SchemaRules *rules, const char *index, const char *name, ArgsCursor *ac,
                        QueryError *err) {
  // Let's add a static schema...
  SchemaPrefixRule *r = rm_calloc(1, sizeof(*r));
  r->index = rm_strdup(index);
  r->name = rm_strdup(name);
  r->rtype = SCRULE_TYPE_KEYPREFIX;
  r->action = &indexAction_g;
  dllist_append(&rules->rules, &r->llnode);
  return REDISMODULE_OK;
}

static int matchPrefix(const SchemaRule *r, RedisModuleCtx *ctx, RuleKeyItem *item) {
  SchemaPrefixRule *prule = (SchemaPrefixRule *)r;
  size_t n;
  const char *s = RedisModule_StringPtrLen(item->kstr, &n);
  if (prule->nprefix > n) {
    return 0;
  }
  return strncmp(prule->prefix, s, prule->nprefix) == 0;
}

static int matchExpression(const SchemaRule *r, RedisModuleCtx *ctx, RuleKeyItem *item) {
  // ....
  return 0;
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
    QueryError_SetError(e, QUERY_ENODOC, "No indexable fields in document");
    goto err;
  }

  aCtx = NewAddDocumentCtx(sp, &d, e);
  if (!aCtx) {
    goto err;
  }
  AddDocumentCtx_Submit(aCtx, &sctx,
          DOCUMENT_ADD_REPLACE | DOCUMENT_ADD_NOSAVE | DOCUMENT_ADD_CURTHREAD);
  return REDISMODULE_OK;

err:
  Document_Free(&d);
  return REDISMODULE_ERR;
}

/**
 * The idea here is to allow multiple rule matching types, and to have a dynamic
 * function table for each rule type
 */
typedef int (*scruleMatchFn)(const SchemaRule *, RedisModuleCtx *, RuleKeyItem *);

static scruleMatchFn matchfuncs_g[] = {[SCRULE_TYPE_KEYPREFIX] = matchPrefix,
                                       [SCRULE_TYPE_EXPRESSION] = matchExpression};

int SchemaRules_Check(const SchemaRules *rules, RedisModuleCtx *ctx, RuleKeyItem *item,
                      MatchAction **results, size_t *nresults) {
  array_clear(rules->actions);
  *results = rules->actions;

  DLLIST_FOREACH(it, &rules->rules) {
    SchemaRule *rule = DLLIST_ITEM(it, SchemaRule, llnode);
    assert(rule->rtype == SCRULE_TYPE_KEYPREFIX);
    scruleMatchFn fn = matchfuncs_g[rule->rtype];
    if (!fn(rule, ctx, item)) {
      continue;
    }

    MatchAction *curAction = NULL;
    for (size_t ii = 0; ii < *nresults; ++ii) {
      if (!strcmp((*results)[ii].index, rule->index)) {
        curAction = (*results) + ii;
      }
    }
    if (!curAction) {
      curAction = array_ensure_tail(results, MatchAction);
      curAction->index = rule->index;
    }
    assert(rule->action->atype == SCACTION_TYPE_INDEX);
  }
  *nresults = array_len(*results);
  return *nresults;
}

static void processKeyItem(RedisModuleCtx *ctx, RuleKeyItem *item, int forceQueue) {
  /**
   * Inspect the key, see which indexes match the key, and then perform the appropriate actions,
   * maybe in a different thread?
   */
  MatchAction *results = NULL;
  size_t nresults;
  SchemaRules_Check(SchemaRules_g, ctx, item, &results, &nresults);
  for (size_t ii = 0; ii < nresults; ++ii) {
    // submit the document for indexing if sync, async otherwise...
    IndexSpec *spec = IndexSpec_Load(ctx, results[ii].index, 1);
    assert(spec);  // todo handle error...
    // check if spec uses synchronous or asynchronous indexing..
    if (forceQueue || (spec->flags & Index_Async)) {
      // submit to queue
      // 1. Create a queue per index
      // 2. Add `item` to queue
      // Somewhere else
      // 3. As a callback, index items into the index
      // 4. Remove items from queue 


      // Queue indexes with documents to be indexed
      // Queue documents to be indexed for each index
      // Schedule

      // index points to a retained ptr at SchemaRules_g
      io_queue_push(asyncIndexQueue_g, results[ii].index);
      io_queue_push(spec->asyncIndexQueue, item); // we do not have the results[ii].attrs. Should save both?
    } else {
      QueryError e = {0};
      int rc = indexDocument(ctx, spec, item, &results[ii].attrs, &e);
      assert(rc == REDISMODULE_OK);
    }
  }
}

static void keyspaceNotificationCallback(RedisModuleCtx *ctx, const char *action,
                                         RedisModuleString *key) {
  RuleKeyItem item = {.kstr = key, .kobj = NULL};
  processKeyItem(ctx, &item, 0);
  if (item.kobj) {
    RedisModule_CloseKey(item.kobj);
  }
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

void aiThreadInit(void *privdata) {
  while(1) {
    while (io_queue_has_front(asyncIndexQueue_g) == IO_QUEUE_RESULT_TRUE) {
      // Get index from queue
      IndexSpec *spec = { 0 };
      io_queue_front(asyncIndexQueue_g, spec);

      RuleKeyItem *rki = { 0 };
      while (io_queue_has_front(spec->asyncIndexQueue == IO_QUEUE_RESULT_TRUE)) {
        io_queue_front(spec->asyncIndexQueue, rki);
        QueryError e = {0};
        int rc = indexDocument(asyncIndexCtx_g.aiCtx, spec, rki, NULL/*&results[ii].attrs*/, &e);
        assert(rc == REDISMODULE_OK);
      }
      
    } 
    usleep(asyncIndexCtx_g.interval);
  }
}

void SchemaRules_InitGlobal(RedisModuleCtx *ctx) {
  if(IO_QUEUE_RESULT_SUCCESS != io_queue_init(asyncIndexQueue_g, sizeof(char *))) {
    RedisModule_Log(ctx, "verbose", "%s", "Creation of global async queue for rules failed");
  }
  asyncIndexCtx_g.aiCtx = RedisModule_GetThreadSafeContext(NULL);
  asyncIndexCtx_g.interval = 1000; // interval in milliseconds

  pthread_create(&asyncIndexCtx_g.aiThread, NULL, aiThreadInit, NULL);
  pthread_detach(asyncIndexCtx_g.aiThread);
}

void SchemaRules_ShutdownGlobal() {
  io_queue_clear(asyncIndexQueue_g);
  RedisModule_FreeThreadSafeContext(asyncIndexCtx_g);
}