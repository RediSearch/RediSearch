#include "rules.h"
#include "spec.h"
#include "ruledefs.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "module.h"
#include "document.h"
#include "search_ctx.h"
#include "redis_version.h"

#include <unistd.h>

/* Global list of all indexes created using `WITHRULES` */
static IndexSpec **rindexes_g = NULL;

SchemaRules *SchemaRules_g = NULL;
AsyncIndexQueue *asyncQueue_g = NULL;
int SchemaRules_InitialScanStatus_g = 0;

SchemaRules *SchemaRules_Create(void) {
  SchemaRules *rules = rm_calloc(1, sizeof(*rules));
  rules->rules = array_new(SchemaRule *, 8);
  rules->actions = array_new(MatchAction, 1);
  return rules;
}

void SchemaRules_CleanRules(SchemaRules *rules) {
  size_t n = array_len(rules->rules);
  for (size_t ii = 0; ii < n; ++ii) {
    SchemaRule *rule = rules->rules[ii];
    SchemaRule_Free(rule);
  }
  array_free(rules->rules);
  rules->rules = NULL;
}

void SchemaRules_Free(SchemaRules *rules) {
  SchemaRules_CleanRules(rules);
  for (size_t ii = 0; ii < array_len(rules->actions); ++ii) {
    MatchAction_Clear(rules->actions + ii);
  }
  array_free(rules->actions);
  array_free(rules->rules);
  rules->revision = 0;
  rm_free(rules);
}

static void loadAttrFields(RuleKeyItem *item, const IndexItemAttrs *iia, Document *d) {
  SchemaAttrFieldpack *fp = iia->fp;
  if (!fp) {
    return;  // eh?
  }
  if (fp->lang) {
    RedisModuleString *langstr = NULL;
    RedisModule_HashGet(item->kobj, 0, fp->lang, &langstr, NULL);
    if (langstr) {
      RSLanguage lang = RSLanguage_Find(RedisModule_StringPtrLen(langstr, NULL));
      if (lang != RS_LANG_UNSUPPORTED) {
        d->language = lang;
      } else {
        // Send warning about invalid language?
      }
    }
  }

  if (fp->score) {
    RedisModuleString *scorestr = NULL;
    RedisModule_HashGet(item->kobj, 0, fp->score, &scorestr, NULL);
    double dbl = 0;
    if (scorestr) {
      int rc = RedisModule_StringToDouble(scorestr, &dbl);
      if (rc == REDISMODULE_OK) {
        d->score = dbl;
      } else {
        // send warning about bad score
      }
      RedisModule_FreeString(RSDummyContext, scorestr);
    }
  }
  if (fp->payload) {
    RedisModuleString *payload = NULL;
    RedisModule_HashGet(item->kobj, 0, fp->payload, &payload, NULL);
    if (payload) {
      size_t len;
      const char *buf = RedisModule_StringPtrLen(payload, &len);
      Document_SetPayload(d, buf, len);
      RedisModule_FreeString(RSDummyContext, payload);
    }
  }
}

RSAddDocumentCtx *SchemaRules_InitACTX(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                                       const IndexItemAttrs *attrs, QueryError *e) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);

  RSAddDocumentCtx *aCtx = NULL;
  if (!item->kobj) {
    item->kobj = RedisModule_OpenKey(ctx, item->kstr, REDISMODULE_READ);
    if (!item->kobj) {
      QueryError_SetError(e, QUERY_ENODOC, "Could not open document");
      return NULL;
    }
  }

  Document d = {.docKey = item->kstr};
  Document_MakeStringsOwner(&d);
  d.keyobj = item->kobj;
  if (attrs->predefMask & SCATTR_TYPE_LANGUAGE) {
    d.language = attrs->language;
  }
  if (attrs->predefMask & SCATTR_TYPE_SCORE) {
    d.score = attrs->score;
  }
  if (attrs->predefMask & SCATTR_TYPE_PAYLOAD) {
    size_t len;
    const char *buf = RedisModule_StringPtrLen(attrs->payload, &len);
    Document_SetPayload(&d, buf, len);
  }
  loadAttrFields(item, attrs, &d);

  int rv = Document_LoadSchemaFields(&d, &sctx, e);
  if (rv != REDISMODULE_OK) {
    goto err;
  }

  aCtx = ACTX_New(sp, &d, e);
  if (!aCtx) {
    goto err;
  }
  aCtx->options |= DOCUMENT_ADD_REPLACE;
  return aCtx;

err:
  Document_Free(&d);
  return NULL;
}

int SchemaRules_IndexDocument(RedisModuleCtx *ctx, IndexSpec *sp, RuleKeyItem *item,
                              const IndexItemAttrs *attrs, QueryError *e) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RSAddDocumentCtx *aCtx = SchemaRules_InitACTX(ctx, sp, item, attrs, e);
  if (!aCtx) {
    return REDISMODULE_ERR;
  }
  ACTX_Index(aCtx, &sctx, DOCUMENT_ADD_REPLACE);
  assert(QueryError_HasError(&aCtx->status) == 0);
  ACTX_Free(aCtx);
  return REDISMODULE_OK;
}

static int isAsync(IndexSpec *sp, int flags) {
  if (SchemaRules_InitialScanStatus_g == SC_INITSCAN_REQUIRED && !(flags & RULES_PROCESS_F_ASYNC)) {
    return 0;
  }

  if ((flags & RULES_PROCESS_F_ASYNC) || (sp->flags & Index_Async)) {
    return 1;
  }
  return 0;
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
    IndexSpec *spec = results[ii].spec;
    assert(spec);  // todo handle error...
    if (flags & RULES_PROCESS_F_NOREINDEX) {
      pthread_rwlock_rdlock(&spec->idxlock);
      int doContinue = !!DocTable_GetByKeyR(&spec->docs, item->kstr);
      pthread_rwlock_unlock(&spec->idxlock);
      if (doContinue) {
        continue;
      }
    }

    // check if spec uses synchronous or asynchronous indexing..
    if (isAsync(spec, flags)) {
      AIQ_Submit(asyncQueue_g, spec, results + ii, item);
    } else {
      QueryError e = {0};
      int rc = SchemaRules_IndexDocument(ctx, spec, item, &results[ii].attrs, &e);
      if (rc != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Could not index document %s (%s)",
                        RedisModule_StringPtrLen(item->kstr, NULL), QueryError_GetError(&e));
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

/**
 * In CRDT, any changes will go through the HASH callback, and it will
 * come with a simple 'change' event, which _includes_ deletions. The only
 * way to detect this is to actually open the key and see if it exists or
 * not. Sad!
 */
// static int crdtChangedCallback(...)

static int delCallback(RedisModuleCtx *ctx, int event, const char *action,
                       RedisModuleString *keyname) {
  int shouldDelete = 0;
  if (event & REDISMODULE_NOTIFY_TRIMMED) {
    RedisModule_Log(NULL, "debug", "Got trimmed notification");
    shouldDelete = 1;
  }
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

static void rdbLoadedCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent,
                              void *data) {
  // RDB is loaded, let's start scanning the indexes..
  if (subevent != REDISMODULE_SUBEVENT_LOADING_ENDED) {
    return;
  }
  SchemaRules_InitialScanStatus_g = SC_INITSCAN_REQUIRED;
  SchemaRules_StartScan(RSGlobalConfig.implicitLoadSync);
}

void SchemaRules_InitGlobal(RedisModuleCtx *ctx) {
  asyncQueue_g = AIQ_Create(1000, 5);

  SchemaRules_g = SchemaRules_Create();
  RedisModule_SubscribeToKeyspaceEvents(RSDummyContext, REDISMODULE_NOTIFY_HASH, hashCallback);

  int delflags = REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_GENERIC |
                 (IsEnterprise() ? REDISMODULE_NOTIFY_TRIMMED : 0);
  RedisModule_SubscribeToKeyspaceEvents(RSDummyContext, delflags, delCallback);
  RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_Loading, rdbLoadedCallback);
}

void SchemaRules_ShutdownGlobal() {
  if (asyncQueue_g) {
    AIQ_Destroy(asyncQueue_g);
    asyncQueue_g = NULL;
  }
  if (SchemaRules_g) {
    SchemaRules_Free(SchemaRules_g);
    SchemaRules_g = NULL;
  }
  if (rindexes_g) {
    array_free(rindexes_g);
    rindexes_g = NULL;
  }
}

void SchemaRules_RegisterIndex(IndexSpec *sp) {
  if (!rindexes_g) {
    rindexes_g = array_new(IndexSpec *, 1);
  }
  rindexes_g = array_append(rindexes_g, sp);
  IndexSpec_Incref(sp);
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
  // Iterate through all the rules which reference this index
  SchemaRules *rules = SchemaRules_g;
  for (size_t ii = 0; ii < array_len(rules->rules); ++ii) {
    SchemaRule *r = rules->rules[ii];
    if (r->spec == sp) {
      ARRAY_DEL_ITER(rules->rules, ii);
      SchemaRule_Free(r);
    }
  }
  IndexSpec_Decref(sp);
}

IndexSpec **SchemaRules_GetRegisteredIndexes(size_t *n) {
  *n = array_len(rindexes_g);
  return rindexes_g;
}

int SchemaRules_SetArgs(ArgsCursor *ac, QueryError *err) {
  size_t n = 0;
  int rc;
  if ((rc = AC_GetSize(ac, &n, 0) != AC_OK)) {
    QERR_MKBADARGS_AC(err, "<num args>", rc);
    return REDISMODULE_ERR;
  }

  SchemaRules *rules = SchemaRules_Create();
  for (size_t ii = 0; ii < n; ++ii) {
    ArgsCursor subac = {0};
    if ((rc = AC_GetVarArgs(ac, &subac)) != AC_OK) {
      QERR_MKBADARGS_AC_FMT(err, rc, "While parsing rule %u/%u", ii, n);
      goto done;
    }
    if (AC_NumRemaining(&subac) < 4) {
      QERR_MKBADARGS_FMT(err, "Not enough arguments for rule %u/%u", ii, n);
      goto done;
    }
    const char *name = AC_GetStringNC(&subac, NULL);
    const char *index = AC_GetStringNC(&subac, NULL);
    if (SchemaRules_AddArgs(index, name, &subac, err) != REDISMODULE_OK) {
      goto done;
    }
  }
  SchemaRules *oldrules = SchemaRules_g;
  SchemaRules_CleanRules(oldrules);
  oldrules->rules = rules->rules;
  oldrules->revision++;
  SchemaRules_StartScan(0);

  rules->rules = NULL;

done:
  SchemaRules_Free(rules);
  if (QueryError_HasError(err)) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int SchemaRules_AddArgs(const char *index, const char *name, ArgsCursor *ac, QueryError *err) {
  IndexSpec *sp = NULL;
  if (index != NULL && *index != '*') {
    sp = IndexSpec_Load(NULL, index, 0);
    if (!sp) {
      QueryError_SetErrorFmt(err, QUERY_ENOINDEX, "No such index %s", index);
      return REDISMODULE_ERR;
    }
    if (!(sp->flags & Index_UseRules)) {
      QueryError_SetError(err, QUERY_EBADATTR, "Index not declared with rules");
      return REDISMODULE_ERR;
    }
  }
  int rc = SchemaRules_AddArgsInternal(SchemaRules_g, sp, name, ac, err);
  if (rc == REDISMODULE_OK) {
    SchemaRules_g->revision++;
    SchemaRules_StartScan(0);
  }
  return rc;
}

SchemaCustomRule *SchemaRules_AddCustomRule(SchemaCustomCallback cb, void *arg, int pos) {
  SchemaCustomRule *crule = rm_calloc(1, sizeof(*crule));
  crule->rtype = SCRULE_TYPE_CUSTOM;
  crule->name = rm_strdup("__custom");
  crule->check = cb;
  crule->arg = arg;
  crule->action.atype = SCACTION_TYPE_CUSTOM;
  SchemaRules *rules = SchemaRules_g;
  if (pos == SCHEMA_CUSTOM_FIRST) {
    array_ensure_prepend(rules->rules, &rules, 1, SchemaRule *);
  } else {
    *(array_ensure_tail(&rules->rules, SchemaRule *)) = (SchemaRule *)crule;
  }
  return crule;
}

void SchemaRules_RemoveCustomRule(SchemaCustomRule *r) {
  SchemaRules *rules = SchemaRules_g;
  for (size_t ii = 0; ii < array_len(rules->rules); ++ii) {
    if (rules->rules[ii] == (void *)r) {
      array_del(rules->rules, ii);
      break;
    }
  }
  SchemaRule_Free((SchemaRule *)r);
}

#define RULES_CURRENT_VERSION 0

/**
 * FORMAT:
 * nrules (int)
 *  index (str)
 *  name (str)
 *  nargs (int)
 *   arg...
 */

static void rulesAuxSave(RedisModuleIO *rdb, int when) {
  if (when != REDISMODULE_AUX_AFTER_RDB) {
    // AIQ_SaveQueue(asyncQueue_g, rdb);
    return;
  }

  SchemaRules *rules = SchemaRules_g;
  RedisModule_SaveUnsigned(rdb, rules->revision);
  size_t nrules = 0;  // count non-custom rules only!
  for (size_t ii = 0; ii < array_len(rules->rules); ++ii) {
    if (rules->rules[ii]->rtype != SCRULE_TYPE_CUSTOM) {
      nrules++;
    }
  }
  RedisModule_SaveUnsigned(rdb, nrules);
  for (size_t ii = 0; ii < array_len(rules->rules); ++ii) {
    SchemaRule *r = rules->rules[ii];
    if (r->rtype == SCRULE_TYPE_CUSTOM) {
      continue;
    }
    size_t n = array_len(r->rawrule);
    const char *ixname = r->spec ? r->spec->name : "*";
    RedisModule_SaveStringBuffer(rdb, ixname, strlen(ixname));
    RedisModule_SaveStringBuffer(rdb, r->name, strlen(r->name));
    RedisModule_SaveUnsigned(rdb, n);
    for (size_t jj = 0; jj < n; ++jj) {
      const char *s = r->rawrule[jj];
      RedisModule_SaveStringBuffer(rdb, s, strlen(s));
    }
  }
}

static int rulesAuxLoad(RedisModuleIO *rdb, int encver, int when) {
  if (when != REDISMODULE_AUX_AFTER_RDB) {
    return REDISMODULE_OK;
  }
  SchemaRules *rules = SchemaRules_g;
  rules->revision = RedisModule_LoadUnsigned(rdb);
  size_t nrules = RedisModule_LoadUnsigned(rdb);
  for (size_t ii = 0; ii < nrules; ++ii) {
    size_t ns;
    RedisModuleString *index = RedisModule_LoadString(rdb);
    RedisModuleString *name = RedisModule_LoadString(rdb);
    size_t nargs = RedisModule_LoadUnsigned(rdb);
    RedisModuleString **args = rm_malloc(sizeof(*args) * nargs);
    for (size_t jj = 0; jj < nargs; ++jj) {
      args[jj] = RedisModule_LoadString(rdb);
    }
    ArgsCursor ac = {0};
    ArgsCursor_InitRString(&ac, args, nargs);
    QueryError status = {0};
    IndexSpec *sp = NULL;
    const char *ixstr = RedisModule_StringPtrLen(index, NULL);
    if (*ixstr && *ixstr != '*' && (sp = IndexSpec_Load(NULL, ixstr, 0)) == NULL) {
      printf("Couldn't load index %s\n", ixstr);
      return REDISMODULE_ERR;
    }
    int rc =
        SchemaRules_AddArgsInternal(rules, sp, RedisModule_StringPtrLen(name, NULL), &ac, &status);
    if (rc != REDISMODULE_OK) {
      printf("Couldn't load rules: %s\n", QueryError_GetError(&status));
    }
    RedisModule_FreeString(NULL, index);
    RedisModule_FreeString(NULL, name);
    for (size_t jj = 0; jj < nargs; ++jj) {
      RedisModule_FreeString(NULL, args[jj]);
    }
    rm_free(args);
    if (rc != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

int SchemaRules_RegisterType(RedisModuleCtx *ctx) {
  return REDISMODULE_OK;
}

void SchemaRules_Save(RedisModuleIO *rdb, int when) {
  rulesAuxSave(rdb, when);
}
int SchemaRules_Load(RedisModuleIO *rdb, int encver, int when) {
  return rulesAuxLoad(rdb, encver, when);
}

void SchemaRules_ReplyForIndex(RedisModuleCtx *ctx, IndexSpec *sp) {
  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  size_t n = 0;
  SchemaRules *rules = SchemaRules_g;
  for (size_t ii = 0; ii < array_len(rules->rules); ++ii) {
    const SchemaRule *r = rules->rules[ii];
    if (r->spec != sp) {
      continue;
    }
    RedisModule_ReplyWithArray(ctx, array_len(r->rawrule));
    for (size_t jj = 0; jj < array_len(r->rawrule); ++jj) {
      RedisModule_ReplyWithSimpleString(ctx, r->rawrule[jj]);
    }
    ++n;
  }
  RedisModule_ReplySetArrayLength(ctx, n);
}

size_t SchemaRules_IncrRevision(void) {
  return ++SchemaRules_g->revision;
}