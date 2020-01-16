#include "rules.h"
#include "spec.h"
#include "ruledefs.h"
#include "redismodule.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "util/arr.h"

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

static int matchPrefix(const SchemaRule *r, RedisModuleCtx *ctx, RedisModuleString *kstr) {
  SchemaPrefixRule *prule = (SchemaPrefixRule *)r;
  size_t n;
  const char *s = RedisModule_StringPtrLen(kstr, &n);
  if (prule->nprefix > n) {
    return 0;
  }
  return strncmp(prule->prefix, s, prule->nprefix) == 0;
}

static int matchExpression(const SchemaRule *r, RedisModuleCtx *ctx, RedisModuleString *kstr) {
  // ....
  return 0;
}

/**
 * The idea here is to allow multiple rule matching types, and to have a dynamic
 * function table for each rule type
 */
typedef int (*scruleMatchFn)(const SchemaRule *, RedisModuleCtx *, RedisModuleString *);

static scruleMatchFn matchfuncs_g[] = {[SCRULE_TYPE_KEYPREFIX] = matchPrefix,
                                       [SCRULE_TYPE_EXPRESSION] = matchExpression};

int SchemaRules_Check(const SchemaRules *rules, RedisModuleCtx *ctx, RedisModuleString *keystr,
                      MatchAction **results, size_t *nresults) {
  array_clear(rules->actions);
  *results = rules->actions;

  DLLIST_FOREACH(it, &rules->rules) {
    SchemaRule *rule = DLLIST_ITEM(it, SchemaRule, llnode);
    assert(rule->rtype == SCRULE_TYPE_KEYPREFIX);
    scruleMatchFn fn = matchfuncs_g[rule->rtype];
    if (!fn(rule, ctx, keystr)) {
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

static void keyspaceNotificationCallback(RedisModuleCtx *ctx, const char *action,
                                         RedisModuleString *key) {
  /**
   * Inspect the key, see which indexes match the key, and then perform the appropriate actions,
   * maybe in a different thread?
   */
  MatchAction *results = NULL;
  size_t nresults;
  SchemaRules_Check(SchemaRules_g, ctx, key, &results, &nresults);
  for (size_t ii = 0; ii < nresults; ++ii) {
    // submit the document for indexing if sync, async otherwise...
    IndexSpec *spec = IndexSpec_Load(ctx, results[ii].index, 1);
    assert(spec);  // todo handle error...
    // check if spec uses synchronous or asynchronous indexing..
    if (spec->flags & Index_Async) {
      // submit to queue
    } else {
      // Index immediately...
    }
  }
}