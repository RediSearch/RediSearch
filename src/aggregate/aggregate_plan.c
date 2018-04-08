#include "aggregate.h"
#include "expr/expression.h"
#include <commands.h>
#include <util/arr.h>

#define FMT_ERR(e, fmt, ...) asprintf(e, fmt, __VA_ARGS__);
#define SET_ERR(e, err) *e = strdup(err);

AggregateStep *newStep(AggregateStepType t) {
  AggregateStep *step = malloc(sizeof(*step));
  step->type = t;
  step->next = NULL;
  step->prev = NULL;
  return step;
}

AggregateStep *newLoadStep(CmdArg *arg, char **err) {

  RSMultiKey *k = RS_NewMultiKeyFromArgs(&CMDARG_ARR(arg), 1);
  AggregateStep *ret = newStep(AggregateStep_Load);
  ret->load.keys = k;
  return ret;
}

AggregateStep *newApplyStep(const char *alias, const char *expr, char **err) {
  RSExpr *pe = RSExpr_Parse(expr, strlen(expr), err);
  if (!pe) {
    return NULL;
  }
  AggregateStep *ret = newStep(AggregateStep_Apply);
  ret->apply = (AggregateApplyStep){
      .rawExpr = expr,
      .parsedExpr = pe,
      .alias = alias,
  };
  return ret;
}

AggregateStep *newApplyStepFmt(const char *alias, char **err, const char *fmt, ...) {
  char *exp;
  va_list ap;
  va_start(ap, fmt);
  vasprintf(&exp, fmt, ap);
  va_end(ap);
  AggregateStep *st = newApplyStep(alias, exp, err);
  if (!st) {
    free(exp);
  }
  return st;
}

AggregateStep *newApplyStepArgs(CmdArg *arg, char **err) {

  CmdArg *expr = CmdArg_FirstOf(arg, "expr");
  if (!expr || CMDARG_TYPE(expr) != CmdArg_String) {
    SET_ERR(err, "Missing or invalid projection expression");
    return NULL;
  }

  const char *exp = CMDARG_STRPTR(expr);
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(arg, "AS"), CMDARG_STRPTR);
  return newApplyStep(alias, exp, err);
}

AggregateStep *newSortStep(CmdArg *srt, char **err) {
  CmdArg *by = CmdArg_FirstOf(srt, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  RSMultiKey *keys = RS_NewMultiKey(CMDARG_ARRLEN(by));
  // We build a bitmap of maximum 64 sorting parameters. 1 means asc, 2 desc
  // By default all bits are 1. Whenever we encounter DESC we flip the corresponding bit
  uint64_t ascMap = 0xFFFFFFFFFFFFFFFF;
  int n = 0;

  // since ASC/DESC are optional, we need a stateful parser
  // state 0 means we are open only to property names
  // state 1 means we are open to either a new property or ASC/DESC
  int state = 0, i = 0;
  for (i = 0; i < CMDARG_ARRLEN(by) && i < sizeof(ascMap) * 8; i++) {
    const char *str = CMDARG_STRPTR(CMDARG_ARRELEM(by, i));
    // New properties are accepted in either state
    if (*str == '@') {
      keys->keys[n++] = RS_KEY(RSKEY(str));
      state = 1;
    } else if (state == 0) {
      // At state 0 we only accept properties, so we don't even need to see what this is
      goto err;
    } else if (!strcasecmp(str, "asc")) {
      // For as - we put a 1 in the map. We don't actually need to, this is just for readability
      ascMap |= 1 << (n - 1);
      // switch back to state 0, ASC/DESC cannot follow ASC
      state = 0;
    } else if (!strcasecmp(str, "desc")) {
      // We turn the current bit to 0 meaning desc for the Nth property
      ascMap &= ~(1 << (n - 1));
      // switch back to state 0, ASC/DESC cannot follow ASC
      state = 0;
    } else {
      // Unkown token - neither a property nor ASC/DESC
      goto err;
    }
  }
  keys->len = n;
  // Parse optional MAX
  CmdArg *max = CmdArg_FirstOf(srt, "MAX");
  long long mx = 0;
  if (max) {
    mx = CMDARG_INT(max);
    if (mx < 0) mx = 0;
  }

  AggregateStep *ret = newStep(AggregateStep_Sort);
  ret->sort = (AggregateSortStep){
      .keys = keys,
      .ascMap = ascMap,
      .max = mx,
  };
  return ret;
err:
  FMT_ERR(err, "Invalid SORTBY arguments near '%s'", CMDARG_STRPTR(CMDARG_ARRELEM(by, i)));
  if (keys) RSMultiKey_Free(keys);
  return NULL;
}

AggregateStep *newLimit(CmdArg *arg, char **err) {
  long long offset, limit;
  offset = CMDARG_INT(CMDARG_ARRELEM(arg, 0));
  limit = CMDARG_INT(CMDARG_ARRELEM(arg, 1));

  if (offset < 0 || limit <= 0) {
    SET_ERR(err, "Invalid offset/num for LIMIT");
    return NULL;
  }

  AggregateStep *ret = newStep(AggregateStep_Limit);
  ret->limit.offset = offset;
  ret->limit.num = limit;
  return ret;
}

size_t group_numReducers(AggregateGroupStep *g) {
  return array_len(g->reducers);
}

const char *getReducerAlias(AggregateGroupStep *g, const char *func) {

  char *ret;
  asprintf(&ret, "grp%d_%s%d", g->idx, func, array_len(g->reducers));
  for (char *c = ret; *c; c++) {
    *c = tolower(*c);
  }
  return ret;
}
const char *group_addReducer(AggregateGroupStep *g, const char *func, const char *alias, int argc,
                             ...) {
  if (!g->reducers) {
    g->reducers = array_new(AggregateGroupReduce, 1);
  }
  RSValue **arr = array_newlen(RSValue *, argc);
  va_list ap;
  va_start(ap, argc);
  for (int i = 0; i < argc; i++) {
    arr[i] = RSValue_IncrRef(va_arg(ap, RSValue *));
  }
  va_end(ap);
  if (!alias) {
    alias = getReducerAlias(g, func);
  } else {
    alias = strdup(alias);
  }

  g->reducers = array_append(g->reducers, ((AggregateGroupReduce){
                                              .reducer = func,
                                              .alias = alias,
                                              .args = arr,
                                          }));
  return alias;
}

void buildReducer(AggregateGroupStep *g, AggregateGroupReduce *gr, CmdArg *red, char **err) {

  gr->reducer = CMDARG_STRPTR(CmdArg_FirstOf(red, "func"));
  CmdArg *args = CmdArg_FirstOf(red, "args");
  gr->args = NULL;
  if (CMDARG_ARRLEN(args) > 0) {
    gr->args = array_newlen(RSValue *, CMDARG_ARRLEN(args));
    for (int i = 0; i < CMDARG_ARRLEN(args); i++) {
      gr->args[i] = RSValue_IncrRef(RS_NewValueFromCmdArg(CMDARG_ARRELEM(args, i)));
    }
  }
  gr->alias = CMDARG_ORNULL(CmdArg_FirstOf(red, "AS"), CMDARG_STRPTR);
  if (!gr->alias) {
    gr->alias = getReducerAlias(g, gr->reducer);
  } else {
    gr->alias = strdup(gr->alias);
  }
}

AggregateStep *newGroupStep(int idx, CmdArg *grp, char **err) {
  CmdArg *by = CmdArg_FirstOf(grp, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) {
    SET_ERR(err, "No fields for GROUPBY");
    return NULL;
  }
  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by), 1);
  size_t numReducers = CmdArg_Count(grp, "REDUCE");
  AggregateGroupReduce *arr = NULL;
  if (numReducers) {
    arr = array_new(AggregateGroupReduce, numReducers);
  }

  AggregateStep *ret = newStep(AggregateStep_Group);
  ret->group = (AggregateGroupStep){
      .properties = keys,
      .reducers = arr,
      .idx = idx,  // FIXME: Global counter
  };
  // Add reducers
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    AggregateGroupReduce agr;
    buildReducer(&ret->group, &agr, result, err);
    ret->group.reducers = array_append(ret->group.reducers, agr);
  });

  return ret;
}

AggregateSchema AggregateSchema_Set(AggregateSchema schema, const char *property, RSValueType t,
                                    AggregatePropertyKind kind, int replace) {
  assert(property);
  for (size_t i = 0; i < array_len(schema); i++) {
    AggregateProperty p = schema[i];
    if (!strcasecmp(RSKEY(schema[i].property), RSKEY(property))) {
      if (replace) {
        schema[i].kind = kind;
        schema[i].type = t;
      }
      return schema;
    }
  }
  schema = array_append(schema, ((AggregateProperty){property, t, kind}));
  return schema;
}

int AggregateSchema_Contains(AggregateSchema schema, const char *property) {
  for (size_t i = 0; i < array_len(schema); i++) {

    if (!strcasecmp(RSKEY(schema[i].property), RSKEY(property))) {
      return 1;
    }
  }
  return 0;
}

AggregateSchema extractExprTypes(RSExpr *expr, AggregateSchema arr, RSValueType typeHint) {
  switch (expr->t) {
    case RSExpr_Function: {
      RSValueType funcType = GetExprType(expr);
      for (int i = 0; i < expr->func.args->len; i++) {
        arr = extractExprTypes(expr->func.args->args[i], arr, funcType);
      }
      break;
    }
    case RSExpr_Property:
      arr = AggregateSchema_Set(arr, expr->property.key, typeHint, Property_Field, 0);
      break;
    case RSExpr_Op:
      // ops are between numeric properties, so the hint is number
      arr = extractExprTypes(expr->op.left, arr, RSValue_Number);
      arr = extractExprTypes(expr->op.right, arr, RSValue_Number);
      break;
    case RSExpr_Literal:
    default:
      break;
  }
  return arr;
}
AggregateSchema AggregatePlan_GetSchema(AggregatePlan *plan, RSSortingTable *tbl) {
  AggregateStep *current = plan->head;
  AggregateSchema arr = array_new(AggregateProperty, 8);
  while (current) {
    switch (current->type) {
      case AggregateStep_Apply:
        arr = extractExprTypes(current->apply.parsedExpr, arr, RSValue_String);
        arr = AggregateSchema_Set(arr, current->apply.alias, GetExprType(current->apply.parsedExpr),
                                  Property_Projection, 1);
        break;

      case AggregateStep_Load:
        for (int i = 0; i < current->load.keys->len; i++) {
          arr = AggregateSchema_Set(arr, current->load.keys->keys[i].key, RSValue_String,
                                    Property_Field, 1);
        }
        break;
      case AggregateStep_Sort:
        for (int i = 0; i < current->sort.keys->len; i++) {
          arr = AggregateSchema_Set(arr, current->sort.keys->keys[i].key, RSValue_String,
                                    Property_Field, 0);
        }
        break;
      case AggregateStep_Group:
        for (int i = 0; i < current->group.properties->len; i++) {
          arr = AggregateSchema_Set(arr, current->group.properties->keys[i].key, RSValue_String,
                                    Property_Field, 0);
        }
        for (int i = 0; i < array_len(current->group.reducers); i++) {
          AggregateGroupReduce *red = &current->group.reducers[i];

          for (int j = 0; j < array_len(red->args); j++) {
            if (RSValue_IsString(red->args[j])) {
              const char *c = RSValue_StringPtrLen(red->args[j], NULL);
              if (c && *c == '@') {
                AggregateSchema_Set(arr, c, RSValue_String, Property_Field, 0);
              }
            }
          }
          if (red->alias) {
            arr = AggregateSchema_Set(arr, red->alias, GetReducerType(red->reducer),
                                      Property_Aggregate, 1);
          }
        }
        break;
      case AggregateStep_Limit:
      default:
        break;
    }
    current = current->next;
  }

  return arr;
}

void step_AddAfter(AggregateStep *step, AggregateStep *add) {
  add->next = step->next;
  if (step->next) step->next->prev = add;
  add->prev = step;
  step->next = add;
}

void step_AddBefore(AggregateStep *step, AggregateStep *add) {
  add->prev = step->prev;
  if (add->prev) add->prev->next = add;

  // if add is several steps connected, go to the end
  while (add->next) {
    add = add->next;
  }

  add->next = step;
  step->prev = add;
}

/* Detach the step and return the previous next of it */
AggregateStep *step_Detach(AggregateStep *step) {
  AggregateStep *tmp = step->next;
  if (step->next) step->next->prev = step->prev;
  if (step->prev) step->prev->next = step->next;
  step->prev = NULL;
  step->next = NULL;
  return tmp;
}
static void plan_AddStep(AggregatePlan *plan, AggregateStep *step) {
  // We assume head and tail are sentinels
  step_AddBefore(plan->tail, step);
  plan->size++;
}
int AggregatePlan_Build(AggregatePlan *plan, CmdArg *cmd, char **err) {
  *plan = (AggregatePlan){0};
  if (!cmd || CMDARG_TYPE(cmd) != CmdArg_Object || CMDARG_OBJLEN(cmd) < 3) {
    goto fail;
  }
  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  int n = 1;
  plan->head = newStep(AggregateStep_Dummy);
  plan->tail = newStep(AggregateStep_Dummy);
  plan->tail->prev = plan->head;
  plan->head->next = plan->tail;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    AggregateStep *next = NULL;
    if (!strcasecmp(key, "idx")) {
      plan->index = CMDARG_STRPTR(child);
      continue;
    } else if (!strcasecmp(key, "query")) {
      next = newStep(AggregateStep_Query);
      next->query.str = CMDARG_STRPTR(child);
    } else if (!strcasecmp(key, "GROUPBY")) {
      next = newGroupStep(n++, child, err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = newSortStep(child, err);
    } else if (!strcasecmp(key, "APPLY")) {
      next = newApplyStepArgs(child, err);
    } else if (!strcasecmp(key, "LIMIT")) {
      next = newLimit(child, err);
    } else if (!strcasecmp(key, "LOAD")) {
      next = newLoadStep(child, err);
    }
    if (!next) {
      goto fail;
    }
    plan_AddStep(plan, next);
  }

  return 1;

fail:
  if (plan->head) {
    AggregateStep *current = plan->head;
    while (current) {
      AggregateStep *tmp = current->next;
      free(current);  // TODO: step_free
      current = tmp;
    }
    plan->head = plan->tail = NULL;
    plan->size = 0;
  }
  return 0;
}

void arrPushStrdup(char ***v, const char *s) {
  char *c = strdup(s);
  *v = array_append(*v, c);
}

void arrPushStrfmt(char ***v, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  char *c;
  vasprintf(&c, fmt, ap);
  va_end(ap);

  *v = array_append(*v, c);
}

void serializeGroup(AggregateGroupStep *g, char ***v) {
  arrPushStrdup(v, "GROUPBY");
  arrPushStrfmt(v, "%d", g->properties->len);

  for (int i = 0; i < g->properties->len; i++) {
    arrPushStrfmt(v, "@%s", g->properties->keys[i].key);
  }
  for (int i = 0; i < group_numReducers(g); i++) {
    arrPushStrdup(v, "REDUCE");
    arrPushStrdup(v, g->reducers[i].reducer);
    arrPushStrfmt(v, "%d", array_len(g->reducers[i].args));
    if (g->reducers[i].args) {
      RSValue tmp = {.allocated = 0};

      for (int j = 0; j < array_len(g->reducers[i].args); j++) {
        RSValue_ToString(&tmp, g->reducers[i].args[j]);
        arrPushStrdup(v, RSValue_StringPtrLen(&tmp, NULL));
        RSValue_Free(&tmp);
      }
    }
    if (g->reducers[i].alias) {
      arrPushStrdup(v, "AS");
      arrPushStrdup(v, g->reducers[i].alias);
    }
  }
}

void serializeSort(AggregateSortStep *s, char ***v) {
  arrPushStrdup(v, "SORTBY");
  arrPushStrfmt(v, "%d", s->keys->len * 2);
  for (int i = 0; i < s->keys->len; i++) {
    arrPushStrfmt(v, "@%s", s->keys->keys[i].key);
    arrPushStrdup(v, s->ascMap & (1 << i) ? "ASC" : "DESC");
  }
  if (s->max) {
    arrPushStrdup(v, "MAX");
    arrPushStrfmt(v, "%d", s->max);
  }
}

void serializeApply(AggregateApplyStep *a, char ***v) {
  arrPushStrdup(v, "APPLY");
  arrPushStrdup(v, a->rawExpr);
  arrPushStrdup(v, "AS");
  arrPushStrdup(v, a->alias);
}

void serializeLimit(AggregateLimitStep *l, char ***v) {
  arrPushStrdup(v, "LIMIT");
  arrPushStrfmt(v, "%lld", l->offset);
  arrPushStrfmt(v, "%lld", l->num);
}

void serializeLoad(AggregateLoadStep *l, char ***v) {
  arrPushStrdup(v, "LOAD");
  arrPushStrfmt(v, "%d", l->keys->len);
  for (int i = 0; i < l->keys->len; i++) {
    arrPushStrfmt(v, "@%s", l->keys->keys[i].key);
  }
}

char **AggregatePlan_Serialize(AggregatePlan *plan) {
  char **vec = array_new(char *, 10);
  arrPushStrdup(&vec, RS_AGGREGATE_CMD);

  if (plan->index) arrPushStrdup(&vec, plan->index);

  AggregateStep *current = plan->head;
  while (current) {
    switch (current->type) {
      case AggregateStep_Query:
        arrPushStrdup(&vec, current->query.str);
        break;

      case AggregateStep_Group:
        serializeGroup(&current->group, &vec);
        break;
      case AggregateStep_Sort:
        serializeSort(&current->sort, &vec);
        break;

      case AggregateStep_Apply:
        serializeApply(&current->apply, &vec);
        break;

      case AggregateStep_Limit:
        serializeLimit(&current->limit, &vec);
        break;

      case AggregateStep_Load:
        serializeLoad(&current->load, &vec);
        break;

      case AggregateStep_Distribute: {
        arrPushStrdup(&vec, "{{");
        char **sub = AggregatePlan_Serialize(current->dist.plan);
        for (int k = 0; k < array_len(sub); k++) {
          vec = array_append(vec, sub[k]);
        }
        arrPushStrdup(&vec, "}}");
        array_free(sub, NULL);
        break;
      }
      case AggregateStep_Dummy:
        break;
    }
    current = current->next;
  }

  return vec;
}

AggregateStep *plan_MoveStep(AggregatePlan *src, AggregatePlan *dist, AggregateStep *step) {
  AggregateStep *next = step_Detach(step);

  plan_AddStep(dist, step);
  return next;
}

typedef struct {

} PlanProcessingCtx;

#define PROPVAL(p) (RS_StringValFmt("@%s", RSKEY(p)))
const char *getReducerAlias(AggregateGroupStep *g, const char *func);

int distributeCount(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  group_addReducer(&remote->group, "COUNT", RSKEY(src->alias), 0);

  group_addReducer(&local->group, "SUM", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

int distributeSum(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  group_addReducer(&remote->group, "SUM", RSKEY(src->alias), 1, src->args[0]);

  group_addReducer(&local->group, "SUM", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

int distributeMin(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  group_addReducer(&remote->group, "MIN", RSKEY(src->alias), 1, src->args[0]);

  group_addReducer(&local->group, "MIN", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

int distributeMax(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {
  group_addReducer(&remote->group, "MAX", RSKEY(src->alias), 1, src->args[0]);

  group_addReducer(&local->group, "MAX", RSKEY(src->alias), 1, PROPVAL(src->alias));

  return 1;
}

int distributeAvg(AggregateGroupReduce *src, AggregateStep *local, AggregateStep *remote) {

  // Add count and sum remotely
  const char *countAlias = group_addReducer(&remote->group, "COUNT", NULL, 0);
  const char *sumAlias = group_addReducer(&remote->group, "SUM", NULL, 1, src->args[0]);

  group_addReducer(&local->group, "SUM", sumAlias, 1, PROPVAL(sumAlias));
  group_addReducer(&local->group, "SUM", countAlias, 1, PROPVAL(countAlias));

  char *err;
  AggregateStep *as = newApplyStepFmt(src->alias, &err, "(@%s/@%s)", sumAlias, countAlias);
  if (!as) {
    return 0;
    // TODO: Free everything
  }
  step_AddAfter(local, as);
  return 1;
}
AggregateStep *distributeGroupStep(AggregatePlan *src, AggregatePlan *dist, AggregateStep *step,
                                   int *cont) {
  AggregateGroupStep *gr = &step->group;
  AggregateStep *remoteStep = newStep(AggregateStep_Group);
  AggregateStep *localStep = newStep(AggregateStep_Group);

  AggregateGroupStep *remote = &remoteStep->group;
  AggregateGroupStep *local = &localStep->group;
  remote->idx = step->group.idx;
  local->idx = step->group.idx;

  remote->properties = gr->properties;
  local->properties = gr->properties;
  remote->reducers = array_new(AggregateGroupStep, group_numReducers(gr));
  local->reducers = array_new(AggregateGroupStep, group_numReducers(gr));
  int success = 1;
  for (int i = 0; i < group_numReducers(gr); i++) {
    AggregateGroupReduce *red = &gr->reducers[i];
    if (!strcasecmp(red->reducer, "COUNT")) {
      if (!distributeCount(red, localStep, remoteStep)) {
        success = 0;
        break;
      }
    } else if (!strcasecmp(red->reducer, "AVG")) {
      if (!distributeAvg(red, localStep, remoteStep)) {
        success = 0;
        break;
      }
    } else if (!strcasecmp(red->reducer, "SUM")) {
      if (!distributeSum(red, localStep, remoteStep)) {
        success = 0;
        break;
      }
    } else if (!strcasecmp(red->reducer, "MIN")) {
      if (!distributeMin(red, localStep, remoteStep)) {
        success = 0;
        break;
      }
    } else if (!strcasecmp(red->reducer, "MAX")) {
      if (!distributeMax(red, localStep, remoteStep)) {
        success = 0;
        break;
      }
    } else {
      success = 0;
      break;
    }
  }
  *cont = 0;

  if (!success) {
    return NULL;  // TODO: free stuff here
  }

  plan_AddStep(dist, remoteStep);
  AggregateStep *tmp = step_Detach(step);
  step_AddBefore(tmp, localStep);
  return localStep->next;
}
int AggregatePlan_MakeDistributed(AggregatePlan *src, AggregatePlan *dist) {
  AggregateStep *current = src->head;
  *dist = (AggregatePlan){.index = src->index,
                          .hasCursor = 1,
                          .head = newStep(AggregateStep_Dummy),
                          .tail = newStep(AggregateStep_Dummy),

                          .cursor = (AggregateCursor){
                              .count = 500,
                          }};
  dist->tail->prev = dist->head;
  dist->head->next = dist->tail;
  // zero the stuff we don't care about in src
  src->index = NULL;

  int cont = 1;
  while (current && cont) {
    AggregateStep *tmp = current;
    switch (current->type) {
      case AggregateStep_Query:
      case AggregateStep_Apply:
      case AggregateStep_Limit:
      case AggregateStep_Load:
      case AggregateStep_Sort:

        current = plan_MoveStep(src, dist, current);
        break;

      case AggregateStep_Group:
        current = distributeGroupStep(src, dist, current, &cont);
        break;

      case AggregateStep_Distribute:
        break;
      case AggregateStep_Dummy:
        current = current->next;
        break;
    }
  }
  // If we can distribute the plan, add a marker for it in the source plan
  if (dist->size > 0) {

    // Add a load step for everything not in the distributed schema
    AggregateSchema as = AggregatePlan_GetSchema(src, NULL);
    AggregateSchema dis = AggregatePlan_GetSchema(dist, NULL);

    AggregateStep *ls = newStep(AggregateStep_Load);
    const char **arr = array_new(const char *, 4);
    for (int i = 0; i < array_len(as); i++) {
      if (as[i].kind == Property_Field && !AggregateSchema_Contains(dis, as[i].property)) {

        // const char *prop = as[i].property;
        arr = array_append(arr, as[i].property);
      }
    }
    if (array_len(arr) > 0) {
      ls->load.keys = RS_NewMultiKey(array_len(arr));
      for (int i = 0; i < array_len(arr); i++) {
        ls->load.keys->keys[i].key = arr[i];
      }
      step_AddAfter(dist->head, ls);
    }

    array_free(arr, NULL);
    array_free(as, NULL);
    array_free(dis, NULL);

    AggregateStep *ds = newStep(AggregateStep_Distribute);
    ds->dist.plan = dist;
    step_AddAfter(src->head, ds);
  }

  return 1;
}