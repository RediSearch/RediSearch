#include "aggregate_plan.h"
#include "reducer.h"
#include "expr/expression.h"
#include <commands.h>
#include <util/arr.h>
#include <ctype.h>
#include <err.h>

AggregateStep *AggregatePlan_NewStep(AggregateStepType t) {
  AggregateStep *step = malloc(sizeof(*step));
  step->type = t;
  step->next = NULL;
  step->prev = NULL;
  return step;
}

AggregateStep *newLoadStep(CmdArg *arg, char **err) {

  RSMultiKey *k = RS_NewMultiKeyFromArgs(&CMDARG_ARR(arg), 1, 1);
  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Load);
  ret->load.keys = k;
  // we do not immediately create the field list, it might not be needed
  ret->load.fl = (FieldList){};
  return ret;
}

AggregateStep *AggregatePlan_NewApplyStep(const char *alias, const char *expr, char **err) {
  RSExpr *pe = RSExpr_Parse(expr, strlen(expr), err);
  if (!pe) {
    return NULL;
  }
  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Apply);
  ret->apply = (AggregateApplyStep){
      .rawExpr = (char *)expr,
      .parsedExpr = pe,
      .alias = alias ? strdup(alias) : NULL,
  };
  return ret;
}

AggregateStep *AggregatePlan_NewFilterStep(const char *expr, char **err) {
  RSExpr *pe = RSExpr_Parse(expr, strlen(expr), err);
  if (!pe) {
    return NULL;
  }
  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Filter);
  ret->filter = (AggregateFilterStep){
      .rawExpr = (char *)expr,
      .parsedExpr = pe,
  };
  return ret;
}

AggregateStep *AggregatePlan_NewApplyStepFmt(const char *alias, char **err, const char *fmt, ...) {
  char *exp;
  va_list ap;
  va_start(ap, fmt);
  vasprintf(&exp, fmt, ap);
  va_end(ap);
  AggregateStep *st = AggregatePlan_NewApplyStep(alias, exp, err);
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

  const char *exp = strdup(CMDARG_STRPTR(expr));
  const char *alias = CMDARG_ORNULL(CmdArg_FirstOf(arg, "AS"), CMDARG_STRPTR);
  return AggregatePlan_NewApplyStep(alias ? strdup(alias) : NULL, exp, err);
}

AggregateStep *newFilterStep(CmdArg *arg, char **err) {

  if (CMDARG_TYPE(arg) != CmdArg_String) {
    SET_ERR(err, "Missing or invalid filter expression");
    return NULL;
  }

  const char *exp = strdup(CMDARG_STRPTR(arg));

  return AggregatePlan_NewFilterStep(exp, err);
}

AggregateStep *newSortStep(CmdArg *srt, char **err) {
  CmdArg *by = CmdArg_FirstOf(srt, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) return NULL;

  RSMultiKey *keys = RS_NewMultiKey(CMDARG_ARRLEN(by));
  keys->keysAllocated = 1;
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
      keys->keys[n++] = RS_KEY_STRDUP(RSKEY(str));
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
      // Unknown token - neither a property nor ASC/DESC
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

  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Sort);
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

  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Limit);
  ret->limit.offset = offset;
  ret->limit.num = limit;
  return ret;
}

size_t AggregateGroupStep_NumReducers(AggregateGroupStep *g) {
  return array_len(g->reducers);
}

char *AggregatePlan_GetReducerAlias(AggregateGroupStep *g, const char *func, RSValue **argv,
                                    int argc) {

  sds out = sdsnew("__generated_alias");
  out = sdscat(out, func);
  // only put parentheses if we actually have args
  char buf[255];

  for (size_t i = 0; i < argc; i++) {
    size_t l;
    const char *s = RSValue_ConvertStringPtrLen(argv[i], &l, buf, sizeof(buf));

    while (*s == '@') {
      // Don't allow the leading '@' to be included as an alias!
      ++s;
      --l;
    }
    out = sdscatlen(out, s, l);
    if (i + 1 < argc) {
      out = sdscat(out, ",");
    }
  }

  // only put parentheses if we actually have args
  sdstolower(out);

  // duplicate everything. yeah this is lame but this function is not in a tight loop
  char *dup = strndup(out, sdslen(out));
  sdsfree(out);
  return dup;
}

char *AggregateGroupStep_AddReducer(AggregateGroupStep *g, const char *func, char *alias, int argc,
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
    alias = AggregatePlan_GetReducerAlias(g, func, arr, argc);
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
    gr->alias =
        AggregatePlan_GetReducerAlias(g, gr->reducer, gr->args, gr->args ? array_len(gr->args) : 0);
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
  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by), 1, 1);
  size_t numReducers = CmdArg_Count(grp, "REDUCE");
  AggregateGroupReduce *arr = NULL;
  if (numReducers) {
    arr = array_new(AggregateGroupReduce, numReducers);
  }

  AggregateStep *ret = AggregatePlan_NewStep(AggregateStep_Group);
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
  schema = array_append(schema, ((AggregateProperty){RSKEY(property), t, kind}));
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

AggregateProperty *AggregateSchema_Get(AggregateSchema sc, const char *prop) {
  if (!sc || !prop) return NULL;
  for (size_t i = 0; i < array_len(sc); i++) {

    if (!strcasecmp(RSKEY(sc[i].property), RSKEY(prop))) {
      return &sc[i];
    }
  }
  return NULL;
}

AggregateSchema extractExprTypes(RSExpr *expr, AggregateSchema arr, RSValueType typeHint,
                                 RSSortingTable *tbl) {
  switch (expr->t) {
    case RSExpr_Function: {
      RSValueType funcType = GetExprType(expr, tbl);
      for (int i = 0; i < expr->func.args->len; i++) {
        arr = extractExprTypes(expr->func.args->args[i], arr, funcType, tbl);
      }
      break;
    }
    case RSExpr_Property: {

      arr = AggregateSchema_Set(arr, expr->property.key,
                                SortingTable_GetFieldType(tbl, expr->property.key, typeHint),
                                Property_Field, 0);
      break;
    }
    case RSExpr_Op:
      // ops are between numeric properties, so the hint is number
      arr = extractExprTypes(expr->op.left, arr, RSValue_Number, tbl);
      arr = extractExprTypes(expr->op.right, arr, RSValue_Number, tbl);
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
        // for literals we just add their type
        if (current->apply.parsedExpr && current->apply.parsedExpr->t == RSExpr_Literal) {
          arr = AggregateSchema_Set(arr, current->apply.alias, current->apply.parsedExpr->literal.t,
                                    Property_Projection, 1);
        } else {  // descend into the compound expression, and try to find out the type of the
                  // sub-expressions
          arr = extractExprTypes(current->apply.parsedExpr, arr, RSValue_String, tbl);
          arr = AggregateSchema_Set(arr, current->apply.alias,
                                    GetExprType(current->apply.parsedExpr, tbl),
                                    Property_Projection, 1);
        }
        break;

      case AggregateStep_Load:
        for (int i = 0; i < current->load.keys->len; i++) {
          arr = AggregateSchema_Set(
              arr, current->load.keys->keys[i].key,
              SortingTable_GetFieldType(tbl, current->load.keys->keys[i].key, RSValue_String),
              Property_Field, 1);
        }
        break;
      case AggregateStep_Sort:
        for (int i = 0; i < current->sort.keys->len; i++) {
          // for sort we may need the keys but they are either the output of apply/reduce, or
          // properties
          arr = AggregateSchema_Set(
              arr, current->sort.keys->keys[i].key,
              SortingTable_GetFieldType(tbl, current->load.keys->keys[i].key, RSValue_String),
              Property_Field, 0);
        }
        break;
      case AggregateStep_Group:
        for (int i = 0; i < current->group.properties->len; i++) {
          arr =
              AggregateSchema_Set(arr, current->group.properties->keys[i].key,
                                  SortingTable_GetFieldType(
                                      tbl, current->group.properties->keys[i].key, RSValue_String),
                                  Property_Field, 0);
        }
        // Now go over the reducers as well
        for (int i = 0; i < array_len(current->group.reducers); i++) {
          AggregateGroupReduce *red = &current->group.reducers[i];
          // descend to each reducer's arguments
          for (int j = 0; j < array_len(red->args); j++) {
            if (RSValue_IsString(red->args[j])) {
              // if the reducer's arg is a property, optionally add that
              const char *c = RSValue_StringPtrLen(red->args[j], NULL);
              if (c && *c == '@') {
                AggregateSchema_Set(arr, c, SortingTable_GetFieldType(tbl, c, RSValue_String),
                                    Property_Field, 0);
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

/* Add a step after a step */
void AggregateStep_AddAfter(AggregateStep *step, AggregateStep *add) {
  add->next = step->next;
  if (step->next) step->next->prev = add;
  add->prev = step;
  step->next = add;
}

/* Add a step before a step */
void AggregateStep_AddBefore(AggregateStep *step, AggregateStep *add) {
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
AggregateStep *AggregateStep_Detach(AggregateStep *step) {
  AggregateStep *tmp = step->next;
  if (step->next) step->next->prev = step->prev;
  if (step->prev) step->prev->next = step->next;
  step->prev = NULL;
  step->next = NULL;
  return tmp;
}

/* Get the first step after start of type t */
AggregateStep *AggregateStep_FirstOf(AggregateStep *start, AggregateStepType t) {
  while (start) {
    if (start->type == t) return start;
    start = start->next;
  }
  return NULL;
}

/* add a step to the plan at its end (before the dummy tail) */
void AggregatePlan_AddStep(AggregatePlan *plan, AggregateStep *step) {
  // We assume head and tail are sentinels
  AggregateStep_AddBefore(plan->tail, step);
}

void AggregatePlan_Init(AggregatePlan *plan) {
  *plan = (AggregatePlan){0};
  plan->head = AggregatePlan_NewStep(AggregateStep_Dummy);
  plan->tail = AggregatePlan_NewStep(AggregateStep_Dummy);
  plan->tail->prev = plan->head;
  plan->head->next = plan->tail;
}

void plan_setCursor(AggregatePlan *plan, CmdArg *arg) {

  CmdArg *tmoarg = CmdArg_FirstOf(arg, "MAXIDLE");
  CmdArg *countarg = CmdArg_FirstOf(arg, "COUNT");
  uint32_t timeout = tmoarg ? CMDARG_INT(tmoarg) : RSGlobalConfig.cursorMaxIdle;
  if (timeout > RSGlobalConfig.cursorMaxIdle) {
    timeout = RSGlobalConfig.cursorMaxIdle;
  }
  plan->cursor.count = countarg ? CMDARG_INT(countarg) : 0;

  plan->hasCursor = 1;
  plan->cursor.maxIdle = timeout;
}

int AggregatePlan_Build(AggregatePlan *plan, CmdArg *cmd, char **err) {
#define LOAD_NO_ALLOW_ERROR "LOAD can not come after GROUPBY/SORTBY/APPLY/LIMIT/FILTER"
  AggregatePlan_Init(plan);
  if (!cmd || CMDARG_TYPE(cmd) != CmdArg_Object || CMDARG_OBJLEN(cmd) < 3) {
    goto fail;
  }
  CmdArgIterator it = CmdArg_Children(cmd);
  CmdArg *child;
  const char *key;
  int n = 1;

  plan->withSchema = CmdArg_GetFlag(cmd, "WITHSCHEMA");
  plan->verbatim = CmdArg_GetFlag(cmd, "VERBATIM");
  bool isLoadAllow = true;

  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    AggregateStep *next = NULL;
    if (!strcasecmp(key, "idx")) {
      plan->index = CMDARG_STRPTR(child);
      continue;

    } else if (!strcasecmp(key, "WITHSCHEMA") || !strcasecmp(key, "VERBATIM")) {
      // skip verbatim and withschema
      continue;
    } else if (!strcasecmp(key, "query")) {
      next = AggregatePlan_NewStep(AggregateStep_Query);
      next->query.str = strdup(CMDARG_STRPTR(child));
    } else if (!strcasecmp(key, "GROUPBY")) {
      next = newGroupStep(n++, child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "SORTBY")) {
      next = newSortStep(child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "APPLY")) {
      next = newApplyStepArgs(child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "LIMIT")) {
      next = newLimit(child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "LOAD")) {
      if (!isLoadAllow) {
        *err = strdup(LOAD_NO_ALLOW_ERROR);
        goto fail;
      }
      next = newLoadStep(child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "FILTER")) {
      next = newFilterStep(child, err);
      isLoadAllow = false;
    } else if (!strcasecmp(key, "WITHCURSOR")) {
      plan_setCursor(plan, child);
      continue;
    }
    if (!next) {
      goto fail;
    }
    AggregatePlan_AddStep(plan, next);
  }

  return 1;

fail:
  AggregatePlan_Free(plan);
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
  for (int i = 0; i < AggregateGroupStep_NumReducers(g); i++) {
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

void serializeFilter(AggregateFilterStep *f, char ***v) {
  arrPushStrdup(v, "FILTER");
  arrPushStrdup(v, f->rawExpr);
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

void plan_serializeCursor(AggregatePlan *plan, char ***vec) {
  arrPushStrdup(vec, "WITHCURSOR");
  arrPushStrdup(vec, "COUNT");
  arrPushStrfmt(vec, "%d", plan->cursor.count);
  if (plan->cursor.maxIdle > 0) {
    arrPushStrdup(vec, "MAXIDLE");
    arrPushStrfmt(vec, "%d", plan->cursor.maxIdle);
  }
}

char **AggregatePlan_Serialize(AggregatePlan *plan) {
  char **vec = array_new(char *, 10);
  arrPushStrdup(&vec, RS_AGGREGATE_CMD);

  if (plan->index) arrPushStrdup(&vec, plan->index);

  // Serialize the cursor if needed

  AggregateStep *current = plan->head;
  while (current) {
    switch (current->type) {
      case AggregateStep_Query:
        arrPushStrdup(&vec, current->query.str);

        if (plan->verbatim) arrPushStrdup(&vec, "VERBATIM");
        if (plan->withSchema) arrPushStrdup(&vec, "WITHSCHEMA");

        if (plan->hasCursor) {
          plan_serializeCursor(plan, &vec);
        }

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

      case AggregateStep_Filter:
        serializeFilter(&current->filter, &vec);
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
        array_free(sub);
        break;
      }

      case AggregateStep_Dummy:
        break;
    }
    current = current->next;
  }

  return vec;
}

AggregateStep *AggregatePlan_MoveStep(AggregatePlan *src, AggregatePlan *dist,
                                      AggregateStep *step) {
  AggregateStep *next = AggregateStep_Detach(step);

  AggregatePlan_AddStep(dist, step);
  return next;
}

void AggregatePlan_FPrint(AggregatePlan *plan, FILE *out) {
  char **args = AggregatePlan_Serialize(plan);
  for (int i = 0; i < array_len(args); i++) {

    sds s = sdscatrepr(sdsnew(""), args[i], strlen(args[i]));
    fputs(s, out);
    fputc(' ', out);
    sdsfree(s);
  }
  array_free_ex(args, free(*(void **)ptr););
  fputs("\n", out);
}

void AggregatePlan_Print(AggregatePlan *plan) {
  AggregatePlan_FPrint(plan, stderr);
}

void value_pFree(void *p) {
  RSValue_Free(p);
}

void reducer_Free(void *p) {

  AggregateGroupReduce *gr = p;
  // the reducer func itself is const char and should not be freed
  free(gr->alias);
  array_free_ex(gr->args, RSValue_Free(*(void **)ptr));
}

void AggregateStep_Free(AggregateStep *s) {
  switch (s->type) {
    case AggregateStep_Query:
      free(s->query.str);
      break;
    case AggregateStep_Group:
      RSMultiKey_Free(s->group.properties);
      array_free_ex(s->group.reducers, reducer_Free(ptr));
      break;
    case AggregateStep_Sort:
      RSMultiKey_Free(s->sort.keys);
      break;
    case AggregateStep_Apply:
      free(s->apply.alias);
      free(s->apply.rawExpr);
      if (s->apply.parsedExpr) RSExpr_Free(s->apply.parsedExpr);
      break;
    case AggregateStep_Filter:
      free(s->filter.rawExpr);
      if (s->filter.parsedExpr) RSExpr_Free(s->apply.parsedExpr);
      break;
    case AggregateStep_Load:
      RSMultiKey_Free(s->load.keys);
      if (s->load.fl.numFields) {
        FieldList_Free(&s->load.fl);
      }
      break;
    case AggregateStep_Distribute:
      AggregatePlan_Free(s->dist.plan);
      free(s->dist.plan);
      break;

    case AggregateStep_Limit:
    case AggregateStep_Dummy:
      break;
  }
  free(s);
}

void AggregatePlan_Free(AggregatePlan *plan) {
  AggregateStep *current = plan->head;
  while (current) {
    AggregateStep *next = current->next;
    AggregateStep_Free(current);
    current = next;
  }
  plan->head = plan->tail = NULL;
}

int AggregatePlan_DumpSchema(RedisModuleCtx *ctx, QueryProcessingCtx *qpc, void *privdata) {
  AggregateSchema sc = privdata;
  if (!ctx || !sc) return 0;
  RedisModule_ReplyWithArray(ctx, array_len(sc));
  for (size_t i = 0; i < array_len(sc); i++) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithStringBuffer(ctx, sc[i].property, strlen(sc[i].property));
    const char *t = RSValue_TypeName(sc[i].type);
    RedisModule_ReplyWithStringBuffer(ctx, t, strlen(t));
  }
  return 1;
}
