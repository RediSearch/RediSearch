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
  return step;
}

AggregateStep *newLoadStep(CmdArg *arg, char **err) {

  RSMultiKey *k = RS_NewMultiKeyFromArgs(&CMDARG_ARR(arg), 1);
  AggregateStep *ret = newStep(AggregateStep_Load);
  ret->load.keys = k;
  return ret;
}

AggregateStep *newApplyStep(CmdArg *arg, char **err) {

  CmdArg *expr = CmdArg_FirstOf(arg, "expr");
  if (!expr || CMDARG_TYPE(expr) != CmdArg_String) {
    SET_ERR(err, "Missing or invalid projection expression");
    return NULL;
  }

  const char *exp = CMDARG_STRPTR(expr);
  RSExpr *pe = RSExpr_Parse(exp, strlen(exp), err);
  if (!pe) {
    return NULL;
  }
  AggregateStep *ret = newStep(AggregateStep_Apply);
  ret->apply.rawExpr = exp;
  ret->apply.parsedExpr = pe;
  ret->apply.alias = CMDARG_ORNULL(CmdArg_FirstOf(arg, "AS"), CMDARG_STRPTR);
  return ret;
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
  FMT_ERR(err, "Invalid sortby arguments near '%s'", CMDARG_STRPTR(CMDARG_ARRELEM(by, i)));
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

void buildReducer(AggregateGroupReduce *gr, CmdArg *red, char **err) {

  gr->reducer = CMDARG_STRPTR(CmdArg_FirstOf(red, "func"));
  CmdArg *args = CmdArg_FirstOf(red, "args");
  gr->args = NULL;
  if (CMDARG_ARRLEN(args) > 0) {
    gr->args = RS_NewValueFromCmdArg(args);
  }
  gr->alias = CMDARG_ORNULL(CmdArg_FirstOf(red, "AS"), CMDARG_STRPTR);
}

AggregateStep *newGroupStep(CmdArg *grp, char **err) {
  CmdArg *by = CmdArg_FirstOf(grp, "by");
  if (!by || CMDARG_ARRLEN(by) == 0) {
    SET_ERR(err, "No fields for GROUPBY");
    return NULL;
  }
  RSMultiKey *keys = RS_NewMultiKeyFromArgs(&CMDARG_ARR(by), 1);
  size_t numReducers = CmdArg_Count(grp, "REDUCE");
  AggregateGroupReduce *arr = NULL;
  if (numReducers) {
    arr = calloc(numReducers, sizeof(AggregateGroupReduce));
  }

  size_t n = 0;
  // Add reducers
  CMD_FOREACH_SELECT(grp, "REDUCE", {
    buildReducer(&arr[n], result, err);
    n++;
  });

  AggregateStep *ret = newStep(AggregateStep_Group);
  ret->group = (AggregateGroupStep){
      .properties = keys,
      .reducers = arr,
      .numReducers = numReducers,
      .alias = CMDARG_ORNULL(CmdArg_FirstOf(grp, "AS"), CMDARG_STRPTR),
  };

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
        for (int i = 0; i < current->group.numReducers; i++) {
          AggregateGroupReduce *red = &current->group.reducers[i];

          for (int j = 0; j < RSValue_ArrayLen(red->args); j++) {
            if (RSValue_IsString(RSValue_ArrayItem(red->args, j))) {
              const char *c = RSValue_StringPtrLen(RSValue_ArrayItem(red->args, j), NULL);
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

static void plan_AddStep(AggregatePlan *plan, AggregateStep *step) {
  if (!plan->head) {
    plan->head = plan->tail = step;
  } else {
    plan->tail->next = step;
    plan->tail = step;
  }
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
  int n = 0;
  while (NULL != (child = CmdArgIterator_Next(&it, &key))) {
    AggregateStep *next = NULL;
    if (!strcasecmp(key, "idx")) {
      plan->index = CMDARG_STRPTR(child);
      continue;
    } else if (!strcasecmp(key, "query")) {
      plan->query = CMDARG_STRPTR(child);
      plan->queryLen = CMDARG_STRLEN(child);
      continue;
    } else if (!strcasecmp(key, "GROUPBY")) {
      next = newGroupStep(child, err);
    } else if (!strcasecmp(key, "SORTBY")) {
      next = newSortStep(child, err);
    } else if (!strcasecmp(key, "APPLY")) {
      next = newApplyStep(child, err);
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
  for (int i = 0; i < g->numReducers; i++) {
    arrPushStrdup(v, "REDUCE");
    arrPushStrdup(v, g->reducers[i].reducer);
    arrPushStrfmt(v, "%d", g->reducers[i].args ? g->reducers[i].args->arrval.len : 0);
    if (g->reducers[i].args) {
      RSValue tmp = {.allocated = 0};

      for (int j = 0; j < g->reducers[i].args->arrval.len; j++) {
        RSValue_ToString(&tmp, g->reducers[i].args->arrval.vals[i]);
        arrPushStrdup(v, RSValue_Dereference(&tmp)->strval.str);
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
  if (plan->query) arrPushStrfmt(&vec, "%.*s", plan->queryLen, plan->query);

  AggregateStep *current = plan->head;
  while (current) {
    switch (current->type) {
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

      case AggregateStep_Distribute:
        arrPushStrdup(&vec, "<DISTRIBUTE-SUBPLAN>");
        break;
    }
    current = current->next;
  }

  return vec;
}

AggregateStep *plan_MoveStep(AggregatePlan *src, AggregatePlan *dist, AggregateStep *step,
                             AggregateStep *prev) {
  if (prev) {
    prev->next = step->next;
  } else {
    src->head = step->next;
  }
  AggregateStep *next = step->next;
  step->next = NULL;
  plan_AddStep(dist, step);
  return next;
}

AggregateStep *distributeGroupStep(AggregatePlan *src, AggregatePlan *dist, AggregateStep *step,
                                   AggregateStep *prev, int *cont) {
  AggregateGroupStep *gr = &step->group;
  AggregateStep *so = newStep(AggregateStep_Group);
  AggregateGroupStep *out = &so->group;
  out->alias = gr->alias;
  out->properties = gr->properties;
  out->reducers = calloc(gr->numReducers, sizeof(AggregateGroupReduce));
  out->numReducers = 0;
  for (int i = 0; i < gr->numReducers; i++) {
    AggregateGroupReduce *red = &gr->reducers[i];
    if (!strcasecmp(red->reducer, "COUNT")) {
      out->reducers[out->numReducers++] =
          (AggregateGroupReduce){.alias = red->alias, .args = NULL, .reducer = red->reducer};

      red->reducer = "SUM";
      char *arg;
      asprintf(&arg, "@%s", RSKEY(red->alias));
      red->args = RS_VStringArray(1, arg);
      *cont = 0;
    } else {
      *cont = 0;
      return NULL;
    }
  }

  plan_AddStep(dist, so);
  return step->next;
}
int AggregatePlan_MakeDistributed(AggregatePlan *src, AggregatePlan *dist) {
  AggregateStep *current = src->head;
  AggregateStep *prev = NULL;
  *dist = (AggregatePlan){.query = src->query,
                          .queryLen = src->queryLen,
                          .index = src->index,
                          .hasCursor = 1,
                          .cursor = (AggregateCursor){
                              .count = 500,
                          }};
  // zero the stuff we don't care about in src
  src->index = NULL;
  src->query = NULL;

  int cont = 1;
  while (current && cont) {
    switch (current->type) {
      case AggregateStep_Apply:
        current = plan_MoveStep(src, dist, current, prev);
        break;
      case AggregateStep_Limit:
        current = plan_MoveStep(src, dist, current, prev);
        break;
      case AggregateStep_Group:
        current = distributeGroupStep(src, dist, current, prev, &cont);
        break;
      case AggregateStep_Load:
        current = plan_MoveStep(src, dist, current, prev);
        break;
      case AggregateStep_Sort:
        current = plan_MoveStep(src, dist, current, prev);
        cont = 0;
        break;
      case AggregateStep_Distribute:
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
      ls->next = dist->head;
      dist->head = ls;
    }

    array_free(arr, NULL);
    array_free(as, NULL);
    array_free(dis, NULL);

    AggregateStep *ds = newStep(AggregateStep_Distribute);
    ds->dist.plan = dist;
    ds->next = src->head;
    src->head = ds;
  }

  return 1;
}