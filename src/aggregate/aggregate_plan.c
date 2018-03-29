#include "aggregate.h"

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
  AggregateStep *ret = newStep(AggregateStep_Apply);
  ret->apply.expr = CMDARG_STRPTR(expr);
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
    if (n++ < 2) continue;

    AggregateStep *next = NULL;
    if (!strcasecmp(key, "GROUPBY")) {
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
    if (!plan->head) {
      plan->head = plan->tail = next;
    } else {
      plan->tail->next = next;
      plan->tail = next;
    }
    plan->size++;
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

void vecPushStrdup(Vector *v, const char *s) {
  char *x = strdup(s);
  printf("%s\n", x);
  Vector_Push(v, x);
}

void vecPushStrfmt(Vector *v, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  char *c;
  vasprintf(&c, fmt, ap);
  va_end(ap);
  Vector_Push(v, c);
}

void serializeGroup(AggregateGroupStep *g, Vector *v) {
  vecPushStrdup(v, "GROUPBY");
  vecPushStrfmt(v, "%d", g->properties->len);

  for (int i = 0; i < g->properties->len; i++) {
    vecPushStrfmt(v, "@%s", g->properties->keys[i].key);
  }
  for (int i = 0; i < g->numReducers; i++) {
    vecPushStrdup(v, "REDUCE");
    vecPushStrdup(v, g->reducers[i].reducer);
    vecPushStrfmt(v, "%d", g->reducers[i].args ? g->reducers[i].args->arrval.len : 0);
    if (g->reducers[i].args) {
      RSValue tmp = {.allocated = 0};
      for (int j = 0; j < g->reducers[i].args->arrval.len; j++) {
        RSValue_ToString(&tmp, g->reducers[i].args->arrval.vals[i]);
        vecPushStrdup(v, tmp.strval.str);
        RSValue_Free(&tmp);
      }
    }
    if (g->reducers[i].alias) {
      vecPushStrdup(v, "AS");
      vecPushStrdup(v, g->reducers[i].alias);
    }
  }
}

void serializeSort(AggregateSortStep *s, Vector *v) {
  vecPushStrdup(v, "SORTBY");
  vecPushStrfmt(v, "%d", s->keys->len * 2);
  for (int i = 0; i < s->keys->len; i++) {
    vecPushStrfmt(v, "@%s", s->keys->keys[i].key);
    vecPushStrdup(v, s->ascMap & (1 << i) ? "ASC" : "DESC");
  }
}

void serializeApply(AggregateApplyStep *a, Vector *v) {
  vecPushStrdup(v, "APPLY");
  vecPushStrdup(v, a->expr);
  vecPushStrdup(v, "AS");
  vecPushStrdup(v, a->alias);
}

void serializeLimit(AggregateLimitStep *l, Vector *v) {
  vecPushStrdup(v, "LIMIT");
  vecPushStrfmt(v, "%lld", l->offset);
  vecPushStrfmt(v, "%lld", l->num);
}

void serializeLoad(AggregateLoadStep *l, Vector *v) {
  vecPushStrdup(v, "LOAD");
  vecPushStrfmt(v, "%d", l->keys->len);
  for (int i = 0; i < l->keys->len; i++) {
    vecPushStrfmt(v, "@%s", l->keys->keys[i].key);
  }
}

Vector *AggregatePlan_Serialize(AggregatePlan *plan) {
  Vector *vec = NewVector(const char *, 10);
  // vecPushStrdup(vec, plan->index);
  // vecPushStrfmt(vec, "%.*s", plan->queryLen, plan->query);

  AggregateStep *current = plan->head;
  while (current) {
    switch (current->type) {
      case AggregateStep_Group:
        serializeGroup(&current->group, vec);
        break;
      case AggregateStep_Sort:
        serializeSort(&current->sort, vec);
        break;

      case AggregateStep_Apply:
        serializeApply(&current->apply, vec);
        break;

      case AggregateStep_Limit:
        serializeLimit(&current->limit, vec);
        break;

      case AggregateStep_Load:
        serializeLoad(&current->load, vec);
        break;
    }
    current = current->next;
  }

  return vec;
}
