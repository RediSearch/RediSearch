#include "aggregate_plan.h"
#include "reducer.h"
#include "expr/expression.h"
#include <commands.h>
#include <util/arr.h>
#include <ctype.h>

static const char *steptypeToString(PLN_StepType type) {
  switch (type) {
    case PLN_T_APPLY:
      return "APPLY";
    case PLN_T_FILTER:
      return "FILTER";
    case PLN_T_ARRANGE:
      return "LIMIT/MAX/SORTBY";
    case PLN_T_ROOT:
      return "<ROOT>";
    case PLN_T_GROUP:
      return "GROUPBY";
    case PLN_T_LOAD:
      return "LOAD";
    case PLN_T_DISTRIBUTE:
      return "DISTRIBUTE";
    case PLN_T_INVALID:
    default:
      return "<UNKNOWN>";
  }
}

/* add a step to the plan at its end (before the dummy tail) */
void AGPLN_AddStep(AGGPlan *plan, PLN_BaseStep *step) {
  printf("Adding step %p (T=%d)\n", step, step->type);
  assert(step->type > PLN_T_INVALID);
  dllist_append(&plan->steps, &step->llnodePln);
}

void AGPLN_AddBefore(AGGPlan *pln, PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  assert(newstp->type > PLN_T_INVALID);
  dllist_insert(posstp->llnodePln.prev, posstp->llnodePln.next, &newstp->llnodePln);
}

void AGPLN_Init(AGGPlan *plan) {
  memset(plan, 0, sizeof *plan);
  dllist_init(&plan->steps);
  dllist_append(&plan->steps, &plan->firstStep_s.base.llnodePln);
  plan->firstStep_s.base.type = PLN_T_ROOT;
}

static RLookup *lookupFromNode(const DLLIST_node *nn) {
  const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
  assert(stp->type != PLN_T_INVALID);
  if (stp->type == PLN_T_ROOT) {
    return &((PLN_FirstStep *)stp)->lookup;
  } else if (stp->type == PLN_T_GROUP) {
    return &((PLN_GroupStep *)stp)->lookup;
  } else {
    return NULL;
  }
}

PLN_ArrangeStep *AGPLN_GetArrangeStep(AGGPlan *pln) {
  // Go backwards.. and stop at the cutoff
  for (const DLLIST_node *nn = pln->steps.prev; nn != &pln->steps; nn = nn->prev) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    if (PLN_IsReduce(stp)) {
      break;
    } else if (stp->type == PLN_T_ARRANGE) {
      return (PLN_ArrangeStep *)stp;
    }
  }
  // If we are still here, then an arrange step does not exist. Create one!
  PLN_ArrangeStep *ret = calloc(1, sizeof(*ret));
  ret->base.type = PLN_T_ARRANGE;
  AGPLN_AddStep(pln, &ret->base);
  return ret;
}

RLookup *AGPLN_GetLookup(const AGGPlan *pln, const PLN_BaseStep *bstp, AGPLNGetLookupMode mode) {
  const DLLIST_node *first = NULL, *last = NULL;
  int isReverse = 0;

  switch (mode) {
    case AGPLN_GETLOOKUP_FIRST:
      first = pln->steps.next;
      last = bstp ? &bstp->llnodePln : &pln->steps;
      break;
    case AGPLN_GETLOOKUP_PREV:
      first = &pln->steps;
      last = bstp->llnodePln.prev;
      isReverse = 1;
      break;
    case AGPLN_GETLOOKUP_NEXT:
      first = bstp->llnodePln.next;
      last = &pln->steps;
      break;
    case AGPLN_GETLOOKUP_LAST:
      first = bstp ? &bstp->llnodePln : &pln->steps;
      last = pln->steps.prev;
      isReverse = 1;
  }

  if (isReverse) {
    for (const DLLIST_node *nn = last; nn && nn != first; nn = nn->prev) {
      RLookup *lk = lookupFromNode(nn);
      if (lk) {
        return lk;
      }
    }
  } else {
    for (const DLLIST_node *nn = first; nn && nn != last; nn = nn->next) {
      RLookup *lk = lookupFromNode(nn);
      if (lk) {
        return lk;
      }
    }
    return NULL;
  }
  return NULL;
}

void AGPLN_Dump(const AGGPlan *pln) {
  for (const DLLIST_node *nn = pln->steps.next; nn && nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    printf("STEP: [T=%s. P=%p]\n", steptypeToString(stp->type), stp);
    RLookup *lk = lookupFromNode(nn);
    if (lk) {
      printf("  NEW LOOKUP: %p\n", lk);
      for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
        printf("    %s @%p: FLAGS=0x%x\n", kk->name, kk, kk->flags);
      }
    }
  }
}

#if 0
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
  for (int i = 0; i < array_len(g->reducers); i++) {
    arrPushStrdup(v, "REDUCE");
    arrPushStrdup(v, g->reducers[i].name);
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
      case AggregateStep_Query:
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

void AggregatePlan_Free(AggregatePlan *plan) {
  AggregateStep *current = plan->head;
  while (current) {
    AggregateStep *next = current->next;
    // FIXME: Actually free!
    free(current);
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
#endif