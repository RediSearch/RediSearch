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
  assert(step->type > PLN_T_INVALID);
  dllist_append(&plan->steps, &step->llnodePln);
  plan->steptypes |= (1 << (step->type - 1));
}

int AGPLN_HasStep(const AGGPlan *pln, PLN_StepType t) {
  return (pln->steptypes & (1 << (t - 1)));
}

void AGPLN_AddBefore(AGGPlan *pln, PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  assert(newstp->type > PLN_T_INVALID);
  if (posstp == NULL || DLLIST_IS_FIRST(&pln->steps, &posstp->llnodePln)) {
    dllist_prepend(&pln->steps, &posstp->llnodePln);
  } else {
    dllist_insert(posstp->llnodePln.prev, &posstp->llnodePln, &newstp->llnodePln);
  }
}

void AGPLN_AddAfter(AGGPlan *pln, PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  assert(newstp->type > PLN_T_INVALID);
  if (posstp == NULL || DLLIST_IS_LAST(&pln->steps, &posstp->llnodePln)) {
    AGPLN_AddStep(pln, newstp);
  } else {
    dllist_insert(&posstp->llnodePln, posstp->llnodePln.next, &newstp->llnodePln);
  }
}

void AGPLN_Prepend(AGGPlan *pln, PLN_BaseStep *newstp) {
  dllist_prepend(&pln->steps, &newstp->llnodePln);
}

void AGPLN_PopStep(AGGPlan *pln, PLN_BaseStep *step) {
  dllist_delete(&step->llnodePln);
  (void)pln;
}

static void rootStepDtor(PLN_BaseStep *bstp) {
  PLN_FirstStep *fstp = (PLN_FirstStep *)bstp;
  RLookup_Cleanup(&fstp->lookup);
}

void AGPLN_Init(AGGPlan *plan) {
  memset(plan, 0, sizeof *plan);
  dllist_init(&plan->steps);
  dllist_append(&plan->steps, &plan->firstStep_s.base.llnodePln);
  plan->firstStep_s.base.type = PLN_T_ROOT;
  plan->firstStep_s.base.dtor = rootStepDtor;
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

const PLN_BaseStep *AGPLN_FindStep(const AGGPlan *pln, const PLN_BaseStep *begin,
                                   const PLN_BaseStep *end, PLN_StepType type) {
  if (!begin) {
    begin = DLLIST_ITEM(pln->steps.next, PLN_BaseStep, llnodePln);
  }
  if (!end) {
    end = DLLIST_ITEM(&pln->steps, PLN_BaseStep, llnodePln);
  }
  for (const PLN_BaseStep *bstp = begin; bstp != end;
       bstp = DLLIST_ITEM(bstp->llnodePln.next, PLN_BaseStep, llnodePln)) {
    if (bstp->type == type) {
      return bstp;
    }
    if (type == PLANTYPE_ANY_REDUCER && PLN_IsReduce(bstp)) {
      return bstp;
    }
  }
  return NULL;
}

static void arrangeDtor(PLN_BaseStep *bstp) {
  PLN_ArrangeStep *astp = (PLN_ArrangeStep *)bstp;
  if (astp->sortKeys) {
    array_free(astp->sortKeys);
  }
  free(bstp);
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
  ret->base.dtor = arrangeDtor;
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

void AGPLN_FreeSteps(AGGPlan *pln) {
  DLLIST_node *nn = pln->steps.next;
  while (nn && nn != &pln->steps) {
    PLN_BaseStep *bstp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    nn = nn->next;
    if (bstp->dtor) {
      bstp->dtor(bstp);
    }
  }
}

void AGPLN_Dump(const AGGPlan *pln) {
  for (const DLLIST_node *nn = pln->steps.next; nn && nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    printf("STEP: [T=%s. P=%p]\n", steptypeToString(stp->type), stp);
    const RLookup *lk = lookupFromNode(nn);
    if (lk) {
      printf("  NEW LOOKUP: %p\n", lk);
      for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
        printf("    %s @%p: FLAGS=0x%x\n", kk->name, kk, kk->flags);
      }
    }

    switch (stp->type) {
      case PLN_T_APPLY:
      case PLN_T_FILTER:
        printf("  EXPR:%s\n", ((PLN_MapFilterStep *)stp)->rawExpr);
        if (stp->alias) {
          printf("  AS:%s\n", stp->alias);
  }
        break;
      case PLN_T_ARRANGE: {
        const PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;
        if (astp->offset || astp->limit) {
          printf("  OFFSET:%lu LIMIT:%lu\n", (unsigned long)astp->offset,
                 (unsigned long)astp->limit);
        }
        if (astp->sortKeys) {
          printf("  SORT:\n");
          for (size_t ii = 0; ii < array_len(astp->sortKeys); ++ii) {
            const char *dir = SORTASCMAP_GETASC(astp->sortAscMap, ii) ? "ASC" : "DESC";
            printf("    %s:%s\n", astp->sortKeys[ii], dir);
          }
        }
        break;
      }
      case PLN_T_LOAD: {
        const PLN_LoadStep *lstp = (PLN_LoadStep *)stp;
        for (size_t ii = 0; ii < lstp->args.argc; ++ii) {
          printf("  %s\n", lstp->args.objs[ii]);
        }
        break;
      }
      case PLN_T_GROUP: {
        const PLN_GroupStep *gstp = (PLN_GroupStep *)stp;
        printf("  BY:\n");
        for (size_t ii = 0; ii < gstp->nproperties; ++ii) {
          printf("    %s\n", gstp->properties[ii]);
        }
        for (size_t ii = 0; ii < array_len(gstp->reducers); ++ii) {
          const PLN_Reducer *r = gstp->reducers + ii;
          printf("  REDUCE: %s AS %s\n", r->name, r->alias);
          if (r->args.argc) {
            printf("    ARGS:[");
          }
          for (size_t jj = 0; jj < r->args.argc; ++jj) {
            printf("%s ", r->args.objs[jj]);
          }
          printf("]\n");
        }
        break;
      }
      case PLN_T_ROOT:
      case PLN_T_DISTRIBUTE:
      case PLN_T_INVALID:
      case PLN_T__MAX:
        break;
    }
  }
}

array_t AGPLN_Serialize(const AGGPlan *pln) {
#define APPEND_STR(s)             \
  {                               \
    char *s__ = strdup(s);        \
    arr = array_append(arr, s__); \
  }
#define APPEND_UINT(n)            \
  {                               \
    unsigned long long ull__ = n; \
    char s__0[64];                \
    sprintf(s__0, "%llu", ull__); \
    APPEND_STR(s__0);             \
  }
#define APPEND_AC(ac)                                \
  for (size_t ii__ = 0; ii__ < (ac)->argc; ++ii__) { \
    APPEND_STR((const char *)(ac)->objs[ii__]);      \
  }

  char **arr = array_new(char *, 1);
  for (const DLLIST_node *nn = pln->steps.next; nn != &pln->steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    switch (stp->type) {
      case PLN_T_APPLY:
      case PLN_T_FILTER: {
        PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)stp;
        if (stp->type == PLN_T_APPLY) {
          APPEND_STR("APPLY");
        } else {
          APPEND_STR("FILTER");
        }
        APPEND_STR(mstp->rawExpr);
        if (stp->alias) {
          APPEND_STR("AS");
          APPEND_STR(stp->alias);
        }
      } break;
      case PLN_T_ARRANGE: {
        PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;
        if (astp->limit || astp->offset) {
          APPEND_STR("LIMIT");
          APPEND_UINT(astp->offset);
          APPEND_UINT(astp->limit);
        }
        if (astp->sortKeys) {
          size_t numsort = array_len(astp->sortKeys);
          APPEND_STR("SORTBY");
          APPEND_UINT(numsort * 2);
          for (size_t ii = 0; ii < numsort; ++ii) {
            char *stmp;
            asprintf(&stmp, "@%s", astp->sortKeys[ii]);
            arr = array_append(arr, stmp);
            if (SORTASCMAP_GETASC(astp->sortAscMap, ii)) {
              APPEND_STR("ASC");
            } else {
              APPEND_STR("DESC");
            }
          }
        }
      } break;
      case PLN_T_LOAD: {
        PLN_LoadStep *lstp = (PLN_LoadStep *)stp;
        if (lstp->args.argc) {
          APPEND_STR("LOAD");
          APPEND_UINT(lstp->args.argc);
          APPEND_AC(&lstp->args);
        }
        break;
      }
      case PLN_T_GROUP: {
        PLN_GroupStep *gstp = (PLN_GroupStep *)stp;
        APPEND_STR("GROUPBY");
        APPEND_UINT(gstp->nproperties);
        for (size_t ii = 0; ii < gstp->nproperties; ++ii) {
          APPEND_STR(gstp->properties[ii]);
        }
        size_t nreducers = array_len(gstp->reducers);
        for (size_t ii = 0; ii < nreducers; ++ii) {
          const PLN_Reducer *r = gstp->reducers + ii;
          APPEND_STR("REDUCE");
          APPEND_STR(r->name);
          APPEND_UINT(r->args.argc);
          APPEND_AC(&r->args);
          if (r->alias) {
            APPEND_STR("AS");
            APPEND_STR(r->alias);
          }
        }
      }
      case PLN_T_INVALID:
      case PLN_T_ROOT:
      case PLN_T_DISTRIBUTE:
      case PLN_T__MAX:
        break;
    }
  }
  return arr;
}