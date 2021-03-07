
#include "aggregate_plan.h"
#include "reducer.h"
#include "expr/expression.h"
#include "commands.h"
#include "util/arr.h"
#include <ctype.h>

///////////////////////////////////////////////////////////////////////////////////////////////

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

//---------------------------------------------------------------------------------------------

/* add a step to the plan at its end (before the dummy tail) */
void AGGPlan::AddStep(PLN_BaseStep *step) {
  RS_LOG_ASSERT(step->type > PLN_T_INVALID, "Step type connot be PLN_T_INVALID");
  dllist_append(&steps, &step->llnodePln);
  steptypes |= (1 << (step->type - 1));
}

//---------------------------------------------------------------------------------------------

bool AGGPlan::HasStep(PLN_StepType t) const {
  return !!(steptypes & (1 << (t - 1)));
}

//---------------------------------------------------------------------------------------------

void AGGPlan::AddBefore(PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  RS_LOG_ASSERT(newstp->type > PLN_T_INVALID, "Step type connot be PLN_T_INVALID");
  if (posstp == NULL || DLLIST_IS_FIRST(&steps, &posstp->llnodePln)) {
    dllist_prepend(&steps, &posstp->llnodePln);
  } else {
    dllist_insert(posstp->llnodePln.prev, &posstp->llnodePln, &newstp->llnodePln);
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::AddAfter(PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  RS_LOG_ASSERT(newstp->type > PLN_T_INVALID, "Step type connot be PLN_T_INVALID");
  if (posstp == NULL || DLLIST_IS_LAST(&steps, &posstp->llnodePln)) {
    AddStep(newstp);
  } else {
    dllist_insert(&posstp->llnodePln, posstp->llnodePln.next, &newstp->llnodePln);
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::Prepend(PLN_BaseStep *newstp) {
  dllist_prepend(&steps, &newstp->llnodePln);
}

//---------------------------------------------------------------------------------------------

void AGGPlan::PopStep(PLN_BaseStep *step) {
  dllist_delete(&step->llnodePln);
}

//---------------------------------------------------------------------------------------------

PLN_FirstStep::~PLN_FirstStep() {
  RLookup_Cleanup(&lookup);
}

//---------------------------------------------------------------------------------------------

RLookup *PLN_FirstStep::getLookup() {
  return &lookup;
}

//---------------------------------------------------------------------------------------------

AGGPlan::AGGPlan() {
  arrangement = NULL;
  steptypes = 0;

  dllist_init(&steps);
  dllist_append(&steps, &firstStep_s.llnodePln);
  firstStep_s.type = PLN_T_ROOT;
  firstStep_s.dtor = rootStepDtor;
  firstStep_s.getLookup = rootStepLookup;
}

//---------------------------------------------------------------------------------------------

static RLookup *lookupFromNode(const DLLIST_node *nn) {
  PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
  if (stp->getLookup) {
    return stp->getLookup(stp);
  } else {
    return NULL;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Locate a plan within the given constraints. begin and end are the plan ranges
 * to check. `end` is considered exclusive while `begin` is inclusive. To search
 * the entire plan, set `begin` and `end` to NULL.
 *
 * @param pln the plan to search
 * @param begin step to start searching from
 * @param end step to stop searching at
 * @param type type of plan to search for. The special PLANTYPE_ANY_REDUCER
 *  can be used for any plan type which creates a new RLookup
 */

const PLN_BaseStep *AGGPlan::FindStep(const PLN_BaseStep *begin, const PLN_BaseStep *end, 
                                      PLN_StepType type) const {
  if (!begin) {
    begin = DLLIST_ITEM(steps.next, PLN_BaseStep, llnodePln);
  }
  if (!end) {
    end = DLLIST_ITEM(&steps, PLN_BaseStep, llnodePln);
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

//---------------------------------------------------------------------------------------------

static void arrangeDtor(PLN_BaseStep *bstp) {
  PLN_ArrangeStep *astp = (PLN_ArrangeStep *)bstp;
  if (astp->sortKeys) {
    array_free(astp->sortKeys);
  }
  rm_free(astp->sortkeysLK);
  rm_free(bstp);
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Gets the last arrange step for the current pipeline stage. If no arrange step exists, return NULL.

PLN_ArrangeStep *AGGPlan::GetArrangeStep() {
  // Go backwards.. and stop at the cutoff
  for (const DLLIST_node *nn = steps.prev; nn != &steps; nn = nn->prev) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    if (PLN_IsReduce(stp)) {
      break;
    } else if (stp->type == PLN_T_ARRANGE) {
      return (PLN_ArrangeStep *)stp;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

// Gets the last arrange step for the current pipeline stage. If no arrange step exists, one is created.
// This function should be used to limit/page through the current step.

PLN_ArrangeStep *AGGPlan::GetOrCreateArrangeStep() {
  PLN_ArrangeStep *ret = GetArrangeStep();
  if (ret) {
    return ret;
  }
  ret = new PLN_ArrangeStep();
  ret->base.type = PLN_T_ARRANGE;
  ret->base.dtor = arrangeDtor;
  AddStep(&ret->base);
  return ret;
}

//---------------------------------------------------------------------------------------------

/**
 * Get the lookup provided the given mode
 * @param pln the plan containing the steps
 * @param bstp - acts as a placeholder for iteration. If mode is FIRST, then
 *  this acts as a barrier and no lookups after this step are returned. If mode
 *  is LAST, then this acts as an initializer, and steps before this (inclusive)
 *  are ignored (NYI).
 */

RLookup *AGGPlan::GetLookup(const PLN_BaseStep *bstp, AGPLNGetLookupMode mode) const {
  const DLLIST_node *first = NULL, *last = NULL;
  int isReverse = 0;

  switch (mode) {
    case AGPLN_GETLOOKUP_FIRST:
      first = steps.next;
      last = bstp ? &bstp->llnodePln : &steps;
      break;
    case AGPLN_GETLOOKUP_PREV:
      first = &steps;
      last = bstp->llnodePln.prev;
      isReverse = 1;
      break;
    case AGPLN_GETLOOKUP_NEXT:
      first = bstp->llnodePln.next;
      last = &steps;
      break;
    case AGPLN_GETLOOKUP_LAST:
      first = bstp ? &bstp->llnodePln : &steps;
      last = steps.prev;
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

//---------------------------------------------------------------------------------------------

void AGGPlan::FreeSteps() {
  DLLIST_node *nn = steps.next;
  while (nn && nn != &steps) {
    PLN_BaseStep *bstp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    nn = nn->next;
    if (bstp->dtor) {
      bstp->dtor(bstp);
    }
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::Dump() const {
  for (const DLLIST_node *nn = steps.next; nn && nn != &steps; nn = nn->next) {
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
          printf("  %s\n", (char *)lstp->args.objs[ii]);
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
            printf("%s ", (char *)r->args.objs[jj]);
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

//---------------------------------------------------------------------------------------------

typedef char **myArgArray_t;

static inline void append_string(myArgArray_t *arr, const char *src) {
  char *s = rm_strdup(src);
  *arr = array_append(*arr, s);
}

//---------------------------------------------------------------------------------------------

static inline void append_uint(myArgArray_t *arr, unsigned long long ll) {
  char s[64] = {0};
  sprintf(s, "%llu", ll);
  append_string(arr, s);
}

//---------------------------------------------------------------------------------------------

static inline void append_ac(myArgArray_t *arr, const ArgsCursor *ac) {
  for (size_t ii = 0; ii < ac->argc; ++ii) {
    append_string(arr, AC_StringArg(ac, ii));
  }
}

//---------------------------------------------------------------------------------------------

static void serializeMapFilter(myArgArray_t *arr, const PLN_BaseStep *stp) {
  const PLN_MapFilterStep *mstp = (PLN_MapFilterStep *)stp;
  if (stp->type == PLN_T_APPLY) {
    append_string(arr, "APPLY");
  } else {
    append_string(arr, "FILTER");
  }
  append_string(arr, mstp->rawExpr);
  if (stp->alias) {
    append_string(arr, "AS");
    append_string(arr, stp->alias);
  }
}

//---------------------------------------------------------------------------------------------

static void serializeArrange(myArgArray_t *arr, const PLN_BaseStep *stp) {
  const PLN_ArrangeStep *astp = (PLN_ArrangeStep *)stp;
  if (astp->limit || astp->offset) {
    append_string(arr, "LIMIT");
    append_uint(arr, astp->offset);
    append_uint(arr, astp->limit);
  }
  if (astp->sortKeys) {
    size_t numsort = array_len(astp->sortKeys);
    append_string(arr, "SORTBY");
    append_uint(arr, numsort * 2);
    for (size_t ii = 0; ii < numsort; ++ii) {
      char *stmp;
      rm_asprintf(&stmp, "@%s", astp->sortKeys[ii]);
      *arr = array_append(*arr, stmp);
      if (SORTASCMAP_GETASC(astp->sortAscMap, ii)) {
        append_string(arr, "ASC");
      } else {
        append_string(arr, "DESC");
      }
    }
  }
}

//---------------------------------------------------------------------------------------------

static void serializeLoad(myArgArray_t *arr, const PLN_BaseStep *stp) {
  PLN_LoadStep *lstp = (PLN_LoadStep *)stp;
  if (lstp->args.argc) {
    append_string(arr, "LOAD");
    append_uint(arr, lstp->args.argc);
    append_ac(arr, &lstp->args);
  }
}

//---------------------------------------------------------------------------------------------

static void serializeGroup(myArgArray_t *arr, const PLN_BaseStep *stp) {
  const PLN_GroupStep *gstp = (PLN_GroupStep *)stp;
  append_string(arr, "GROUPBY");
  append_uint(arr, gstp->nproperties);
  for (size_t ii = 0; ii < gstp->nproperties; ++ii) {
    append_string(arr, gstp->properties[ii]);
  }
  size_t nreducers = array_len(gstp->reducers);
  for (size_t ii = 0; ii < nreducers; ++ii) {
    const PLN_Reducer *r = gstp->reducers + ii;
    append_string(arr, "REDUCE");
    append_string(arr, r->name);
    append_uint(arr, r->args.argc);
    append_ac(arr, &r->args);
    if (r->alias) {
      append_string(arr, "AS");
      append_string(arr, r->alias);
    }
  }
}

//---------------------------------------------------------------------------------------------

/* Serialize the plan into an array of string args, to create a command to be sent over the network.
 * The strings need to be freed with free and the array needs to be freed with array_free().
 * The length can be extracted with array_len */

array_t AGGPlan::Serialize() const {
  char **arr = array_new(char *, 1);
  for (const DLLIST_node *nn = steps.next; nn != &steps; nn = nn->next) {
    const PLN_BaseStep *stp = DLLIST_ITEM(nn, PLN_BaseStep, llnodePln);
    switch (stp->type) {
      case PLN_T_APPLY:
      case PLN_T_FILTER:
        serializeMapFilter(&arr, stp);
        break;
      case PLN_T_ARRANGE:
        serializeArrange(&arr, stp);
        break;
      case PLN_T_LOAD:
        serializeLoad(&arr, stp);
        break;
      case PLN_T_GROUP:
        serializeGroup(&arr, stp);
        break;
      case PLN_T_INVALID:
      case PLN_T_ROOT:
      case PLN_T_DISTRIBUTE:
      case PLN_T__MAX:
        break;
    }
  }
  return arr;
}

///////////////////////////////////////////////////////////////////////////////////////////////
