
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
  steps.append(step);
  steptypes |= (1 << (step->type - 1));
}

//---------------------------------------------------------------------------------------------

bool AGGPlan::HasStep(PLN_StepType t) const {
  return !!(steptypes & (1 << (t - 1)));
}

//---------------------------------------------------------------------------------------------

void AGGPlan::AddBefore(PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  RS_LOG_ASSERT(newstp->type > PLN_T_INVALID, "Step type connot be PLN_T_INVALID");
  if (posstp == NULL || steps.first() == posstp) {
    steps.prepend(posstp);
  } else {
    steps.prepend(posstp, newstp);
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::AddAfter(PLN_BaseStep *posstp, PLN_BaseStep *newstp) {
  RS_LOG_ASSERT(newstp->type > PLN_T_INVALID, "Step type connot be PLN_T_INVALID");
  if (posstp == NULL || steps.last() == posstp) {
    AddStep(newstp);
  } else {
    steps.append(posstp, newstp);
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::Prepend(PLN_BaseStep *newstp) {
  steps.prepend(newstp);
}

//---------------------------------------------------------------------------------------------

void AGGPlan::PopStep(PLN_BaseStep *step) {
  steps.remove(step);
}

//---------------------------------------------------------------------------------------------

PLN_FirstStep::~PLN_FirstStep() {
}

//---------------------------------------------------------------------------------------------

RLookup *PLN_FirstStep::getLookup() {
  return lookup.get();
}

//---------------------------------------------------------------------------------------------

AGGPlan::AGGPlan() {
  arrangement = NULL;
  steptypes = 0;
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
    //begin = DLLIST_ITEM(steps.next, PLN_BaseStep, llnodePln);
    begin = steps.first();
  }
  /*if (!end) {
    end = DLLIST_ITEM(&steps, PLN_BaseStep, llnodePln);
  }*/
  //for (const PLN_BaseStep *bstp = begin; bstp != end;
  //     bstp = DLLIST_ITEM(bstp->llnodePln.next, PLN_BaseStep, llnodePln)) {
  for (const PLN_BaseStep *bstp = begin; bstp != end; bstp = bstp->list_node.next) {
    if (bstp->type == type) {
      return bstp;
    }
    if (type == PLANTYPE_ANY_REDUCER && bstp->IsReduce()) {
      return bstp;
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

PLN_ArrangeStep::~PLN_ArrangeStep() {
  if (sortKeys) {
    array_free(sortKeys);
  }
  rm_free(sortkeysLK);
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Gets the last arrange step for the current pipeline stage. If no arrange step exists, return NULL.

PLN_ArrangeStep *AGGPlan::GetArrangeStep() {
  // Go backwards.. and stop at the cutoff
  //for (const DLLIST_node *nn = steps.prev; nn != &steps; nn = nn->prev) {
  for (PLN_BaseStep *step = steps.tail(); step; step = step->list_node.prev) {
    if (step->IsReduce()) {
      break;
    }
    if (step->type == PLN_T_ARRANGE) {
      return static_cast<PLN_ArrangeStep *>(step);
    }
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

// Gets the last arrange step for the current pipeline stage. If no arrange step exists, one is created.
// This function should be used to limit/page through the current step.

PLN_ArrangeStep *AGGPlan::GetOrCreateArrangeStep() {
  PLN_ArrangeStep *step = GetArrangeStep();
  if (step) {
    return step;
  }
  step = new PLN_ArrangeStep();
  AddStep(step);
  return step;
}

//---------------------------------------------------------------------------------------------

/**
 * Get the lookup provided the given mode
 * @param bstp - acts as a placeholder for iteration. If mode is FIRST, then
 *  this acts as a barrier and no lookups after this step are returned. If mode
 *  is LAST, then this acts as an initializer, and steps before this (inclusive)
 *  are ignored (NYI).
 */

RLookup *AGGPlan::GetLookup(const PLN_BaseStep *bstp, AGPLNGetLookupMode mode) const {
  const PLN_BaseStep *first = NULL, *last = NULL;
  bool isReverse = false;

  switch (mode) {
    case AGPLN_GETLOOKUP_FIRST:
      first = steps.first();
      last = bstp ? bstp : NULL;
      break;
    case AGPLN_GETLOOKUP_PREV:
      first = NULL;
      last = bstp->list_node.prev;
      isReverse = true;
      break;
    case AGPLN_GETLOOKUP_NEXT:
      first = bstp->list_node.next;
      last = NULL;
      break;
    case AGPLN_GETLOOKUP_LAST:
      first = bstp ? bstp : NULL;
      last = steps.last();
      isReverse = true;
  }

  if (isReverse) {
    for (const PLN_BaseStep *step = last; step && step != first; step = step->list_node.prev) {
      RLookup *lk = step->getLookup();
      if (lk) {
        return lk;
      }
    }
  } else {
    for (const PLN_BaseStep *step = first; step && step != last; step = step->list_node.next) {
      RLookup *lk = step->getLookup();
      if (lk) {
        return lk;
      }
    }
    return NULL;
  }
  return NULL;
}

//---------------------------------------------------------------------------------------------

AGGPlan::~AGGPlan() {
  PLN_BaseStep *step = steps.first();
  while (step) {
    PLN_BaseStep *next = step->list_node.next;
    delete step;
    step = next;
  }
}

//---------------------------------------------------------------------------------------------

void AGGPlan::Dump() const {
  for (const PLN_BaseStep *step = steps.first(); step; step = step->list_node.next) {
    printf("STEP: [T=%s. P=%p]\n", steptypeToString(step->type), step);
    const RLookup *lk = step->getLookup();
    if (lk) {
      printf("  NEW LOOKUP: %p\n", lk);
      for (const RLookupKey *kk = lk->head; kk; kk = kk->next) {
        printf("    %s @%p: FLAGS=0x%x\n", kk->name, kk, kk->flags);
      }
    }

    step->Dump();
  }
}

//---------------------------------------------------------------------------------------------

void PLN_MapFilterStep::Dump() const {
  printf("  EXPR:%s\n", rawExpr);
  if (alias) {
    printf("  AS:%s\n", alias);
  }
}

//---------------------------------------------------------------------------------------------

void PLN_ArrangeStep::Dump() const {
  if (offset || limit) {
    printf("  OFFSET:%lu LIMIT:%lu\n", (unsigned long) offset, (unsigned long) limit);
  }
  if (sortKeys) {
    printf("  SORT:\n");
    for (size_t ii = 0; ii < array_len(sortKeys); ++ii) {
      const char *dir = SORTASCMAP_GETASC(sortAscMap, ii) ? "ASC" : "DESC";
      printf("    %s:%s\n", sortKeys[ii], dir);
    }
  }
}

//---------------------------------------------------------------------------------------------

void PLN_LoadStep::Dump() const {
  for (size_t ii = 0; ii < args.argc; ++ii) {
    printf("  %s\n", (char *)args.objs[ii]);
  }
}

//---------------------------------------------------------------------------------------------

void PLN_GroupStep::Dump() const {
  printf("  BY:\n");
  for (size_t ii = 0; ii < nproperties; ++ii) {
    printf("    %s\n", properties[ii]);
  }
  for (size_t ii = 0; ii < array_len(reducers); ++ii) {
    const PLN_Reducer &r = reducers[ii];
    printf("  REDUCE: %s AS %s\n", r.name, r.alias);
    if (r.args.argc) {
      printf("    ARGS:[");
    }
    for (size_t jj = 0; jj < r.args.argc; ++jj) {
      printf("%s ", (char *)r.args.objs[jj]);
    }
    printf("]\n");
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////////////////////

/* Serialize the plan into an array of string args, to create a command to be sent over the network.
 * The strings need to be freed with free and the array needs to be freed with array_free().
 * The length can be extracted with array_len */

array_t AGGPlan::Serialize() const {
  char **arr = array_new(char *, 1);
  for (const PLN_BaseStep *step = steps.first(); step; step = step->list_node.next) {
    switch (step->type) {
      case PLN_T_APPLY:
      case PLN_T_FILTER:
        serializeMapFilter(&arr, step);
        break;
      case PLN_T_ARRANGE:
        serializeArrange(&arr, step);
        break;
      case PLN_T_LOAD:
        serializeLoad(&arr, step);
        break;
      case PLN_T_GROUP:
        serializeGroup(&arr, step);
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
