
#pragma once

#include "value.h"
#include "rlookup.h"
#include "search_options.h"
#include "aggregate/expr/expression.h"
#include "util/dllist.h"

///////////////////////////////////////////////////////////////////////////////////////////////

typedef struct AGGPlan AGGPlan, AggregatePlan;

enum PLN_StepType {
  PLN_T_INVALID = 0,
  PLN_T_ROOT = 1,
  PLN_T_GROUP,
  PLN_T_DISTRIBUTE,
  PLN_T_FILTER,
  PLN_T_APPLY,
  PLN_T_ARRANGE,
  PLN_T_LOAD,
  PLN_T__MAX
};

#define PLANTYPE_ANY_REDUCER (PLN_T__MAX + 1)

enum PlanFlags {
  PLN_F_ALIAS = 0x01,  // Plan step has an alias

  // Plan step is a reducer. This does not mean it uses a reduce function, but
  // rather that it fundamentally modifies the rows.
  PLN_F_REDUCER = 0x02
};

//---------------------------------------------------------------------------------------------

struct PLN_BaseStep {
  DLLIST_node llnodePln;  // Linked list node for previous/next

  PLN_StepType type : 32;
  uint32_t flags;  // PLN_F_XXX

  const char *alias;
  // Called to destroy step-specific data
  void (*dtor)(struct PLN_BaseStep *);

  // Called to yield the lookup structure for the given step. If this object
  // does not have a lookup, can be set to NULL.
  RLookup *(*getLookup)(struct PLN_BaseStep *);

  // Type specific stuff goes here..
};

//---------------------------------------------------------------------------------------------

#define PLN_NEXT_STEP(step) DLLIST_ITEM((step)->llnodePln.next, PLN_BaseStep, llnodePln)
#define PLN_PREV_STEP(step) DLLIST_ITEM((step)->llnodePln.prev, PLN_BaseStep, llnodePln)

/**
 * JUNCTION/REDUCTION POINTS
 *
 * While generally the plan steps are serial, in which they transform rows, some
 * steps may reduce rows and modify them, so that the rows do not really match one another.
 */

// First step. This contains the lookup used for the initial document keys.
struct PLN_FirstStep {
  PLN_BaseStep base;
  RLookup lookup;
};

//---------------------------------------------------------------------------------------------

struct PLN_MapFilterStep {
  PLN_BaseStep base;
  const char *rawExpr;
  RSExpr *parsedExpr;
  int shouldFreeRaw;  // Whether we own the raw expression, used on coordinator only
};

//---------------------------------------------------------------------------------------------

// Magic value -- will sort by score. For use in SEARCH mode
#define PLN_SORTKEYS_DFLSCORE (const char **)0xdeadbeef

// ARRANGE covers sort, limit, and so on
struct PLN_ArrangeStep {
  PLN_BaseStep base;
  const RLookupKey **sortkeysLK;  // simple array
  const char **sortKeys;          // array_*
  uint64_t sortAscMap;            // Mapping of ascending/descending. Bitwise
  uint64_t offset;                // Seek results. If 0, then no paging is applied
  uint64_t limit;                 // Number of rows to output
};

//---------------------------------------------------------------------------------------------

// LOAD covers any fields not implicitly found within the document
struct PLN_LoadStep {
  PLN_BaseStep base;
  ArgsCursor args;
  const RLookupKey **keys;
  size_t nkeys;
};

//---------------------------------------------------------------------------------------------

// Group step - group by properties and reduce by several reducers
struct PLN_GroupStep {
  PLN_BaseStep base;
  RLookup lookup;

  const char **properties;
  size_t nproperties;

  // Group step single reducer, a function and its args
  struct PLN_Reducer {
    const char *name;  // Name of function
    char *alias;       // Output key
    ArgsCursor args;
  };

  PLN_Reducer *reducers;
  int idx;
};

// Returns a new group step with the appropriate constructor

PLN_GroupStep *PLNGroupStep_New(const char **props, size_t nprops);

int PLNGroupStep_AddReducer(PLN_GroupStep *gstp, const char *name, ArgsCursor *ac,
                            QueryError *status);

PLN_MapFilterStep *PLNMapFilterStep_New(const char *expr, int mode);

#ifdef __cplusplus
typedef PLN_GroupStep::PLN_Reducer PLN_Reducer;
#else
typedef struct PLN_Reducer PLN_Reducer;
#endif

//---------------------------------------------------------------------------------------------

// A plan is a linked list of all steps
struct AGGPlan {
  DLLIST steps;
  PLN_ArrangeStep *arrangement;
  PLN_FirstStep firstStep_s;  // Storage for initial plan
  uint64_t steptypes;         // Mask of step-types contained in plan
};

array_t AGPLN_Serialize(const AGGPlan *plan);

// Free the plan resources, not the plan itself
void AGPLN_Free(AGGPlan *plan);

void AGPLN_Print(AGGPlan *plan);

void AGPLN_Init(AGGPlan *plan);

// Frees all the steps within the plan
void AGPLN_FreeSteps(AGGPlan *pln);

void AGPLN_AddStep(AGGPlan *plan, PLN_BaseStep *step);
void AGPLN_AddBefore(AGGPlan *pln, PLN_BaseStep *step, PLN_BaseStep *add);
void AGPLN_AddAfter(AGGPlan *pln, PLN_BaseStep *step, PLN_BaseStep *add);
void AGPLN_Prepend(AGGPlan *pln, PLN_BaseStep *newstp);

// Removes the step from the plan
void AGPLN_PopStep(AGGPlan *pln, PLN_BaseStep *step);

// Checks if a step with the given type is contained within the plan
int AGPLN_HasStep(const AGGPlan *pln, PLN_StepType t);
PLN_ArrangeStep *AGPLN_GetArrangeStep(AGGPlan *pln);

PLN_ArrangeStep *AGPLN_GetOrCreateArrangeStep(AGGPlan *pln);

const PLN_BaseStep *AGPLN_FindStep(const AGGPlan *pln, const PLN_BaseStep *begin,
                                   const PLN_BaseStep *end, PLN_StepType type);

enum AGPLNGetLookupMode {
  AGPLN_GETLOOKUP_FIRST,  // Get the root lookup, stopping at stp if provided
  AGPLN_GETLOOKUP_PREV,   // Gets the previous lookup in respect to stp
  AGPLN_GETLOOKUP_LAST,   // Get the last lookup, stopping at bstp
  AGPLN_GETLOOKUP_NEXT    // Get the next lookup, starting from bstp
};

RLookup *AGPLN_GetLookup(const AGGPlan *pln, const PLN_BaseStep *bstp, AGPLNGetLookupMode mode);

void AGPLN_Dump(const AGGPlan *pln);

//---------------------------------------------------------------------------------------------

// Determines if the plan is a 'reduce' type. A 'reduce' plan is one which
// consumes (in entirety) all of its inputs and produces a new output (and thus a new 'Lookup' table)

inline int PLN_IsReduce(const PLN_BaseStep *pln) {
  switch (pln->type) {
    case PLN_T_ROOT:
    case PLN_T_GROUP:
      return 1;
    default:
      return 0;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
