
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
  PLN_BaseStep();
  virtual ~PLN_BaseStep();

  DLLIST_node llnodePln;  // Linked list node for previous/next

  PLN_StepType type : 32;
  uint32_t flags;  // PLN_F_XXX

  const char *alias;

  // Called to yield the lookup structure for the given step. If this object
  // does not have a lookup, can be set to NULL.
  virtual RLookup *getLookup();

  // Determines if the plan is a 'reduce' type. A 'reduce' plan is one which
  // consumes (in entirety) all of its inputs and produces a new output (and thus a new 'Lookup' table)

  bool IsReduce(const PLN_BaseStep *pln) const {
    switch (pln->type) {
      case PLN_T_ROOT:
      case PLN_T_GROUP:
        return true;
      default:
        return false;
    }
  }


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
struct PLN_FirstStep : PLN_BaseStep {
  virtual ~PLN_FirstStep();
  RLookup lookup;
};

//---------------------------------------------------------------------------------------------

struct PLN_MapFilterStep : PLN_BaseStep {
  const char *rawExpr;
  RSExpr *parsedExpr;
  int shouldFreeRaw;  // Whether we own the raw expression, used on coordinator only

  PLNMapFilterStep(const char *expr, int mode);
};

//---------------------------------------------------------------------------------------------

// Magic value -- will sort by score. For use in SEARCH mode
#define PLN_SORTKEYS_DFLSCORE (const char **)0xdeadbeef

// ARRANGE covers sort, limit, and so on
struct PLN_ArrangeStep : PLN_BaseStep {
  const RLookupKey **sortkeysLK;  // simple array
  const char **sortKeys;          // array_*
  uint64_t sortAscMap;            // Mapping of ascending/descending. Bitwise
  uint64_t offset;                // Seek results. If 0, then no paging is applied
  uint64_t limit;                 // Number of rows to output
};

//---------------------------------------------------------------------------------------------

// LOAD covers any fields not implicitly found within the document
struct PLN_LoadStep : PLN_BaseStep {
  ArgsCursor args;
  const RLookupKey **keys;
  size_t nkeys;
};

//---------------------------------------------------------------------------------------------

// Group step - group by properties and reduce by several reducers
struct PLN_GroupStep : PLN_BaseStep{
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

  PLN_GroupStep(const char **props, size_t nprops);

  int AddReducer(const char *name, ArgsCursor *ac, QueryError *status);
};

typedef PLN_GroupStep::PLN_Reducer PLN_Reducer;

//---------------------------------------------------------------------------------------------

enum AGPLNGetLookupMode {
  AGPLN_GETLOOKUP_FIRST,  // Get the root lookup, stopping at stp if provided
  AGPLN_GETLOOKUP_PREV,   // Gets the previous lookup in respect to stp
  AGPLN_GETLOOKUP_LAST,   // Get the last lookup, stopping at bstp
  AGPLN_GETLOOKUP_NEXT    // Get the next lookup, starting from bstp
};

//---------------------------------------------------------------------------------------------

// A plan is a linked list of all steps
struct AGGPlan : Object {
  DLLIST steps;
  PLN_ArrangeStep *arrangement;
  PLN_FirstStep firstStep_s;  // Storage for initial plan
  uint64_t steptypes;         // Mask of step-types contained in plan

  array_t Serialize() const;

  AGGPlan();

  // Free the plan resources, not the plan itself
  ~AGGPlan();

  void Print() const;

  // Frees all the steps within the plan
  void FreeSteps();

  void AddStep(PLN_BaseStep *step);
  void AddBefore(PLN_BaseStep *step, PLN_BaseStep *add);
  void AddAfter(PLN_BaseStep *step, PLN_BaseStep *add);
  void Prepend(PLN_BaseStep *newstp);

  // Removes the step from the plan
  void PopStep(PLN_BaseStep *step);

  RLookup *GetLookup(const PLN_BaseStep *bstp, AGPLNGetLookupMode mode) const;

  void Dump() const;

  // Checks if a step with the given type is contained within the plan
  bool HasStep(PLN_StepType t) const;

  PLN_ArrangeStep *GetArrangeStep();
  PLN_ArrangeStep *GetOrCreateArrangeStep();

  const PLN_BaseStep *FindStep(const PLN_BaseStep *begin, const PLN_BaseStep *end, PLN_StepType type) const;
};

//---------------------------------------------------------------------------------------------

///////////////////////////////////////////////////////////////////////////////////////////////
