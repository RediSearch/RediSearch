#ifndef AGGREGATE_PLAN_H_
#define AGGREGATE_PLAN_H_
#include <value.h>
#include <rlookup.h>
#include <search_options.h>
#include <aggregate/expr/expression.h>
#include <util/dllist.h>

typedef struct AGGPlan AGGPlan, AggregatePlan;

struct AggregatePlan;

typedef enum {
  PLN_T_ROOT = 0,
  PLN_T_GROUP,
  PLN_T_DISTRIBUTE,
  PLN_T_FILTER,
  PLN_T_APPLY,
  PLN_T_ARRANGE
} PLN_StepType;

typedef enum {
  PLN_F_ALIAS = 0x01,  // Plan step has an alias

  // Plan step is a reducer. This does not mean it uses a reduce function, but
  // rather that it fundamentally modifies the rows.
  PLN_F_REDUCER = 0x02
} PlanFlags;

typedef struct PLN_BaseStep {
  DLLIST_node llnodePln;  // Linked list node for previous/next

  PLN_StepType type : 32;
  uint32_t flags;  // PLN_F_XXX

  const char *alias;
  void (*dtor)(struct PLN_BaseStep *);  // Called to destroy step-specific data

  // Type specific stuff goes here..
} PLN_BaseStep;

/**
 * JUNCTION/REDUCTION POINTS
 *
 * While generally the plan steps are serial, in which they transform rows, some
 * steps may reduce rows and modify them, so that the rows do not really match
 * one another.
 */

/**
 * First step. This contains the lookup used for the initial document keys.
 */
typedef struct {
  PLN_BaseStep base;
  RLookup lookup;
} PLN_FirstStep;

typedef struct {
  PLN_BaseStep base;
  const char *rawExpr;
  RSExpr *parsedExpr;
} PLN_MapFilterStep;

/** ARRANGE covers sort, limit, and so on */
typedef struct {
  PLN_BaseStep base;

  const char **sortKeys;  // array_*
  uint64_t sortAscMap;    // Mapping of ascending/descending. Bitwise

  uint64_t offset;  // Seek results. If 0, then no paging is applied
  uint64_t limit;   // Number of rows to output
} PLN_ArrangeStep;

/* Group step - group by properties and reduce by several reducers */
typedef struct {
  PLN_BaseStep base;
  RLookup lookup;

  const char **properties;
  size_t nproperties;

  /* Group step single reducer, a function and its args */
  struct PLN_Reducer {
    const char *name;  // Name of function
    char *alias;       // Output key
    ArgsCursor args;
  } * reducers;
  int idx;
} PLN_GroupStep;

typedef struct PLN_Reducer PLN_Reducer;

/* Schema property kind (not type!) is this a field from the result, a projection or an aggregation?
 */
typedef enum {
  Property_Field = 1,
  Property_Aggregate = 2,
  Property_Projection = 3,
} AggregatePropertyKind;

/* Distribute step - send a sub-plan to all shards and collect the results */
typedef struct {
  struct AggregatePlan *plan;
} AggregateDistributeStep;

/* A plan is a linked list of all steps */
struct AGGPlan {
  DLLIST steps;
  PLN_ArrangeStep *arrangement;
  PLN_FirstStep firstStep_s;  // Storage for initial plan
};

/* Serialize the plan into an array of string args, to create a command to be sent over the network.
 * The strings need to be freed with free and the array needs to be freed with array_free(). The
 * length can be extracted with array_len */
char **AGPLN_Serialize(AGGPlan *plan);

/* Free the plan resources, not the plan itself */
void AGPLN_Free(AGGPlan *plan);

/* Print the plan */
void AGPLN_Print(AGGPlan *plan);

void AGPLN_Init(AGGPlan *plan);

void AGPLN_AddStep(AGGPlan *plan, PLN_BaseStep *step);
void AGPLN_AddBefore(AGGPlan *pln, PLN_BaseStep *step, PLN_BaseStep *add);

/**
 * Gets the last arrange step for the current pipeline stage. If no arrange
 * step exists, one is created.
 *
 * This function should be used to limit/page through the current step
 */
PLN_ArrangeStep *AGPLN_GetArrangeStep(AggregatePlan *pln);

typedef enum {
  // Get the root lookup, stopping at stp if provided
  AGPLN_GETLOOKUP_FIRST,

  // Gets the previous lookup in respect to stp
  AGPLN_GETLOOKUP_PREV,

  // Get the last lookup, stopping at bstp
  AGPLN_GETLOOKUP_LAST,

  // Get the next lookup, starting from bstp
  AGPLN_GETLOOKUP_NEXT
} AGPLNGetLookupMode;

/**
 * Get the lookup provided the given mode
 * @param pln the plan containing the steps
 * @param bstp - acts as a placeholder for iteration. If mode is FIRST, then
 *  this acts as a barrier and no lookups after this step are returned. If mode
 *  is LAST, then this acts as an initializer, and steps before this (inclusive)
 *  are ignored (NYI).
 */
RLookup *AGPLN_GetLookup(const AGGPlan *pln, const PLN_BaseStep *bstp, AGPLNGetLookupMode mode);

/**
 * Determines if the plan is a 'reduce' type. A 'reduce' plan is one which
 * consumes (in entirety) all of its inputs and produces a new output (and thus
 * a new 'Lookup' table)
 */
static inline int PLN_IsReduce(const PLN_BaseStep *pln) {
  switch (pln->type) {
    case PLN_T_ROOT:
    case PLN_T_GROUP:
      return 1;
    default:
      return 0;
  }
}
#endif