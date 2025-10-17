/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef AGGREGATE_PLAN_H_
#define AGGREGATE_PLAN_H_
#include <value.h>
#include <rlookup.h>
#include <search_options.h>
#include <aggregate/expr/expression.h>
#include <util/dllist.h>
#include <obfuscation/hidden.h>
#include <util/references.h>
#include <util/arr.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct AGGPlan AGGPlan, AggregatePlan;

typedef enum {
  PLN_T_INVALID = 0,
  PLN_T_ROOT = 1,
  PLN_T_GROUP,
  PLN_T_DISTRIBUTE,
  PLN_T_FILTER,
  PLN_T_APPLY,
  PLN_T_ARRANGE,
  PLN_T_LOAD,
  PLN_T_VECTOR_NORMALIZER,
  PLN_T__MAX
} PLN_StepType;

#define PLANTYPE_ANY_REDUCER (PLN_T__MAX + 1)

typedef enum {
  PLN_F_ALIAS = 0x01,  // Plan step has an alias

  // Plan step is a reducer. This does not mean it uses a reduce function, but
  // rather that it fundamentally modifies the rows.
  PLN_F_REDUCER = 0x02,

  // Plan to load all fields by RPLoader
  PLN_F_LOAD_ALL = 0x04,
} PlanFlags;

typedef struct PLN_BaseStep {
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
} PLN_BaseStep;

#define PLN_NEXT_STEP(step) DLLIST_ITEM((step)->llnodePln.next, PLN_BaseStep, llnodePln)
#define PLN_PREV_STEP(step) DLLIST_ITEM((step)->llnodePln.prev, PLN_BaseStep, llnodePln)
#define PLN_END_STEP(plan) DLLIST_ITEM(&(plan)->steps, PLN_BaseStep, llnodePln)

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
  HiddenString *expr;
  RSExpr *parsedExpr;
  bool noOverride;     // Whether we should override the alias if it exists. We allow it by default
} PLN_MapFilterStep;

/** ARRANGE covers sort, limit, and so on */
typedef struct {
  PLN_BaseStep base;
  const RLookupKey **sortkeysLK;  // simple array
  const char **sortKeys;          // array_*
  uint64_t sortAscMap;            // Mapping of ascending/descending. Bitwise
  bool isLimited;                 // Flag if `LIMIT` keyword was used.
  bool runLocal;                  // Indicator that this step should run only local (not in shards)
  uint64_t offset;                // Seek results. If 0, then no paging is applied
  uint64_t limit;                 // Number of rows to output
} PLN_ArrangeStep;

/** LOAD covers any fields not implicitly found within the document */
typedef struct {
  PLN_BaseStep base;
  ArgsCursor args;
  const RLookupKey **keys;
  size_t nkeys;
} PLN_LoadStep;

/** VECTOR_NORMALIZER normalizes vector distance scores to [0,1] range */
typedef struct {
  PLN_BaseStep base;
  const char *vectorFieldName;     // Vector field name (NOT owned - points to parser tokens)
  const char *distanceFieldAlias;  // Distance field alias (owned)
} PLN_VectorNormalizerStep;

/* Group step - group by properties and reduce by several reducers */
typedef struct {
  PLN_BaseStep base;
  RLookup lookup;

  StrongRef properties_ref;  // StrongRef to properties array

  /* Group step single reducer, a function and its args */
  struct PLN_Reducer {
    const char *name;  // Name of function
    char *alias;       // Output key
    bool isHidden;     // If the output key is hidden. Used by the coordinator
    ArgsCursor args;
  } * reducers;
  int idx;
} PLN_GroupStep;

 /**
  * Allocates and initializes a new group step.
  * @param properties_ref StrongRef referencing the properties array (must be cloned by caller)
  * @return Pointer to the newly created group step
  */
PLN_GroupStep *PLNGroupStep_New(StrongRef properties_ref);

/**
 * Gets the properties array from a group step (via StrongRef)
 */
arrayof(const char*) PLNGroupStep_GetProperties(const PLN_GroupStep *gstp);

/**
 * Adds a reducer (with its arguments) to the group step
 * @param gstp the group step
 * @param name the name of the reducer
 * @param ac arguments to the reducer; if an alias is used, it is provided
 *  here as well.
 */
int PLNGroupStep_AddReducer(PLN_GroupStep *gstp, const char *name, ArgsCursor *ac,
                            QueryError *status);

PLN_MapFilterStep *PLNMapFilterStep_New(const HiddenString *expr, int mode);

/**
 * Clone a LOAD step for use in individual AREQ pipelines.
 * Handles only unprocessed (has args) LOAD steps.
 * This is used to clone and propagate the LOAD step to the individual AREQ pipelines (Hybrid)
 *
 * @param original The original PLN_LoadStep to clone
 * @return New cloned PLN_LoadStep or NULL if original is NULL
 */
PLN_LoadStep *PLNLoadStep_Clone(const PLN_LoadStep *original);

#ifdef __cplusplus
typedef PLN_GroupStep::PLN_Reducer PLN_Reducer;
#else
typedef struct PLN_Reducer PLN_Reducer;
#endif

/**
 * Find a reducer by name and args in the group step
 * @param gstp the group step
 * @param name the name of the reducer
 * @param ac arguments to the reducer; if an alias is used, it is provided
 *  here as well.
 */
PLN_Reducer *PLNGroupStep_FindReducer(PLN_GroupStep *gstp, const char *name, ArgsCursor *ac);

/* A plan is a linked list of all steps */
struct AGGPlan {
  DLLIST steps;
  PLN_ArrangeStep *arrangement;
  PLN_FirstStep firstStep_s;  // Storage for initial plan
  uint64_t steptypes;         // Mask of step-types contained in plan
};

/* Serialize the plan into an array of string args, to create a command to be sent over the network.
 * The strings need to be freed with free and the array needs to be freed with array_free(). The
 * length can be extracted with array_len */
void AGPLN_Serialize(const AGGPlan *plan, arrayof(char*) *target);

/* Free the plan resources, not the plan itself */
void AGPLN_Free(AGGPlan *plan);

void AGPLN_Init(AGGPlan *plan);

/* Frees all the steps within the plan */
void AGPLN_FreeSteps(AGGPlan *pln);

/* Destructor for PLN_LoadStep */
void loadDtor(PLN_BaseStep *bstp);

/* Constructor for PLN_VectorNormalizerStep */
PLN_VectorNormalizerStep *PLNVectorNormalizerStep_New(const char *vectorFieldName, const char *distanceFieldAlias);

void AGPLN_AddStep(AGGPlan *plan, PLN_BaseStep *step);
void AGPLN_AddBefore(AGGPlan *pln, PLN_BaseStep *step, PLN_BaseStep *add);
void AGPLN_AddAfter(AGGPlan *pln, PLN_BaseStep *step, PLN_BaseStep *add);
void AGPLN_Prepend(AGGPlan *pln, PLN_BaseStep *newstp);

/* Removes the step from the plan */
void AGPLN_PopStep(PLN_BaseStep *step);

/** Checks if a step with the given type is contained within the plan */
int AGPLN_HasStep(const AGGPlan *pln, PLN_StepType t);
/**
 * Gets the last arrange step for the current pipeline stage. If no arrange
 * step exists, return NULL.
 *
 */
PLN_ArrangeStep *AGPLN_GetArrangeStep(AGGPlan *pln);

/**
 * Add an arrange step that corresponds a KNN clause in the query, where the field to sort by it is
 * the distFieldName, and k is the limit. We add this step to the head of the steps linked list,
 * as this is the first one to be executed before the rest of the local pipeline.
 * @param pln the local aggregate plan the was built.
 * @param k the number of results to return from this step onward.
 * @param distFieldName the field that stores the vector metric distance of some result from the
 * query vector to sort by it (note that this is owned  by the query node).
 * @return the newly created step
 */
PLN_ArrangeStep *AGPLN_AddKNNArrangeStep(AGGPlan *pln, size_t k, const char *distFieldName);

/**
 * Gets the last arrange step for the current pipeline stage. If no arrange
 * step exists, one is created.
 *
 * This function should be used to limit/page through the current step
 */
PLN_ArrangeStep *AGPLN_GetOrCreateArrangeStep(AGGPlan *pln);

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
const PLN_BaseStep *AGPLN_FindStep(const AGGPlan *pln, const PLN_BaseStep *begin,
                                   const PLN_BaseStep *end, PLN_StepType type);

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
 * @brief Dumps the contents of an aggregation plan to stdout for debugging.
 *
 * This function iterates through all steps in the given AGGPlan and prints
 * detailed information about each step, including step type, pointers, lookup
 * keys, expressions, sorting, grouping, and reducer details. It is useful for
 * inspecting the structure and configuration of an aggregation plan during
 * development or troubleshooting.
 *
 * @param pln Pointer to the AGGPlan to be dumped.
 */
void AGPLN_Dump(const AGGPlan *pln);

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

#ifdef __cplusplus
}
#endif
#endif
