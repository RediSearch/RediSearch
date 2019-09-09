#ifndef RS_REDUCER_H_
#define RS_REDUCER_H_

#include <redisearch.h>
#include <result_processor.h>
#include <dep/triemap/triemap.h>
#include <util/block_alloc.h>
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  REDUCER_T_COUNT = 0,
  REDUCER_T_SUM,
  REDUCER_T_MIN,
  REDUCER_T_MAX,
  REDUCER_T_AVG,
  REDUCER_T_QUANTILE,
  REDUCER_T_STDDEV,
  REDUCER_T_DISTINCT,
  REDUCER_T_DISTINCTISH,
  REDUCER_T_HLL,
  REDUCER_T_HLLSUM,
  REDUCER_T_SAMPLE,

  /** Not a reducer, but a marker of the end of the list */
  REDUCER_T__END
} ReducerType;

/* Maximum possible value to random sample group size */
#define MAX_SAMPLE_SIZE 1000

typedef struct Reducer {
  /**
   * Most reducers only operate on a single source key. This can be used to
   * store the key. This value is not read by the grouper system.
   */
  const RLookupKey *srckey;

  RLookupKey *dstkey;  // Destination key where the reducer output is placed

  /**
   * Common allocator for all groups. Used to reduce fragmentation when allocating
   * like-sized objects for different groups.
   */
  BlkAlloc alloc;

  /** Numeric ID identifying this reducer */
  uint32_t reducerId;

  /**
   * Creates a new per-group instance of this reducer. This is used to create
   * actual data. The reducer structure itself, on the other hand, may be
   * used to retain settings common to all group.s
   */
  void *(*NewInstance)(struct Reducer *r);

  /**
   * Passes a result through the reducer. The reducer can then store the
   * results internally until it can be outputted in `dstrow`.
   *
   * The function should return 1 if added successfully, or nonzero if an error
   * occurred
   */
  int (*Add)(struct Reducer *parent, void *instance, const RLookupRow *srcrow);

  /**
   * Called when Add() has been invoked for the last time. This is used to
   * populate the result of the reduce function.
   */
  RSValue *(*Finalize)(struct Reducer *parent, void *instance);

  /** Frees the object created by NewInstance() */
  void (*FreeInstance)(struct Reducer *parent, void *instance);

  /**
   * Frees the global reducer struct (this object)
   */
  void (*Free)(struct Reducer *r);

} Reducer;

static inline void Reducer_GenericFree(Reducer *r) {
  BlkAlloc_FreeAll(&r->alloc, NULL, 0, 0);
  rm_free(r);
}

// Format a function name in the form of s(arg). Returns a pointer for use with 'free'
static inline char *FormatAggAlias(const char *alias, const char *fname, const char *propname) {
  if (alias) {
    return rm_strdup(alias);
  }

  if (!propname || *propname == 0) {
    return rm_strdup(fname);
  }

  char *s = NULL;
  rm_asprintf(&s, "%s(%s)", fname, propname);
  return s;
}

typedef struct {
  const char *name;    // Name the reducer was called as
  ArgsCursor *args;    // Raw reducer arguments
  RLookup *srclookup;  // Lookup to used for locating fields

  /**
   * OUT parameter. If the return value is NULL, AND this value on input is
   * NOT NULL, then the error information will be set here.
   */
  QueryError *status;
} ReducerOptions;

/**
 * Macro to ensure that we don't skip important initialization steps
 */
#define REDUCEROPTS_INIT(name_, args_, lk_, statusp_) \
  { name_, args_, lk_, statusp_ }

/**
 * Utility function to read the next argument as a lookup key.
 * This advances the args variable (ReducerOptions::options) by one.
 *
 * If the lookup fails, the appropriate error code is stored in the status
 * within the options
 *
 * @return boolean - 0=fail, !0=success
 */
int ReducerOpts_GetKey(const ReducerOptions *options, const RLookupKey **kout);
#define ReducerOptions_GetKey ReducerOpts_GetKey

/**
 * This helper function ensures that all of a reducer's arguments are consumed.
 * Otherwise, an error is raised to the user.
 */
int ReducerOpts_EnsureArgsConsumed(const ReducerOptions *options);

void *Reducer_BlkAlloc(Reducer *r, size_t elemsz, size_t absBlkSize);

Reducer *RDCRCount_New(const ReducerOptions *);
Reducer *RDCRSum_New(const ReducerOptions *);
Reducer *RDCRToList_New(const ReducerOptions *);
Reducer *RDCRMin_New(const ReducerOptions *);
Reducer *RDCRMax_New(const ReducerOptions *);
Reducer *RDCRAvg_New(const ReducerOptions *);
Reducer *RDCRCountDistinct_New(const ReducerOptions *);
Reducer *RDCRCountDistinctish_New(const ReducerOptions *);
Reducer *RDCRQuantile_New(const ReducerOptions *);
Reducer *RDCRStdDev_New(const ReducerOptions *);
Reducer *RDCRFirstValue_New(const ReducerOptions *);
Reducer *RDCRRandomSample_New(const ReducerOptions *);
Reducer *RDCRHLL_New(const ReducerOptions *);
Reducer *RDCRHLLSum_New(const ReducerOptions *);

typedef Reducer *(*ReducerFactory)(const ReducerOptions *);
ReducerFactory RDCR_GetFactory(const char *name);
void RDCR_RegisterFactory(const char *name, ReducerFactory factory);
void RDCR_RegisterBuiltins(void);

#ifdef __cplusplus
}
#endif
#endif
