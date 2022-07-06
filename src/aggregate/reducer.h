#pragma once

#include "redisearch.h"
#include "result_processor.h"
#include "triemap/triemap.h"
#include "util/block_alloc.h"
#include "query_error.h"
#include "hll/hll.h"

///////////////////////////////////////////////////////////////////////////////////////////////

enum ReducerType {
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

  // Not a reducer, but a marker of the end of the list
  REDUCER_T__END
};

//---------------------------------------------------------------------------------------------

// Maximum possible value to random sample group size
#define MAX_SAMPLE_SIZE 1000

//---------------------------------------------------------------------------------------------

struct Reducer : public Object {
  // Most reducers only operate on a single source key. This can be used to
  // store the key. This value is not read by the grouper system.
  const RLookupKey *srckey;

  // Destination key where the reducer output is placed
  RLookupKey *dstkey;

  // Common allocator for all groups. Used to reduce fragmentation when allocating
  // like-sized objects for different groups.
  BlkAlloc alloc;

  // Numeric ID identifying this reducer
  uint32_t reducerId;

  // Creates a new per-group instance of this reducer. This is used to create
  // actual data. The reducer structure itself, on the other hand, may be
  // used to retain settings common to all group.
  virtual Reducer() {}

  // Frees the global reducer struct (this object)
  virtual ~Reducer() {}

  // Passes a result through the reducer. The reducer can then store the
  // results internally until it can be outputted in `dstrow`.
  //
  // The function should return 1 if added successfully, or nonzero if an error occurred
  virtual int Add(const RLookupRow *srcrow);

  // Called when Add() has been invoked for the last time. This is used to
  // populate the result of the reduce function.
  virtual RSValue *Finalize();

  // Frees the object created by NewInstance()
  virtual void FreeInstance();

  void *BlkAlloc(size_t elemsz, size_t absBlkSize);
};

//---------------------------------------------------------------------------------------------

struct ReducerOptions {
  const char *name;    // Name the reducer was called as
  ArgsCursor *args;    // Raw reducer arguments
  RLookup *srclookup;  // Lookup to used for locating fields

  // OUT parameter. If the return value is NULL, AND this value on input is
  // NOT NULL, then the error information will be set here.

  QueryError *status;

  bool EnsureArgsConsumed() const;
  bool GetKey(const RLookupKey **kout) const;
};

//---------------------------------------------------------------------------------------------

struct RDCRCount : public Reducer {
  RDCRCount(const ReducerOptions *);

  struct Data {
    size_t count;
  };
};

//---------------------------------------------------------------------------------------------

struct RDCRSum : public Reducer {
  RDCRSum(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRToList : public Reducer {
  RDCRToList(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRMin : public Reducer {
  RDCRMin(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRMax : public Reducer {
  RDCRMax(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRAvg : public Reducer {
  RDCRAvg(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRCountDistinct : public Reducer {
  RDCRCountDistinct(const ReducerOptions *);

  struct Data {
    size_t count;
    const RLookupKey *srckey;
    //@@khash_t(khid) * dedup;
  };
};

//---------------------------------------------------------------------------------------------

struct RDCRQuantile : public Reducer {
  RDCRQuantile(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRStdDev : public Reducer {
  RDCRStdDev(const ReducerOptions *);

  struct Data {
    const RLookupKey *srckey;
    size_t n;
    double oldM, newM, oldS, newS;

    void Add(double d);
  };
};

//---------------------------------------------------------------------------------------------

struct RDCRFirstValue : public Reducer {
  RDCRFirstValue(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRRandomSample : public Reducer {
  RDCRRandomSample(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRHLLCommon : public Reducer {
  RDCRHLLCommon(const ReducerOptions *);

  struct Data {
    struct HLL hll;
    const RLookupKey *key;
  };
};

struct RDCRCountDistinctish : public RDCRHLLCommon {
  RDCRCountDistinctish(const ReducerOptions *);
};

struct RDCRHLL : public RDCRHLLCommon {
  RDCRHLL(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRHLLSum : public Reducer {
  RDCRHLLSum(const ReducerOptions *);

  struct Data {
    const RLookupKey *srckey;
    struct HLL hll;

    int Add(const char *buf);
  };
};

//---------------------------------------------------------------------------------------------

// Macro to ensure that we don't skip important initialization steps
#define REDUCEROPTS_INIT(name_, args_, lk_, statusp_) { name_, args_, lk_, statusp_ }

typedef Reducer *(*ReducerFactory)(const ReducerOptions *);
ReducerFactory RDCR_GetFactory(const char *name);
void RDCR_RegisterFactory(const char *name, ReducerFactory factory);
void RDCR_RegisterBuiltins();

///////////////////////////////////////////////////////////////////////////////////////////////
