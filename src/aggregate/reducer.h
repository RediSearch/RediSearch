#pragma once

#include "redisearch.h"
#include "result_processor.h"
#include "query_error.h"
#include "triemap/triemap.h"
#include "hll/hll.h"
#include "util/block_alloc.h"
#include "util/quantile.h"

#include <unordered_set>

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
  virtual ~Reducer() {
    delete srckey;
    delete dstkey;
  }

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
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRSum : public Reducer {
  RDCRSum(const ReducerOptions *);

  struct Data {
    double total;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

struct RDCRAvg : public Reducer {
  RDCRAvg(const ReducerOptions *);

  struct Data {
    size_t count;
    double total;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRToList : public Reducer {
  RDCRToList(const ReducerOptions *);

  struct Data {
    TrieMap values;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRMin : public Reducer {
  RDCRMin(const ReducerOptions *);

  struct Data {
    double val;
    size_t numMatches;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

struct RDCRMax : public Reducer {
  RDCRMax(const ReducerOptions *);

  struct Data {
    double val;
    size_t numMatches;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};
//---------------------------------------------------------------------------------------------

struct RDCRCountDistinct : public Reducer {
  RDCRCountDistinct(const ReducerOptions *);

  struct Data {
    std::unordered_set<uint64_t> dedup; //@@
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRQuantile : public Reducer {
  RDCRQuantile(const ReducerOptions *);

  double pct;
  unsigned resolution;

  struct Data {
    QuantStream *qs;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRStdDev : public Reducer {
  RDCRStdDev(const ReducerOptions *);

  struct Data {
    size_t n;
    double oldM, newM, oldS, newS;

    void Add(double d);
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

struct RDCRFirstValue : public Reducer {
  RDCRFirstValue(const ReducerOptions *);
  ~RDCRFirstValue();

  bool ascending;

  struct Data {
    const RLookupKey *sortprop;  // The key to sort by
    RSValue *value;              // Value to return
    RSValue *sortval;            // Top sorted value
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();

  // private
  int sort(const RLookupRow *srcrow);
  int noSort(const RLookupRow *srcrow);
};

//---------------------------------------------------------------------------------------------

struct RDCRRandomSample : public Reducer {
  RDCRRandomSample(const ReducerOptions *);
  ~RDCRRandomSample();

  size_t len;

  struct Data {
    size_t seen;  // how many items we've seen
    RSValue *samplesArray;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
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

  struct Data {
    std::unordered_set<uint64_t> dedup;
    struct HLL hll;
    const RLookupKey *key;
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

struct RDCRHLL : public RDCRHLLCommon {
  RDCRHLL(const ReducerOptions *);
};

//---------------------------------------------------------------------------------------------

struct RDCRHLLSum : public Reducer {
  RDCRHLLSum(const ReducerOptions *);
  ~RDCRHLLSum();

  struct Data {
    struct HLL hll;

    int Add(const char *buf);
  } data;

  int Add(const RLookupRow *srcrow);
  RSValue *Finalize();
};

//---------------------------------------------------------------------------------------------

// Macro to ensure that we don't skip important initialization steps
#define REDUCEROPTS_INIT(name_, args_, lk_, statusp_) { name_, args_, lk_, statusp_ }

typedef Reducer *(*ReducerFactory)(const ReducerOptions *);
ReducerFactory RDCR_GetFactory(const char *name);
void RDCR_RegisterFactory(const char *name, ReducerFactory factory);
void RDCR_RegisterBuiltins();

///////////////////////////////////////////////////////////////////////////////////////////////
