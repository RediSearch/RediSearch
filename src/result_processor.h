
#pragma once

#include "redisearch.h"
#include "redis_index.h"
#include "tag_index.h"
#include "sortable.h"
#include "value.h"
#include "concurrent_ctx.h"
#include "search_ctx.h"
#include "index_iterator.h"
#include "search_options.h"
#include "rlookup.h"
#include "extension.h"
#include "score_explain.h"
#include "util/minmax_heap.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct ResultProcessor;
struct RLookup;

//---------------------------------------------------------------------------------------------

// Result Processor Chain
//
// We use a chain of result processors to sort, score, filter and page the results coming from the
// index.
// The index iterator tree is responsible for extracting results from the index, and the processor
// chain is responsible for processing those and preparing them for the users.
// The processors are exposing an iterator interface, adding values to SearchResult objects.
// SearchResult objects contain all the data needed for a search result - from docId and score, to
// the actual fields loaded from redis.
// Processors can add more fields, rewrite them, change the score, etc.
// The query plan builds the chain based on the request, and then the chain just processes the
// results.

// Query processing state
enum QITRState {
  QITR_S_RUNNING,
  QITR_S_ABORTED,

  // TimedOut state differs from aborted in that it lets the processors drain their accumulated
  // results instead of stopping in our tracks and returning nothing.
  QITR_S_TIMEDOUT
};

//---------------------------------------------------------------------------------------------

struct QueryIterator {
  struct ResultProcessor *rootProc; // First processor
  struct ResultProcessor *endProc;  // Last processor
  ConcurrentSearch *conc;           // Concurrent search context for thread switching
  RedisSearchCtx *sctx;             // Contains our spec
  double minScore;                  // the minimal score applicable for a result. It can be used to optimize the scorers
  uint32_t totalResults;            // the total results found in the query, incremented by the root processors and decremented by
                                    // others who might disqualify results
  QueryError *err;                  // Object which contains the error
  QITRState state;                  // the state - used for aborting queries
  struct timespec startTime;

  void Cleanup();
  IndexIterator *GetRootFilter();
  void PushRP(struct ResultProcessor *rp);
  void FreeChain();
};

//---------------------------------------------------------------------------------------------

// SearchResult
//
// the object all the processing chain is working on.
// It has the indexResult which is what the index scan brought - scores, vectors, flags, etc.
// And a list of fields loaded by the chain - currenly only by the loader, but possibly by
// aggregators later on.

struct SearchResult {
  t_docId docId;

  // not all results have score - TBD
  double score;
  RSScoreExplain *scoreExplain;

  std::shared_ptr<RSDocumentMetadata> dmd;

  // index result should cover what you need for highlighting, but we will add a method to
  // duplicate index results to make them thread safe.
  IndexResult *indexResult;

  // Row data. Use RLookup* functions to access
  RLookupRow rowdata;

  SearchResult();
  ~SearchResult();

  // Resets the search result, so that it may be reused. Internal caches are reset but not freed.
  void Clear();
};

//---------------------------------------------------------------------------------------------

// Result processor return codes

// Possible return values from ResultProcessor::Next()

enum RPStatus {
  // Result is filled with valid data
  RS_RESULT_OK = 0,
  // Result is empty, and the last result has already been returned.
  RS_RESULT_EOF,
  // Execution paused due to rate limiting (or manual pause from ext. thread??)
  RS_RESULT_PAUSED,
  // Execution halted because of timeout
  RS_RESULT_TIMEDOUT,
  // Aborted because of error. The QueryState (parent->status) should have more information
  RS_RESULT_ERROR,

  // Not a return code per se, but a marker signifying the end of the 'public' return codes.
  // Implementations can use this for extensions.
  RS_RESULT_MAX
};

//---------------------------------------------------------------------------------------------

// Result processor structure. This should be "Subclassed" by the actual implementations

struct ResultProcessor : public Object {
  QueryIterator *parent;             // Reference to the parent structure
  struct ResultProcessor *upstream;  // Previous result processor in the chain
  const char *name;                  // For debugging purposes

  // Populates the result pointed to by `res`. The existing data of `res` is not read,
  // so it is the responsibility of the caller to ensure that there
  // are no refcount leaks in the structure.
  //
  // Users can use SearchResult::Clear() to reset the structure without freeing it.
  //
  // The populated structure (if RS_RESULT_OK is returned) does contain references
  // to document data. Callers *MUST* ensure they are eventually freed.

  virtual int Next(SearchResult *res);

  ResultProcessor(const char *name) : name(name) {}
  virtual ~ResultProcessor() {}

  void DumpChain() const;
};

//---------------------------------------------------------------------------------------------

// Get the index spec from the result processor
#define RP_SPEC(rpctx) ((rpctx)->parent->sctx->spec)

// Base Result Processor - this processor is the topmost processor of every processing chain.
// It takes the raw index results from the index, and builds the search result to be sent downstream.
struct RPIndexIterator : public ResultProcessor {
  IndexIterator *iiter;

  RPIndexIterator(IndexIterator *itr);

  int Next(SearchResult *res);
};

//---------------------------------------------------------------------------------------------

// Functions abstracting the sortmap. Hides the bitwise logic
#define SORTASCMAP_INIT 0xFFFFFFFFFFFFFFFF
#define SORTASCMAP_MAXFIELDS 8
#define SORTASCMAP_SETASC(mm, pos) ((mm) |= (1LLU << (pos)))
#define SORTASCMAP_SETDESC(mm, pos) ((mm) &= ~(1LLU << (pos)))
#define SORTASCMAP_GETASC(mm, pos) ((mm) & (1LLU << (pos)))

//---------------------------------------------------------------------------------------------

// Loading Processor
//
// This processor simply takes the search results, and based on the request parameters, loads the
// relevant fields for the results that need to be displayed to the user, from redis.
// It fills the result objects' field map with values corresponding to the requested return fields

struct ResultsLoader : public ResultProcessor {
  RLookup *lk;
  const RLookupKey **fields;
  size_t nfields;

  ResultsLoader(RLookup *lk, const RLookupKey **keys, size_t nkeys);
  ~ResultsLoader();

  int Next(SearchResult *res);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// Scoring Processor
//
// It takes results from upstream, and using a scoring function applies the score to each one.
// It may not be invoked if we are working in SORTBY mode (or later on in aggregations)

struct RPScorer : public ResultProcessor {
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  ScoringFunctionArgs scorerCtx;

  RPScorer(const ExtScoringFunction *funcs, const ScoringFunctionArgs *fnargs);
  ~RPScorer();

  int Next(SearchResult *res);
};

//---------------------------------------------------------------------------------------------

// Sorting Processor
//
// This is where things become a bit complex...
// The sorter takes scored results from the scorer (or in the case of SORTBY, the raw results), and
// maintains a heap of the top N results.
// Since we need it to be thread safe, every result that's put on the heap is copied, including its
// index result tree.
// This means that from here down-stream, everything is thread safe, but we also need to properly
// free discarded results.
// The sorter is actually a reducer - it returns RS_RESULT_QUEUED until its upstream parent returns
// EOF. then it starts yielding results one by one by popping from the top of the heap.
// Note: We use a min-max heap to simplify maintaining a max heap where we can pop from the bottom
// while finding the top N results

typedef int (*RPSorterCompareFunc)(const void *e1, const void *e2, const void *udata);

struct RPSorter : public ResultProcessor {
  // The desired size of the heap - top N results
  // If set to 0 this is a growing heap
  uint32_t size;

  // The offset - used when popping result after we're done
  uint32_t offset;

  // The heap. We use a min-max heap here
  MinMaxHeap<SearchResult *> *pq;

  // the compare function for the heap. We use it to test if a result needs to be added to the heap
  RPSorterCompareFunc cmp;

  // pooled result - we recycle it to avoid allocations
  SearchResult *pooledResult;

  struct Cmp {
    const RLookupKey **keys;
    size_t nkeys;
    uint64_t ascendMap;
  } fieldcmp;

  void ctor(size_t maxresults, const RLookupKey **keys, size_t nkeys, uint64_t ascmap);

  RPSorter(size_t maxresults, const RLookupKey **keys = NULL, size_t nkeys = 0, uint64_t ascmap = 0) :
    ResultProcessor("") {
    ctor(maxresults, keys, nkeys, ascmap);
  }

  ~RPSorter();

  int Next(SearchResult *r);
  int Accum(SearchResult *r);
  int innerLoop(SearchResult *r);
};

//---------------------------------------------------------------------------------------------

// Paging Processor
//
// The sorter builds a heap of size N, but the pager is responsible for taking result
// FIRST...FIRST+NUM from it.
// For example, if we want to get results 40-50, we build a heap of size 50 on the sorter, and
// the pager is responsible for discarding the first 40 results and returning just 10
// They are separated so that later on we can cache the sorter's heap, and continue paging it
// without re-executing the entire query

struct RPPager : public ResultProcessor {
  uint32_t offset;
  uint32_t limit;
  uint32_t count;

  RPPager(size_t offset, size_t limit);
  int Next(SearchResult *r);
};

///////////////////////////////////////////////////////////////////////////////////////////////
