
#include "aggregate/aggregate.h"
#include "redisearch.h"
#include "result_processor.h"
#include "aggregate/reducer.h"

#include "util/block_alloc.h"
#include "util/map.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define GROUPS_PER_BLOCK 1024

//---------------------------------------------------------------------------------------------

// Create a new group. groupvals is the key of the group. This will be the
// number of field arguments passed to GROUPBY, e.g.
// GROUPBY 2 @foo @bar will have a `groupvals` of `{"foo", "bar"}`.
// These will be placed in the output row.

Group::Group(Grouper &grouper, const arrayof(RSValue*) groupvals, size_t ngrpvals) {
  // Initialize the row data!
  for (size_t ii = 0; ii < ngrpvals; ++ii) {
    const RLookupKey *dstkey = grouper.dstkeys[ii];
    rowdata.WriteKey(dstkey, (RSValue *)groupvals[ii]);
  }
}

//---------------------------------------------------------------------------------------------

void Grouper::writeGroupValues(const Group *gr, SearchResult &r) const {
  for (auto dstkey: dstkeys) {
    RSValue *groupval = gr->rowdata.GetItem(dstkey);
    if (groupval) {
      r.rowdata.WriteKey(dstkey, groupval);
    }
  }
}

//---------------------------------------------------------------------------------------------

int Grouper::Yield(SearchResult &r) {
  while (iter != groups.end()) {
    if (!groups.contains(iter->first)) {
      iter++;
      continue;
    }

    Group *gr = iter->second;
    // no reducers; just a terminal GROUPBY...

    if (!numReducers()) {
      writeGroupValues(gr, r);
    }
    // else...
    for (auto rd: reducers) {
      RSValue *v = rd->Finalize();
      if (v) {
        r.rowdata.WriteOwnKey(rd->dstkey, v);
        writeGroupValues(gr, r);
      } else {
        throw Error("Couldn't get value");
      }
    }
    ++iter;
    return RS_RESULT_OK;
  }

  return RS_RESULT_EOF;
}

//---------------------------------------------------------------------------------------------

void Grouper::invokeGroupReducers(Group *gr, RLookupRow &srcrow) {
  for (auto reducer: reducers) {
    reducer->Add(&srcrow);
  }
}

//---------------------------------------------------------------------------------------------

/**
 * This function recursively descends into each value within a group and invokes
 * Add() for each cartesian product of the current row.
 *
 * @param g the grouper
 * @param xarr the array of 'x' values - i.e. the raw results received from the
 *  upstream result processor.
 * @param xpos the current position in xarr
 * @param xlen cached value of nkeys
 * @param ypos if xarr[xpos] is an array, this is the current position within
 *  the array
 * @param hval current X-wise hash value. Note that members of the same Y array
 *  are not hashed together.
 * @param res the row is passed to each reducer
 */

void Grouper::extractGroups(const RSValue **xarr, size_t xpos, size_t xlen, size_t arridx,
                            uint64_t hval, RLookupRow *res) {
  // end of the line - create/add to group
  if (xpos == xlen) {
    Group *group = NULL;

    // Get or create the group
    if (!groups.contains(hval)) {
      group = groupsAlloc.Alloc(Group(*this, xarr, xlen));
      groups.insert({ hval, group });
    } else {
      group = groups[hval];
    }

    // send the result to the group and its reducers
    invokeGroupReducers(group, *res);
    return;
  }

  // get the value
  const RSValue *v = xarr[xpos]->Dereference();
  // regular value - just move one step -- increment XPOS
  if (v->t != RSValue_Array) {
    hval = v->Hash(hval);
    extractGroups(xarr, xpos + 1, xlen, 0, hval, res);
  } else {
    // Array value. Replace current XPOS with child temporarily
    const RSValue *array = xarr[xpos];
    const RSValue *elem;

    if (arridx >= v->ArrayLen()) {
      elem = NULL;
    } else {
      elem = v->ArrayItem(arridx);
    }

    if (elem == NULL) {
      elem = RS_NullVal();
    }
    uint64_t hh = elem->Hash(hval);

    xarr[xpos] = elem;
    extractGroups(xarr, xpos, xlen, arridx, hh, res);
    xarr[xpos] = array;

    // Replace the value back, and proceed to the next value of the array
    if (++arridx < v->ArrayLen()) {
      extractGroups(xarr, xpos, xlen, arridx, hval, res);
    }
  }
}

//---------------------------------------------------------------------------------------------

void Grouper::invokeReducers(RLookupRow &srcrow) {
  uint64_t hval = 0;
  const RSValue *groupvals[srckeys.size()];  //@@ Change to vector?

  for (size_t i = 0; i < srckeys.size(); ++i) {
    const RLookupKey *srckey = srckeys[i];
    RSValue *v = srcrow.GetItem(srckey);
    if (v == NULL) {
      v = RS_NullVal();
    }
    groupvals[i] = v;
  }
  extractGroups(groupvals, 0, srckeys.size(), 0, 0, &srcrow);
}

//---------------------------------------------------------------------------------------------

int Grouper::Next(SearchResult *res) {
  if (_yield) return Yield(*res);

  int rc;
  while ((rc = upstream->Next(res)) == RS_RESULT_OK) {
    invokeReducers(res->rowdata);
    res->Clear();
  }
  if (rc != RS_RESULT_EOF) return rc;

  _yield = true;
  parent->totalResults = groups.size();
  return Yield(*res);
}

//---------------------------------------------------------------------------------------------

static void cleanCallback(void *ptr, void *arg) {
  Grouper *parent = arg;
  parent->reducers.clear();
}

//---------------------------------------------------------------------------------------------

Grouper::~Grouper() {
  groupsAlloc.Clear();
}

//---------------------------------------------------------------------------------------------

/**
 * Creates a new grouper object. This is equivalent to a GROUPBY clause.
 * A `Grouper` object contains at the minimum, the keys on which it groups
 * (indicated by the srckeys) and the keys on which it outputs (indicated by
 * dstkeys).
 *
 * The Grouper will create a new group for each unique cartesian of values found
 * in srckeys within each row, and invoke associated Reducers (can be added via
 * @ref Grouper_AddReducer()) within that context.
 *
 * The srckeys and dstkeys parameters are mirror images of one another, but are
 * necessary because a reducer function will convert and reduce one or more
 * source rows into a single destination row. The srckeys are the values to
 * group by within the source rows, and the dstkeys are the values as they are
 * stored within the destination rows. It is assumed that two RLookups are used
 * like so:
 *
 * @code {.c}
 * RLookup lksrc;
 * RLookup lkdst;
 * const char *kname[] = {"foo", "bar", "baz"};
 * RLookupKey *srckeys[3];
 * RLookupKey *dstkeys[3];
 * for (size_t ii = 0; ii < 3; ++ii) {
 *  srckeys[ii] = lksrc.GetKey(kname[ii], RLOOKUP_F_OCREAT);
 *  dstkeys[ii] = lkdst.GetKey(kname[ii], RLOOKUP_F_OCREAT);
 * }
 * @endcode
 *
 * ResultProcessors (and a grouper is a ResultProcessor) before the grouper
 * should write their data using `lksrc` as a reference point.
 */

Grouper::Grouper(const RLookupKey **srckeys_, const RLookupKey **dstkeys_, size_t nkeys) : ResultProcessor("Grouper"),
    groupsAlloc(GROUPS_PER_BLOCK) {
  srckeys.reserve(nkeys);
  dstkeys.reserve(nkeys);
  for (size_t i = 0; i < nkeys; ++i) {
    srckeys.push_back(srckeys_[i]);
    dstkeys.push_back(dstkeys_[i]);
  }
  _yield = false;
}

//---------------------------------------------------------------------------------------------

// Adds a reducer to the grouper. This must be called before any results are
// processed by the grouper.

void Grouper::AddReducer(Reducer *r, RLookupKey *dstkey) {
  r->dstkey = dstkey;
  reducers.push_back(r);
}

//---------------------------------------------------------------------------------------------

// Gets the result processor associated with the grouper.
// This is used for building the query pipeline

ResultProcessor *Grouper::GetRP() {
  return this;
}

//---------------------------------------------------------------------------------------------

size_t Grouper::numReducers() const {
  return reducers.size();
}

///////////////////////////////////////////////////////////////////////////////////////////////
