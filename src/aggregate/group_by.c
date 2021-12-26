
#pragma once

#include "redisearch.h"
#include "result_processor.h"
#include "reducer.h"

#include "util/block_alloc.h"
#include "util/khash.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define kh_set(kname, hash, key, val)   \
  ({                                    \
    int ret;                            \
    k = kh_put(kname, hash, key, &ret); \
    kh_value(hash, k) = val;            \
    ret;                                \
  })

static const int khid = 33;
//KHASH_MAP_INIT_INT64(khid, Group *);

#define GROUPER_NREDUCERS(g) (array_len((g)->reducers))
#define GROUP_BYTESIZE(parent) (sizeof(Group) + (sizeof(void *) * GROUPER_NREDUCERS(parent)))
#define GROUPS_PER_BLOCK 1024
#define GROUPER_NSRCKEYS(g) ((g)->nkeys)

//---------------------------------------------------------------------------------------------

// Create a new group. groupvals is the key of the group. This will be the
// number of field arguments passed to GROUPBY, e.g.
// GROUPBY 2 @foo @bar will have a `groupvals` of `{"foo", "bar"}`.
//
// These will be placed in the output row.

Group::Group(Grouper &grouper, const RSValue **groupvals, size_t ngrpvals) {
  size_t numReducers = array_len(grouper.reducers);
  size_t elemSize = GROUP_BYTESIZE(&grouper);
  Group *group = groupsAlloc.Alloc(elemSize, GROUPS_PER_BLOCK * elemSize);
  memset(group, 0, elemSize);

  for (size_t ii = 0; ii < grouper.numReducers; ++ii) {
    accumdata[ii] = grouper.reducers[ii]->NewInstance(grouper.reducers[ii]);
  }

  // Initialize the row data!
  for (size_t ii = 0; ii < ngrpvals; ++ii) {
    const RLookupKey *dstkey = dstkeys[ii];
    RLookup_WriteKey(dstkey, &rowdata, (RSValue *)grouper.groupvals[ii]);
    // printf("Write: %s => ", dstkey->name);
    // RSValue_Print(groupvals[ii]);
    // printf("\n");
  }
}

//---------------------------------------------------------------------------------------------

void Grouper::writeGroupValues(const Group *gr, SearchResult *r) const {
  for (size_t ii = 0; ii < nkeys; ++ii) {
    const RLookupKey *dstkey = dstkeys[ii];
    RSValue *groupval = RLookup_GetItem(dstkey, &gr->rowdata);
    if (groupval) {
      RLookup_WriteKey(dstkey, &r->rowdata, groupval);
    }
  }
}

//---------------------------------------------------------------------------------------------

int Grouper::rpYield(SearchResult *r) {
  while (g->iter != kh_end(groups)) {
    if (!kh_exist(groups, iter)) {
      iter++;
      continue;
    }

    Group *gr = kh_value(groups, iter);
    // no reducers; just a terminal GROUPBY...

    if (!GROUPER_NREDUCERS(this)) {
      writeGroupValues(gr, r);
    }
    // else...
    for (size_t ii = 0; ii < GROUPER_NREDUCERS(this); ++ii) {
      Reducer *rd = reducers[ii];
      RSValue *v = rd->Finalize(rd, gr->accumdata[ii]);
      if (v) {
        RLookup_WriteOwnKey(rd->dstkey, &r->rowdata, v);
        writeGroupValues(gr, r);
      } else {
        // FIXME!
        // Error! Couldn't get value? Handle me here!
        // printf("Finalize() returned bad value!\n");
      }
    }
    ++iter;
    return RS_RESULT_OK;
  }

  return RS_RESULT_EOF;
}

//---------------------------------------------------------------------------------------------

void Grouper::invokeReducers(Group *gr, RLookupRow *srcrow) {
  size_t nreducers = GROUPER_NREDUCERS(this);
  for (size_t ii = 0; ii < nreducers; ii++) {
    reducers[ii]->Add(reducers[ii], gr->accumdata[ii], srcrow);
  }
}

//---------------------------------------------------------------------------------------------

/**
 * This function recursively descends into each value within a group and invokes
 * Add() for each cartesian product of the current row.
 *
 * @param g the grouper
 * @param xarr the array of 'x' values - i.e. the raw results received from the
 *  upstream result processor. The number of results can be found via
 *  the `GROUPER_NSRCKEYS(g)` macro
 * @param xpos the current position in xarr
 * @param xlen cached value of GROUPER_NSRCKEYS
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
    khiter_t k = kh_get(khid, groups, hval);  // first have to get ieter
    if (k == kh_end(groups)) {                // k will be equal to kh_end if key not present
      group = createGroup(xarr, xlen);
      kh_set(khid, groups, hval, group);
    } else {
      group = kh_value(groups, k);
    }

    // send the result to the group and its reducers
    invokeReducers(group, res);
    return;
  }

  // get the value
  const RSValue *v = xarr[xpos]->Dereference();
  // regular value - just move one step -- increment XPOS
  if (v->t != RSValue_Array) {
    hval = RSValue_Hash(v, hval);
    extractGroups(xarr, xpos + 1, xlen, 0, hval, res);
  } else {
    // Array value. Replace current XPOS with child temporarily
    const RSValue *array = xarr[xpos];
    const RSValue *elem;

    if (arridx >= RSValue_ArrayLen(v)) {
      elem = NULL;
    } else {
      elem = RSValue_ArrayItem(v, arridx);
    }

    if (elem == NULL) {
      elem = RS_NullVal();
    }
    uint64_t hh = RSValue_Hash(elem, hval);

    xarr[xpos] = elem;
    extractGroups(xarr, xpos, xlen, arridx, hh, res);
    xarr[xpos] = array;

    // Replace the value back, and proceed to the next value of the array
    if (++arridx < RSValue_ArrayLen(v)) {
      extractGroups(xarr, xpos, xlen, arridx, hval, res);
    }
  }
}

//---------------------------------------------------------------------------------------------

void Group::invokeReducers(RLookupRow *srcrow) {
  uint64_t hval = 0;
  size_t nkeys = GROUPER_NSRCKEYS(this);
  const RSValue *groupvals[nkeys];

  for (size_t ii = 0; ii < nkeys; ++ii) {
    const RLookupKey *srckey = srckeys[ii];
    RSValue *v = RLookup_GetItem(srckey, srcrow);
    if (v == NULL) {
      v = RS_NullVal();
    }
    groupvals[ii] = v;
  }
  extractGroups(groupvals, 0, nkeys, 0, 0, srcrow);
}

//---------------------------------------------------------------------------------------------

int Grouper::Next(SearchResult *res) {
  int rc;
  while ((rc = upstream->Next(upstream, res)) == RS_RESULT_OK) {
    invokeGroupReducers(&res->rowdata);
    res->Clear();
  }
  if (rc != RS_RESULT_EOF) {
    return rc;
  }
  base->Next = Grouper_rpYield;
  parent->totalResults = kh_size(groups);
  iter = kh_begin(khid);
  return rpYield(res);
}

//---------------------------------------------------------------------------------------------

static void cleanCallback(void *ptr, void *arg) {
  Group *group = ptr;
  Grouper *parent = arg;
  // Call the reducer's FreeInstance
  for (size_t ii = 0; ii < GROUPER_NREDUCERS(parent); ++ii) {
    Reducer *rr = parent->reducers[ii];
    if (rr->FreeInstance) {
      rr->FreeInstance(rr, group->accumdata[ii]);
    }
  }
}

//---------------------------------------------------------------------------------------------

Grouper::~Grouper() {
  for (khiter_t it = kh_begin(groups); it != kh_end(groups); ++it) {
    if (!kh_exist(groups, it)) {
      continue;
    }
    Group *gr = kh_value(groups, it);
    RLookupRow_Cleanup(&gr->rowdata);
  }
  kh_destroy(khid, groups);
  BlkAlloc_FreeAll(&groupsAlloc, cleanCallback, g, GROUP_BYTESIZE(g));

  for (size_t i = 0; i < GROUPER_NREDUCERS(this); i++) {
    delete reducers[i];
  }
  if (reducers) {
    array_free(reducers);
  }
  rm_free(srckeys);
  rm_free(dstkeys);
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
 *  srckeys[ii] = RLookup_GetKey(&lksrc, kname[ii], RLOOKUP_F_OCREAT);
 *  dstkeys[ii] = RLookup_GetKey(&lkdst, kname[ii], RLOOKUP_F_OCREAT);
 * }
 * @endcode
 *
 * ResultProcessors (and a grouper is a ResultProcessor) before the grouper
 * should write their data using `lksrc` as a reference point.
 */

Grouper::Grouper(const RLookupKey **srckeys_, const RLookupKey **dstkeys, size_t nkeys) {
  BlkAlloc_Init(&g->groupsAlloc);
  groups = kh_init(khid);

  srckeys = rm_calloc(nkeys, sizeof(*srckeys));
  dstkeys = rm_calloc(nkeys, sizeof(*dstkeys));
  nkeys = nkeys;
  for (size_t ii = 0; ii < nkeys; ++ii) {
    srckeys[ii] = srckeys_[ii];
    dstkeys[ii] = dstkeys_[ii];
  }

  name = "Grouper";
}

//---------------------------------------------------------------------------------------------

// Adds a reducer to the grouper. This must be called before any results are
// processed by the grouper.

void Grouper::AddReducer(Reducer *r, RLookupKey *dstkey) {
  Reducer **rpp = array_ensure_tail(&reducers, Reducer*);
  *rpp = r;
  r->dstkey = dstkey;
}

//---------------------------------------------------------------------------------------------

// Gets the result processor associated with the grouper.
// This is used for building the query pipeline

ResultProcessor *Grouper::GetRP() {
  return this;
}

///////////////////////////////////////////////////////////////////////////////////////////////
