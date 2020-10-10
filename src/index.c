
#include "index.h"
#include "varint.h"
#include "spec.h"
#include "object.h"

#include <math.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator::~IndexIterator() {
  if (current) {
    IndexResult_Free(current);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

class UnionIterator : public IndexIterator {
public:
  UnionIterator(IndexIterator **its, int num_, DocTable *dt, int quickExit, double weight_);
  ~UnionIterator();

  // We maintain two iterator arrays. One is the original iterator list, and
  // the other is the list of currently active iterators. When an iterator
  // reaches EOF, it is set to NULL in the `its` list, but is still retained in
  // the `origits` list, for the purpose of supporting things like Rewind() and Free().

  IndexIterator **its;
  IndexIterator **origits;
  uint32_t num;
  uint32_t norig;
  uint32_t currIt;
  t_docId minDocId;

  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
  size_t nexpected;
  double weight;
  uint64_t len;

  void SyncIterList();
  size_t RemoveExhausted(size_t badix);
  t_docId LastDocId() const;

  int (UnionIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len();

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }
};

//---------------------------------------------------------------------------------------------

t_docId UnionIterator::LastDocId() const {
  return minDocId;
}

void UnionIterator::SyncIterList() {
  num = norig;
  memcpy(its, origits, sizeof(*its) * norig);
  for (size_t ii = 0; ii < num; ++ii) {
    its[ii]->minId = 0;
  }
}

// Removes the exhausted iterator from the active list, so that future
// reads will no longer iterate over it

size_t UnionIterator::RemoveExhausted(size_t badix) {
  // e.g. assume we have 10 entries, and we want to remove index 8, which means
  // one more valid entry at the end. This means we use
  // source: its + 8 + 1
  // destination: its + 8
  // number: it->len (10) - (8) - 1 == 1
  memmove(its + badix, its + badix + 1, sizeof(*its) * (num - badix - 1));
  num--;
  // Repeat the same index again, because we have a new iterator at the same position
  return badix - 1;
}

void UnionIterator::Abort() {
  IITER_SET_EOF(this);
  for (int i = 0; i < num; i++) {
    if (its[i]) {
      its[i]->Abort();
    }
  }
}

void UnionIterator::Rewind() {
  IITER_CLEAR_EOF(this);
  minDocId = 0;
  current->docId = 0;

  SyncIterList();

  // rewind all child iterators
  for (size_t i = 0; i < num; i++) {
    its[i]->minId = 0;
    its[i]->Rewind();
  }
}

//---------------------------------------------------------------------------------------------

UnionIterator::UnionIterator(IndexIterator **its, int num_, DocTable *dt, int quickExit, double weight_) {
  origits = its;
  weight = weight_;
  num = num_;
  norig = num;
  IITER_CLEAR_EOF(this);
  current = new UnionResult(num, weight);
  len = 0;
  quickExit = quickExit;
  its = rm_calloc(num, sizeof(*its));
  nexpected = 0;
  currIt = 0;

  mode = Mode::Sorted;
  _Read = &UnionIterator::ReadSorted;

  SyncIterList();

  //@@ caveat here
  for (size_t i = 0; i < num; ++i) {
    nexpected += its[i]->NumEstimated();
    if (its[i]->mode == Mode::Unsorted) {
      mode = Mode::Unsorted;
      _Read = &UnionIterator::ReadUnsorted;
    }
  }

  const size_t maxresultsSorted = RSGlobalConfig.maxResultsToUnsortedMode;
  // this code is normally (and should be) dead.
  // i.e. the deepest-most IndexIterator does not have a CT
  //      so it will always eventually return NULL CT
  if (mode == Mode::Sorted && nexpected >= maxresultsSorted) {
    // make sure all the children support CriteriaTester
    bool ctSupported = true;
    for (int i = 0; i < num; ++i) {
      IndexCriteriaTester *tester = origits[i]->GetCriteriaTester();
      if (!tester) {
        ctSupported = false;
        break;
      }
      delete tester;
    }
    if (ctSupported) {
      mode = IndexIteratorMode::Unsorted;
      _Read = &UnionIterator::ReadUnsorted;
    }
  }
}

//---------------------------------------------------------------------------------------------

class UnionCriteriaTester : public IndexCriteriaTester {
public:
  UnionCriteriaTester(IndexCriteriaTester **testers, int ntesters);
  ~UnionCriteriaTester();

  IndexCriteriaTester **children;
  int nchildren;

  int Test(t_docId id);
};

int UnionCriteriaTester::Test(t_docId id) {
  for (int i = 0; i < nchildren; ++i) {
    if (children[i]->Test(id)) {
      return 1;
    }
  }
  return 0;
}

UnionCriteriaTester::~UnionCriteriaTester() {
  for (int i = 0; i < nchildren; ++i) {
    if (children[i]) {
      delete children[i];
    }
  }
  rm_free(children);
}

IndexCriteriaTester *UnionIterator::GetCriteriaTester() {
  IndexCriteriaTester **testers = rm_calloc(num, sizeof(IndexCriteriaTester *));
  for (size_t i = 0; i < num; ++i) {
    IndexCriteriaTester *tester = origits[i]->GetCriteriaTester();
    if (!tester) {
      for (size_t j = 0; j < i; j++) {
        delete testers[j];
      }
      rm_free(testers);
      return NULL;
    }
	  testers[i] = tester;
  }
  return new UnionCriteriaTester(testers, num);
}

UnionCriteriaTester::UnionCriteriaTester(IndexCriteriaTester **testers, int ntesters) {
  children = testers;
  nchildren = ntesters;
}

//---------------------------------------------------------------------------------------------

size_t UnionIterator::NumEstimated() {
  return nexpected;
}

int UnionIterator::ReadUnsorted(RSIndexResult **hit) {
  RSIndexResult *res = NULL;
  while (currIt < num) {
    int rc = origits[currIt]->Read(&res);
    if (rc == INDEXREAD_OK) {
      *hit = res;
      return rc;
    }
    ++currIt;
  }
  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

int UnionIterator::ReadSorted(RSIndexResult **hit) {
  // nothing to do
  if (num == 0 || !IITER_HAS_NEXT(this)) {
    IITER_SET_EOF(this);
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  result().Reset();

  do {
    // find the minimal iterator
    t_docId minDocId = UINT32_MAX;
    IndexIterator *minIt = NULL;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    unsigned nits = num;

    for (unsigned i = 0; i < nits; i++) {
      IndexIterator *it = its[i];
      RSIndexResult *res = IITER_CURRENT_RECORD(it);
      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      // printf("docIds[%d]: %d, minDocId: %d\n", i, docIds[i], minDocId);
      while (it->minId <= minDocId && rc != INDEXREAD_EOF) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          rc = it->Read(&res);
          if (res) {
            it->minId = res->docId;
          }
        }
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      } else {
        // Remove this from the active list
        i = RemoveExhausted(i);
        nits = num;
        continue;
      }

      if (rc == INDEXREAD_OK && res->docId <= minDocId) {
        minDocId = res->docId;
        minIt = it;
      }
    }

    // take the minimum entry and collect all results matching to it
    if (minIt) {
      SkipTo(minIt->minId, hit);
      // return INDEXREAD_OK;
      minDocId = minIt->minId;
      len++;
      return INDEXREAD_OK;
    }

  } while (numActive > 0);
  IITER_SET_EOF(this);

  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF if at EOF
*/

int UnionIterator::SkipTo(t_docId docId, RSIndexResult **hit) {
  RS_LOG_ASSERT(mode != Mode::Sorted, "union iterator mode is not MODE_SORTED");

  // printf("UI %p skipto %d\n", this, docId);

  if (docId == 0) {
    return ReadSorted(hit);
  }

  if (!IITER_HAS_NEXT(this)) {
    return INDEXREAD_EOF;
  }

  // reset the current hitf
  result().Reset();
  result().weight = weight;
  int numActive = 0;
  int found = 0;
  int rc = INDEXREAD_EOF;
  const int quickExit = quickExit;
  t_docId minDocId1 = UINT32_MAX;
  IndexIterator *it;
  RSIndexResult *res;
  RSIndexResult *minResult = NULL;
  // skip all iterators to docId
  unsigned xnum = num;
  for (unsigned i = 0; i < xnum; i++) {
    it = its[i];
    // this happens for non existent words
    res = NULL;
    // If the requested docId is larger than the last read id from the iterator,
    // we need to read an entry from the iterator, seeking to this docId
    if (it->minId < docId) {
      if ((rc = it->SkipTo(docId, &res)) == INDEXREAD_EOF) {
        i = RemoveExhausted(i);
        xnum = num;
        continue;
      }
      if (res) {
        it->minId = res->docId;
      }

    } else {
      // if the iterator is ahead of docId - we avoid reading the entry
      // in this case, we are either past or at the requested docId, no need to actually read
      rc = it->minId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
      res = IITER_CURRENT_RECORD(it);
    }

    // if we've read successfully, update the minimal docId we've found
    if (it->minId && rc != INDEXREAD_EOF) {
      if (it->minId < minDocId1 || !minResult) {
        minResult = res;
        minDocId1 = it->minId;
      }
      // sminDocId = MIN(docIds[i], minDocId);
    }

    // we found a hit - continue to all results matching the same docId
    if (rc == INDEXREAD_OK) {

      // add the result to the aggregate result we are holding
      if (hit) {
        result().AddChild(res ? res : IITER_CURRENT_RECORD(it));
      }
      minDocId1 = it->minId;
      ++found;
    }
    ++numActive;
    // If we've found a single entry and we are iterating in quick exit mode - exit now
    if (found && quickExit) break;
  }

  // all iterators are at the end
  if (numActive == 0) {
    IITER_SET_EOF(this);
    return INDEXREAD_EOF;
  }

  // copy our aggregate to the upstream hit
  *hit = current;
  if (found > 0) {
    return INDEXREAD_OK;
  }
  if (minResult) {
    *hit = minResult;
    result().AddChild(minResult);
  }
  // not found...
  minDocId = minDocId1;
  return INDEXREAD_NOTFOUND;
}

//---------------------------------------------------------------------------------------------

UnionIterator::~UnionIterator() {
  for (int i = 0; i < norig; i++) {
    IndexIterator *it = origits[i];
    if (it) {
      delete it;
    }
  }

  rm_free(its);
  rm_free(origits);
}

//---------------------------------------------------------------------------------------------

size_t UnionIterator::Len() {
  return len;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Context used by intersection methods during iterating an intersect iterator

class IntersectIterator : public IndexIterator {
public:
  IntersectIterator(IndexIterator **its_, size_t num_, DocTable *dt, t_fieldMask fieldMask_,
                    int maxSlop_, int inOrder_, double weight_);
  ~IntersectIterator();

  IndexIterator **its;
  IndexIterator *bestIt;
  IndexCriteriaTester **testers;
  t_docId *docIds;
  int *rcs;
  unsigned num;
  size_t len;
  int maxSlop;
  int inOrder;
  t_docId lastDocId; // last read docId from any child
  t_docId lastFoundId; // last id that was found on all children

  DocTable *docTable;
  t_fieldMask fieldMask;
  double weight;
  size_t nexpected;

  int (IntersectIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  void SortChildren();
  t_docId LastDocId();

  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual void Abort();
  virtual void Rewind();
  virtual size_t NumEstimated();
  virtual size_t Len();

  AggregateResult &result() { return reinterpret_cast<AggregateResult&>(*current); }
};

//---------------------------------------------------------------------------------------------

IntersectIterator::~IntersectIterator() {
  for (int i = 0; i < num; i++) {
    if (its[i] != NULL) {
      delete its[i];
    }
    // IndexResult_Free(&ui->currentHits[i]);
  }

  for (int i = 0; i < array_len(testers); i++) {
    if (testers[i]) {
      delete testers[i];
    }
  }
  if (bestIt) {
    delete bestIt;
  }

  rm_free(docIds);
  rm_free(its);
  //IndexResult_Free(current);
  array_free(testers);
}

//---------------------------------------------------------------------------------------------

void IntersectIterator::Abort() {
  isValid = 0;
  for (int i = 0; i < num; i++) {
    if (its[i]) {
      its[i]->Abort();
    }
  }
}

void IntersectIterator::Rewind() {
  isValid = 1;
  lastDocId = 0;

  // rewind all child iterators
  for (int i = 0; i < num; i++) {
    docIds[i] = 0;
    if (its[i]) {
      its[i]->Rewind();
    }
  }
}

//---------------------------------------------------------------------------------------------

void IntersectIterator::SortChildren() {

  // 1. Go through all the iterators, ensuring none of them is NULL
  //    (replace with empty if indeed NULL)
  // 2. If all the iterators are unsorted then set the mode to UNSORTED
  // 3. If all or any of the iterators are sorted, then remove the
  //    unsorted iterators from the equation, simply adding them to the tester list

  IndexIterator **unsortedIts = NULL;
  IndexIterator **sortedIts = rm_malloc(sizeof(IndexIterator *) * num);
  size_t sortedItsSize = 0;
  for (size_t i = 0; i < num; ++i) {
    IndexIterator *curit = its[i];

    if (!curit) {
      // If the current iterator is empty, then the entire
      // query will fail; just free all the iterators and call it good
      if (sortedIts) {
        rm_free(sortedIts);
      }
      if (unsortedIts) {
        array_free(unsortedIts);
      }
      bestIt = NULL;
      return;
    }

    size_t amount = curit->NumEstimated();
    if (amount < nexpected) {
      nexpected = amount;
      bestIt = curit;
    }

    if (curit->mode == Mode::Unsorted) {
      unsortedIts = array_ensure_append(unsortedIts, &curit, 1, IndexIterator *);
    } else {
      sortedIts[sortedItsSize++] = curit;
    }
  }

  if (unsortedIts) {
    if (array_len(unsortedIts) == num) {
      mode = Mode::Unsorted;
      _Read = &IntersectIterator::ReadUnsorted;
      num = 1;
      its[0] = bestIt;
      // The other iterators are also stored in unsortedIts
      // and because we know that there are no sorted iterators
    }

    for (size_t ii = 0; ii < array_len(unsortedIts); ++ii) {
      IndexIterator *cur = unsortedIts[ii];
      if (mode == Mode::Unsorted && bestIt == cur) {
        continue;
      }
      IndexCriteriaTester *tester = cur->GetCriteriaTester();
      testers = array_ensure_append(testers, &tester, 1, IndexCriteriaTester *);
      delete cur;
    }
  } else {
    bestIt = NULL;
  }

  rm_free(its);
  its = sortedIts;
  num = sortedItsSize;
  array_free(unsortedIts);
}

//---------------------------------------------------------------------------------------------

IntersectIterator::IntersectIterator(IndexIterator **its_, size_t num_, DocTable *dt,
                                     t_fieldMask fieldMask_, int maxSlop_, int inOrder_, double weight_) {
  // printf("Creating new intersection iterator with fieldMask=%llx\n", fieldMask);
  IntersectIterator *ctx = rm_calloc(1, sizeof(*ctx));
  lastDocId = 0;
  lastFoundId = 0;
  len = 0;
  maxSlop = maxSlop_;
  inOrder = inOrder_;
  fieldMask = fieldMask_;
  weight = weight_;
  docIds = rm_calloc(num_, sizeof(t_docId));
  docTable = dt;
  nexpected = UINT32_MAX;

  isValid = 1;
  current = NewIntersectResult(num, weight);
  its = its_;
  num = num_;

  SortChildren();
}

//---------------------------------------------------------------------------------------------

int IntersectIterator::SkipTo(t_docId docId, RSIndexResult **hit) {
  // A seek with docId 0 is equivalent to a read
  if (docId == 0) {
    return ReadSorted(hit);
  }

  result().Reset();
  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < num; i++) {
    IndexIterator *it = its[i];

    if (!it || !IITER_HAS_NEXT(it)) return INDEXREAD_EOF;

    RSIndexResult *res = IITER_CURRENT_RECORD(it);
    rc = INDEXREAD_OK;

    // only read if we are not already at the seek to position
    if (docIds[i] != docId) {
      rc = it->SkipTo(docId, &res);
      if (rc != INDEXREAD_EOF) {
        if (res) docIds[i] = res->docId;
      }
    }

    if (rc == INDEXREAD_EOF) {
      // we are at the end!
      isValid = 0;
      return rc;
    } else if (rc == INDEXREAD_OK) {
      // YAY! found!
      result().AddChild(res);
      lastDocId = docId;

      ++nfound;
    } else if (docIds[i] > lastDocId) {
      lastDocId = docIds[i];
      break;
    }
  }

  // unless we got an EOF - we put the current record into hit

  // if the requested id was found on all children - we return OK
  if (nfound == num) {
    // printf("Skipto %d hit @%d\n", docId, current->docId);

    // Update the last found id
    // if maxSlop == -1 there is no need to verify maxSlop and inorder, otherwise lets verify
    if (maxSlop == -1 ||
        IndexResult_IsWithinRange(current, maxSlop, inOrder)) {
      lastFoundId = current->docId;
      if (hit) *hit = current;
      return INDEXREAD_OK;
    }
  }

  // Not found - but we need to read the next valid result into hit
  rc = ReadSorted(hit);
  // this might have brought us to our end, in which case we just terminate
  if (rc == INDEXREAD_EOF) return INDEXREAD_EOF;

  // otherwise - not found
  return INDEXREAD_NOTFOUND;
}

//---------------------------------------------------------------------------------------------

int IntersectIterator::ReadUnsorted(RSIndexResult **hit) {
  int rc = INDEXREAD_OK;
  RSIndexResult *res = NULL;
  while (1) {
    rc = bestIt->Read(&res);
    if (rc == INDEXREAD_EOF) {
      return INDEXREAD_EOF;
      *hit = res;
      return rc;
    }
    int isMatch = 1;
    for (size_t i = 0; i < array_len(testers); ++i) {
      if (!testers[i]->Test(res->docId)) {
        isMatch = 0;
        break;
      }
    }
    if (!isMatch) {
      continue;
    }
    *hit = res;
    return rc;
  }
}

//---------------------------------------------------------------------------------------------

class IICriteriaTester : public IndexCriteriaTester {
public:
  IICriteriaTester(IndexCriteriaTester **testers);
  ~IICriteriaTester();

  IndexCriteriaTester **children;

  int Test(t_docId id);
};

IICriteriaTester::IICriteriaTester(IndexCriteriaTester **testers) {
  children = testers;
}

int IICriteriaTester::Test(t_docId id) {
  for (size_t i = 0; i < array_len(children); ++i) {
    if (!children[i]->Test(id)) {
      return 0;
    }
  }
  return 1;
}

IICriteriaTester::~IICriteriaTester() {
  for (size_t i = 0; i < array_len(children); ++i) {
    delete children[i];
  }
  array_free(children);
}

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *IntersectIterator::GetCriteriaTester() {
  for (size_t i = 0; i < num; ++i) {
    IndexCriteriaTester *tester = NULL;
    if (its[i]) {
      tester = its[i]->GetCriteriaTester();
    }
    if (!tester) {
      for (int j = 0; j < i; j++) {
        delete testers[j];
      }
      array_free(testers);
      return NULL;
    }
    testers = array_ensure_append(testers, tester, 1, IndexCriteriaTester *);
  }
  IICriteriaTester *it = new IICriteriaTester(testers);
  testers = NULL;
  return it;
}

//---------------------------------------------------------------------------------------------

size_t IntersectIterator::NumEstimated() {
  return nexpected;
}

int IntersectIterator::ReadSorted(RSIndexResult **hit) {
  if (num == 0) return INDEXREAD_EOF;

  int nh = 0;
  int i = 0;

  do {
    nh = 0;
    result().Reset();

    for (i = 0; i < num; i++) {
      IndexIterator *it = its[i];

      if (!it) goto eof;

      RSIndexResult *h = IITER_CURRENT_RECORD(it);
      // skip to the next
      int rc = INDEXREAD_OK;
      if (docIds[i] != lastDocId || lastDocId == 0) {

        if (i == 0 && docIds[i] >= lastDocId) {
          rc = it->Read(&h);
        } else {
          rc = it->SkipTo(lastDocId, &h);
        }
        // printf("II %p last docId %d, it %d read docId %d(%d), rc %d\n", ic, lastDocId, i, h->docId, it->LastDocId(), rc);

        if (rc == INDEXREAD_EOF) goto eof;
        docIds[i] = h->docId;
      }

      if (docIds[i] > lastDocId) {
        lastDocId = docIds[i];
        break;
      }
      if (rc == INDEXREAD_OK) {
        ++nh;
        result().AddChild(h);
      } else {
        lastDocId++;
      }
    }

    if (nh == num) {
      // printf("II %p HIT @ %d\n", this, current->docId);
      // sum up all hits
      if (hit != NULL) {
        *hit = current;
      }
      // Update the last valid found id
      lastFoundId = current->docId;

      // advance the doc id so next time we'll read a new record
      lastDocId++;

      // // make sure the flags are matching.
      if ((current->fieldMask & fieldMask) == 0) {
        // printf("Field masks don't match!\n");
        continue;
      }

      // If we need to match slop and order, we do it now, and possibly skip the result
      if (maxSlop >= 0) {
        // printf("Checking SLOP... (%d)\n", maxSlop);
        if (!current->IsWithinRange(maxSlop, inOrder)) {
          // printf("Not within range!\n");
          continue;
        }
      }

      len++;
      // printf("Returning OK\n");
      return INDEXREAD_OK;
    }
  } while (1);
eof:
  isValid = 0;
  return INDEXREAD_EOF;
}

t_docId IntersectIterator::LastDocId() {
  // return last FOUND id, not last read id form any child
  return lastFoundId;
}

size_t IntersectIterator::Len() {
  return len;
}

///////////////////////////////////////////////////////////////////////////////////////////////

// A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for hits

class NotIterator : public IndexIterator {
public:
  NotIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~NotIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  t_docId lastDocId;
  t_docId maxDocId;
  size_t len;
  double weight;

  int (NotIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual void Abort();
  virtual void Rewind();
  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual size_t NumEstimated();
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t Len();
  virtual int HasNext();

  t_docId LastDocId();
};

typedef NotIterator NotContext;

void NotIterator::Abort() {
  if (child) {
    child->Abort();
  }
}

void NotIterator::Rewind() {
  lastDocId = 0;
  current->docId = 0;
  if (child) {
    child->Rewind();
  }
}

NotIterator::~NotIterator() {
  if (child) {
    delete child;
  }
  if (childCT) {
    delete childCT;
  }
  IndexResult_Free(current);
}

// If we have a match - return NOTFOUND. If we don't or we're at the end - return OK

int NotIterator::SkipTo(t_docId docId, RSIndexResult **hit) {
  // do not skip beyond max doc id
  if (docId > maxDocId) {
    return INDEXREAD_EOF;
  }
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!child) {
    goto ok;
  }

  // Get the child's last read docId
  t_docId childId = child->LastDocId();

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (childId > docId) {
    goto ok;
  }

  // If the child docId is the one we are looking for, it's an anti match!
  if (childId == docId) {
    current->docId = docId;
    lastDocId = docId;
    *hit = current;
    return INDEXREAD_NOTFOUND;
  }

  // read the next entry from the child
  int rc = child->SkipTo(docId, hit);

  // OK means not found
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }

ok:
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  current->docId = docId;
  lastDocId = docId;
  *hit = current;
  return INDEXREAD_OK;
}

//---------------------------------------------------------------------------------------------

class NI_CriteriaTester : public IndexCriteriaTester {
public:
  NI_CriteriaTester(IndexCriteriaTester *childTester);
  ~NI_CriteriaTester();

  IndexCriteriaTester *child;

  int Test(t_docId id);
};

NI_CriteriaTester::NI_CriteriaTester(IndexCriteriaTester *childTester) {
  child = childTester; //@@ ownership?
}

int NI_CriteriaTester::Test(t_docId id) {
  return !child->Test(id);
}

NI_CriteriaTester::~NI_CriteriaTester() {
  delete child; //@@ ownership?
}

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *NotIterator::GetCriteriaTester() {
  if (!child) {
    return NULL;
  }
  IndexCriteriaTester *ct = child->GetCriteriaTester();
  if (!ct) {
    return NULL;
  }
  return new NI_CriteriaTester(ct);
}

//---------------------------------------------------------------------------------------------

size_t NotIterator::NumEstimated() {
  return maxDocId;
}

int NotIterator::ReadUnsorted(RSIndexResult **hit) {
  while (lastDocId > maxDocId) {
    if (!childCT->Test(lastDocId)) {
      current->docId = lastDocId;
      *hit = current;
      ++lastDocId;
      return INDEXREAD_OK;
    }
    ++lastDocId;
  }
  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

// Read from a NOT iterator.
// This is applicable only if the only or leftmost node of a query is a NOT node. 
// We simply read until max docId, skipping docIds that exist in the child.

int NotIterator::ReadSorted(RSIndexResult **hit) {
  if (lastDocId > maxDocId) return INDEXREAD_EOF;

  RSIndexResult *cr = NULL;
  // if we have a child, get the latest result from the child
  if (child) {
    cr = IITER_CURRENT_RECORD(child);

    if (cr == NULL || cr->docId == 0) {
      child->Read(&cr);
    }
  }

  // advance our reader by one, and let's test if it's a valid value or not
  current->docId++;

  // If we don't have a child result, or the child result is ahead of the current counter,
  // we just increment our virtual result's id until we hit the child result's
  // in which case we'll read from the child and bypass it by one.
  if (cr == NULL || cr->docId > current->docId) {
    goto ok;
  }

  while (cr->docId == current->docId) {
    // advance our docId to the next possible id
    current->docId++;

    // read the next entry from the child
    if (child->Read(&cr) == INDEXREAD_EOF) {
      break;
    }
  }

  // make sure we did not overflow
  if (current->docId > maxDocId) {
    return INDEXREAD_EOF;
  }

ok:
  // Set the next entry and return ok
  lastDocId = current->docId;
  if (hit) *hit = current;
  ++len;

  return INDEXREAD_OK;
}

//---------------------------------------------------------------------------------------------

// We always have next, in case anyone asks... ;)
int NotIterator::HasNext() {
  return lastDocId <= maxDocId;
}

// Our len is the child's len? TBD it might be better to just return 0
size_t NotIterator::Len() {
  return len;
}

// Last docId
t_docId NotIterator::LastDocId() {
  return lastDocId;
}

//---------------------------------------------------------------------------------------------

NotIterator::NotIterator(IndexIterator *it, t_docId maxDocId_, double weight_) {
  current = new VirtualResult(weight_);
  current->fieldMask = RS_FIELDMASK_ALL;
  current->docId = 0;

  child = it; //@@ ownership?
  childCT = NULL;
  lastDocId = 0;
  maxDocId = maxDocId_;
  len = 0;
  weight = weight_;

  _Read = &NotIterator::ReadSorted;
  mode = Mode::Sorted;

  if (child && child->mode == Mode::Unsorted) {
    childCT = child->GetCriteriaTester();
    RS_LOG_ASSERT(childCT, "childCT should not be NULL");
    _Read = &NotIterator::ReadUnsorted;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Optional clause iterator

class OptionalIterator : public IndexIterator {
public:
  OptionalIterator(IndexIterator *it, t_docId maxDocId_, double weight_);
  ~OptionalIterator();

  IndexIterator *child;
  IndexCriteriaTester *childCT;
  RSIndexResult *virt;
  t_fieldMask fieldMask;
  t_docId lastDocId;
  t_docId maxDocId;
  t_docId nextRealId;
  double weight;

  int (OptionalIterator::*_Read)(RSIndexResult **hit);

  virtual int Read(RSIndexResult **hit) { return (this->*_Read)(hit); }
  int ReadSorted(RSIndexResult **hit);
  int ReadUnsorted(RSIndexResult **hit);

  virtual int SkipTo(t_docId docId, RSIndexResult **hit);
  virtual IndexCriteriaTester *GetCriteriaTester();
  virtual size_t NumEstimated();
  virtual int HasNext();
  virtual void Abort();
  virtual size_t Len();
  virtual void Rewind();

  t_docId LastDocId();
};

typedef OptionalIterator OptionalMatchContext;

//---------------------------------------------------------------------------------------------

OptionalIterator::~OptionalIterator() {
  if (child) {
    delete child;
  }
  if (childCT) {
    delete childCT;
  }
  IndexResult_Free(virt);
}

//---------------------------------------------------------------------------------------------

int OptionalIterator::SkipTo(t_docId docId, RSIndexResult **hit) {
  //  printf("OI_SkipTo => %llu!. NextReal: %llu. Max: %llu. Last: %llu\n", docId, nextRealId,
  //  maxDocId, lastDocId);

  int found = 0;
  if (lastDocId > maxDocId) {
    return INDEXREAD_EOF;
  }

  // Set the current ID
  lastDocId = docId;

  if (!child) {
    virt->docId = docId;
    current = virt;
    return INDEXREAD_OK;
  }

  if (docId == 0) {
    return Read(hit);
  }

  if (docId == nextRealId) {
    // Edge case -- match on the docid we just looked for
    found = 1;
    // reset current pointer since this might have been a prior virt return
    current = child->current;

  } else if (docId > nextRealId) {
    int rc = child->SkipTo(docId, &current);
    if (rc == INDEXREAD_OK) {
      found = 1;
    }
    nextRealId = current->docId;
  }

  if (found) {
    // Has a real hit
    RSIndexResult *r = current;
  } else {
    virt->docId = docId;
    current = virt;
  }

  *hit = current;
  return INDEXREAD_OK;
}

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *OptionalIterator::GetCriteriaTester() {
  return new IndexCriteriaTester();
}

size_t OptionalIterator::NumEstimated() {
  return maxDocId;
}

int OptionalIterator::ReadUnsorted(RSIndexResult **hit) {
  if (lastDocId >= maxDocId) return INDEXREAD_EOF;
  lastDocId++;
  current = virt;
  current->docId = lastDocId;
  *hit = current;
  if (childCT->Test(lastDocId)) {
    current->weight = weight * 2;  // we increase the weight cause we found the id
  } else {
    current->weight = weight * 2;  // we do increase the weight cause id was not found
  }
  return INDEXREAD_OK;
}

// Read has no meaning in the sense of an OPTIONAL iterator, so we just read the next record from
// our child

int OptionalIterator::ReadSorted(RSIndexResult **hit) {
  if (lastDocId >= maxDocId) {
    return INDEXREAD_EOF;
  }

  // Increase the size by one
  lastDocId++;

  if (lastDocId > nextRealId) {
    int rc = child->Read(&current);
    if (rc == INDEXREAD_EOF) {
      nextRealId = maxDocId + 1;
    } else {
      nextRealId = current->docId;
    }
  }

  if (lastDocId != nextRealId) {
    current = virt;
    current->weight = 0;
  } else {
    current->weight = weight;
  }

  current->docId = lastDocId;
  *hit = current;
  return INDEXREAD_OK;
}

// We always have next, in case anyone asks... ;)

int OptionalIterator::HasNext() {
  return lastDocId <= maxDocId;
}

void OptionalIterator::Abort() {
  if (child) {
    child->Abort();
  }
}

// Our len is the child's len? TBD it might be better to just return 0

size_t OptionalIterator::Len() {
  return child ? child->Len() : 0;
}

// Last docId
t_docId OptionalIterator::LastDocId() {
  return lastDocId;
}

void OptionalIterator::Rewind() {
  lastDocId = 0;
  virt->docId = 0;
  if (child) {
    child->Rewind();
  }
}

//---------------------------------------------------------------------------------------------

OptionalIterator::OptionalIterator(IndexIterator *it, t_docId maxDocId_, double weight_) {
  virt = NewVirtualResult(weight);
  virt->fieldMask = RS_FIELDMASK_ALL;
  virt->freq = 1;
  current = virt;
  child = it;
  childCT = NULL;
  lastDocId = 0;
  maxDocId = maxDocId_;
  weight = weight_;
  nextRealId = 0;

  _Read = &OptionalIterator::ReadSorted;
  mode = Mode::Sorted;

  if (child && child->mode == Mode::Unsorted) {
    childCT = child->GetCriteriaTester();
    RS_LOG_ASSERT(childCT, "childCT should not be NULL");
    _Read = &OptionalIterator::ReadUnsorted;
  }
  if (!child) {
    child = NewEmptyIterator();
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Wildcard iterator, matchin ALL documents in the index. This is used for one thing only -
// purely negative queries.
// If the root of the query is a negative expression, we cannot process it without a positive expression. 
// So we create a wildcard iterator that basically just iterates all the incremental document ids, 
// and matches every skip within its range.

class WildcardIterator : public IndexIterator {
  WildcardIterator(t_docId maxId);

  t_docId topId;
  t_docId currentId;

  int Read(RSIndexResult **hit);
  int SkipTo(t_docId docId, RSIndexResult **hit);
  void Abort();
  int HasNext();
  size_t Len();
  t_docId LastDocId();
  void Rewind();
  size_t NumEstimated();
};

typedef WildcardIterator WildcardIteratorCtx;

//---------------------------------------------------------------------------------------------

// Read reads the next consecutive id, unless we're at the end

int WildcardIterator::Read(RSIndexResult **hit) {
  if (current > topId) {
    return INDEXREAD_EOF;
  }
  current->docId = currentId++;
  if (hit) {
    *hit = current;
  }
  return INDEXREAD_OK;
}

// Skipto for wildcard iterator - always succeeds, but this should normally not happen as it has no meaning

int WildcardIterator::SkipTo(t_docId docId, RSIndexResult **hit) {
  // printf("WI_Skipto %d\n", docId);
  if (currentId > topId) return INDEXREAD_EOF;

  if (docId == 0) return Read(hit);

  currentId = docId;
  current->docId = docId;
  if (hit) {
    *hit = current;
  }
  return INDEXREAD_OK;
}

void WildcardIterator::Abort() {
  currentId = topId + 1;
}

// We always have next, in case anyone asks... ;)

int WildcardIterator::HasNext() {
  return currentId <= topId;
}

// Our len is the len of the index...
size_t WildcardIterator::Len() {
  return topId;
}

// Last docId
t_docId WildcardIterator::LastDocId() {
  return currentId;
}

void WildcardIterator::Rewind() {
  currentId = 1;
}

size_t WildcardIterator::NumEstimated() {
  return SIZE_MAX;
}

WildcardIterator::WildcardIterator(t_docId maxId) {
  currentId = 1;
  topId = maxId;

  current = new VirtualResult(1);
  current->freq = 1;
  current->fieldMask = RS_FIELDMASK_ALL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

class EOFIterator : public IndexIterator {
public:
  virtual RSIndexResult *GetCurrent() {}
  virtual size_t NumEstimated() { return 0; }
  virtual IndexCriteriaTester *GetCriteriaTester() { return NULL; }
  virtual int Read(RSIndexResult **e) { return INDEXREAD_EOF; }
  virtual int SkipTo(t_docId docId, RSIndexResult **hit) { return INDEXREAD_EOF; }
  virtual t_docId LastDocId() { return 0; }
  virtual int HasNext() { return 0; }
  virtual size_t Len() { return 0; }
  virtual void Abort() {}
  virtual void Rewind() {}
};

static EOFIterator eofIterator;

IndexIterator *NewEmptyIterator() {
  return &eofIterator;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

// LCOV_EXCL_START unused
const char *IndexIterator_GetTypeString(const IndexIterator *it) {
  if (it->Free == UnionIterator_Free) {
    return "UNION";
  } else if (it->Free == IntersectIterator_Free) {
    return "INTERSECTION";
  } else if (it->Free == OI_Free) {
    return "OPTIONAL";
  } else if (it->Free == WI_Free) {
    return "WILDCARD";
  } else if (it->Free == NI_Free) {
    return "NOT";
  } else if (it->Free == ReadIterator_Free) {
    return "IIDX";
  } else if (it == &eofIterator) {
    return "EMPTY";
  } else {
    return "Unknown";
  }
}
// LCOV_EXCL_STOP

#endif //0

///////////////////////////////////////////////////////////////////////////////////////////////
