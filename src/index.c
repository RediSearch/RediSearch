
#include "index.h"
#include "varint.h"
#include "spec.h"
#include "object.h"

#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#include <math.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator::~IndexIterator() {
  if (current) {
    delete current;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

t_docId UnionIterator::LastDocId() const {
  return minDocId;
}

//---------------------------------------------------------------------------------------------

void UnionIterator::SyncIterList() {
  its = origits;
  // memcpy(its, origits, sizeof(*its) * origits.size());
  for (size_t i = 0; i < its.size(); ++i) {
    its[i]->minId = 0;
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

  // memmove(its + badix, its + badix + 1, sizeof(*its) * (num - badix - 1));
  its.erase(its.begin() + badix);
  // Repeat the same index again, because we have a new iterator at the same position
  return badix - 1;
}

//---------------------------------------------------------------------------------------------

void UnionIterator::Abort() {
  IITER_SET_EOF(this);
  for (int i = 0; i < its.size(); i++) {
    if (its[i]) {
      its[i]->Abort();
    }
  }
}

//---------------------------------------------------------------------------------------------

void UnionIterator::Rewind() {
  IITER_CLEAR_EOF(this);
  minDocId = 0;
  current->docId = 0;

  SyncIterList();

  // rewind all child iterators
  for (size_t i = 0; i < its.size(); i++) {
    its[i]->minId = 0;
    its[i]->Rewind();
  }
}

//---------------------------------------------------------------------------------------------

UnionIterator::UnionIterator(IndexIterators its_, DocTable *dt, int quickExit, double weight_) {
  origits = its_;
  weight = weight_;
  IITER_CLEAR_EOF(this);
  current = new UnionResult(its_.size(), weight);
  len = 0;
  quickExit = quickExit;
  nexpected = 0;
  currIt = 0;

  mode = IndexIteratorMode::Sorted;
  _Read = &UnionIterator::ReadSorted;

  SyncIterList();

  //@@ caveat here
  for (size_t i = 0; i < its_.size(); ++i) {
    nexpected += its_[i]->NumEstimated();
    if (its_[i]->mode == IndexIteratorMode::Unsorted) {
      mode = IndexIteratorMode::Unsorted;
      _Read = &UnionIterator::ReadUnsorted;
    }
  }

  const size_t maxresultsSorted = RSGlobalConfig.maxResultsToUnsortedMode;
  // this code is normally (and should be) dead.
  // i.e. the deepest-most IndexIterator does not have a CT
  //      so it will always eventually return NULL CT
  if (mode == IndexIteratorMode::Sorted && nexpected >= maxresultsSorted) {
    // make sure all the children support CriteriaTester
    bool ctSupported = true;
    for (int i = 0; i < origits.size(); ++i) {
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

///////////////////////////////////////////////////////////////////////////////////////////////

bool UnionCriteriaTester::Test(t_docId id) {
  for (int i = 0; i < children.size(); ++i) {
    if (children[i]->Test(id)) {
      return true;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *UnionIterator::GetCriteriaTester() {
  Vector<IndexCriteriaTester *> testers; // = rm_calloc(num, sizeof(IndexCriteriaTester *));
  for (size_t i = 0; i < origits.size(); ++i) {
    IndexCriteriaTester *tester = origits[i]->GetCriteriaTester();
    if (!tester) {
      delete tester;
      return NULL;
    }
	  testers[i] = tester;
  }
  return new UnionCriteriaTester(testers);
}

//---------------------------------------------------------------------------------------------

UnionCriteriaTester::UnionCriteriaTester(Vector<IndexCriteriaTester *> testers) :
  children(testers) {}

//---------------------------------------------------------------------------------------------

size_t UnionIterator::NumEstimated() const {
  return nexpected;
}

//---------------------------------------------------------------------------------------------

int UnionIterator::ReadUnsorted(IndexResult **hit) {
  IndexResult *res = NULL;
  while (currIt < origits.size()) {
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

int UnionIterator::ReadSorted(IndexResult **hit) {
  // nothing to do
  if (its.empty() || !IITER_HAS_NEXT(this)) {
    IITER_SET_EOF(this);
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  result().Reset();

  do {
    // find the minimal iterator
    t_docId minDocId = t_docId{UINT32_MAX};
    IndexIterator *minIt = NULL;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    unsigned nits = its.size();

    for (unsigned i = 0; i < nits; i++) {
      IndexIterator *it = its[i];
      IndexResult *res = IITER_CURRENT_RECORD(it);
      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
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
        nits = its.size();
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

// Skip to the given docId, or one place after it
// @param ctx IndexReader context
// @param docId docId to seek to
// @param hit an index hit we put our reads into
// @return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF if at EOF

int UnionIterator::SkipTo(t_docId docId, IndexResult **hit) {
  if (mode != IndexIteratorMode::Sorted) {
    throw Error("union iterator mode is not MODE_SORTED");
  }

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
  t_docId minDocId1 = t_docId{UINT32_MAX};
  IndexIterator *it;
  IndexResult *res;
  IndexResult *minResult = NULL;
  // skip all iterators to docId
  unsigned xnum = its.size();
  for (unsigned i = 0; i < xnum; i++) {
    it = its[i];
    // this happens for non existent words
    res = NULL;
    // If the requested docId is larger than the last read id from the iterator,
    // we need to read an entry from the iterator, seeking to this docId
    if (it->minId < docId) {
      if ((rc = it->SkipTo(docId, &res)) == INDEXREAD_EOF) {
        i = RemoveExhausted(i);
        xnum = its.size();
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

size_t UnionIterator::Len() {
  return len;
}

///////////////////////////////////////////////////////////////////////////////////////////////

IntersectIterator::~IntersectIterator() {
  if (bestIt) {
    delete bestIt;
  }
  rm_free(docIds);
}

//---------------------------------------------------------------------------------------------

void IntersectIterator::Abort() {
  isValid = 0;
  for (int i = 0; i < its.size(); i++) {
    if (its[i]) {
      its[i]->Abort();
    }
  }
}

void IntersectIterator::Rewind() {
  isValid = true;
  lastDocId = 0;

  // rewind all child iterators
  for (int i = 0; i < its.size(); i++) {
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

  IndexIterators unsortedIts;
  IndexIterators sortedIts;
  for (size_t i = 0; i < its.size(); ++i) {
    IndexIterator *curit = its[i];

    if (!curit) {
      // If the current iterator is empty, then the entire
      // query will fail; just free all the iterators and call it good
      bestIt = NULL;
      return;
    }

    size_t amount = curit->NumEstimated();
    if (amount < nexpected) {
      nexpected = amount;
      bestIt = curit;
    }

    if (curit->mode == IndexIteratorMode::Unsorted) {
      unsortedIts.push_back(curit);
    } else {
      sortedIts.push_back(curit);
    }
  }

  if (!unsortedIts.empty()) {
    if (unsortedIts.size() == its.size()) {
      mode = IndexIteratorMode::Unsorted;
      _Read = &IntersectIterator::ReadUnsorted;
      its.clear();
      its.push_back(bestIt);
      // The other iterators are also stored in unsortedIts
      // and because we know that there are no sorted iterators
    }

    for (size_t i = 0; i < unsortedIts.size(); ++i) {
      IndexIterator *cur = unsortedIts[i];
      if (mode == IndexIteratorMode::Unsorted && bestIt == cur) {
        continue;
      }
      IndexCriteriaTester *tester = cur->GetCriteriaTester();
      testers.push_back(tester);
      delete cur;
    }
  } else {
    bestIt = NULL;
  }

  delete &its;
  its = sortedIts;
}

//---------------------------------------------------------------------------------------------

IntersectIterator::IntersectIterator(IndexIterators its, DocTable *dt, t_fieldMask fieldMask_,
                                     int maxSlop_, int inOrder_, double weight_) : its(its){
  IntersectIterator *ctx = rm_calloc(1, sizeof(*ctx));
  lastDocId = 0;
  lastFoundId = 0;
  len = 0;
  maxSlop = maxSlop_;
  inOrder = inOrder_;
  fieldMask = fieldMask_;
  weight = weight_;
  docIds = rm_calloc(its.size(), sizeof(t_docId));
  docTable = dt;
  nexpected = UINT32_MAX;

  isValid = 1;
  current = new IntersectResult(its.size(), weight);

  SortChildren();
}

//---------------------------------------------------------------------------------------------

int IntersectIterator::SkipTo(t_docId docId, IndexResult **hit) {
  // A seek with docId 0 is equivalent to a read
  if (docId == 0) {
    return ReadSorted(hit);
  }

  result().Reset();
  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < its.size(); i++) {
    IndexIterator *it = its[i];

    if (!it || !IITER_HAS_NEXT(it)) return INDEXREAD_EOF;

    IndexResult *res = IITER_CURRENT_RECORD(it);
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
  if (nfound == its.size()) {
    // Update the last found id
    // if maxSlop == -1 there is no need to verify maxSlop and inorder, otherwise lets verify
    if (maxSlop == -1 || current->IsWithinRange(maxSlop, inOrder)) {
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

int IntersectIterator::ReadUnsorted(IndexResult **hit) {
  int rc = INDEXREAD_OK;
  IndexResult *res = NULL;
  for (;;) {
    rc = bestIt->Read(&res);
    if (rc == INDEXREAD_EOF) {
      return INDEXREAD_EOF;
      *hit = res;
      return rc;
    }
    bool isMatch = true;
    for (auto &tester: testers) {
      if (!tester.Test(res->docId)) {
        isMatch = false;
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

///////////////////////////////////////////////////////////////////////////////////////////////

IntersectIterator::CriteriaTester::CriteriaTester(Vector<IndexCriteriaTester*> testers) : children(testers) {}

//---------------------------------------------------------------------------------------------

bool IntersectIterator::CriteriaTester::Test(t_docId id) {
  for (auto &child: children) {
    if (!child.Test(id)) {
      return false;
    }
  }
  return true;
}

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *IntersectIterator::GetCriteriaTester() {
  for (auto &it: its) {
    IndexCriteriaTester *tester = NULL;
    if (it) {
      tester = it->GetCriteriaTester();
    }
    if (!tester) {
      testers.clear();
      return NULL;
    }
    testers.push_back(tester);
  }
  CriteriaTester *it = new CriteriaTester(testers);
  testers.clear();
  return it;
}

//---------------------------------------------------------------------------------------------

size_t IntersectIterator::NumEstimated() const {
  return nexpected;
}

//---------------------------------------------------------------------------------------------

int IntersectIterator::ReadSorted(IndexResult **hit) {
  if (its.empty()) return INDEXREAD_EOF;

  int nh = 0;
  int i = 0;

  do {
    nh = 0;
    result().Reset();

    for (i = 0; i < its.size(); i++) {
      IndexIterator *it = its[i];

      if (!it) goto eof;

      IndexResult *h = IITER_CURRENT_RECORD(it);
      // skip to the next
      int rc = INDEXREAD_OK;
      if (docIds[i] != lastDocId || lastDocId == 0) {

        if (i == 0 && docIds[i] >= lastDocId) {
          rc = it->Read(&h);
        } else {
          rc = it->SkipTo(lastDocId, &h);
        }

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

    if (nh == its.size()) {
      // sum up all hits
      if (hit != NULL) {
        *hit = current;
      }

      // Update the last valid found id
      lastFoundId = current->docId;

      // advance the doc id so next time we'll read a new record
      lastDocId++;

      // make sure the flags are matching.
      if ((current->fieldMask & fieldMask) == 0) {
        continue;
      }

      // If we need to match slop and order, we do it now, and possibly skip the result
      if (maxSlop >= 0) {
        if (!current->IsWithinRange(maxSlop, inOrder)) {
          continue;
        }
      }

      len++;
      return INDEXREAD_OK;
    }
  } while (1);
eof:
  isValid = 0;
  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

t_docId IntersectIterator::LastDocId() const {
  // return last FOUND id, not last read id form any child
  return lastFoundId;
}

//---------------------------------------------------------------------------------------------

size_t IntersectIterator::Len() {
  return len;
}

///////////////////////////////////////////////////////////////////////////////////////////////

void NotIterator::Abort() {
  if (child) {
    child->Abort();
  }
}

//---------------------------------------------------------------------------------------------

void NotIterator::Rewind() {
  lastDocId = 0;
  current->docId = 0;
  if (child) {
    child->Rewind();
  }
}

//---------------------------------------------------------------------------------------------

NotIterator::~NotIterator() {
  if (child) {
    delete child;
  }
  if (childCT) {
    delete childCT;
  }
  delete current;
}

//---------------------------------------------------------------------------------------------

// If we have a match - return NOTFOUND. If we don't or we're at the end - return OK

int NotIterator::SkipTo(t_docId docId, IndexResult **hit) {
  int rc;
  t_docId childId;

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
  childId = child->LastDocId();

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
  rc = child->SkipTo(docId, hit);

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

///////////////////////////////////////////////////////////////////////////////////////////////

NI_CriteriaTester::NI_CriteriaTester(IndexCriteriaTester *childTester) {
  child = childTester; //@@ ownership?
}

//---------------------------------------------------------------------------------------------

int NI_CriteriaTester::Test(t_docId id) {
  return !child->Test(id);
}

//---------------------------------------------------------------------------------------------

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

size_t NotIterator::NumEstimated() const {
  return maxDocId;
}

int NotIterator::ReadUnsorted(IndexResult **hit) {
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

int NotIterator::ReadSorted(IndexResult **hit) {
  if (lastDocId > maxDocId) return INDEXREAD_EOF;

  IndexResult *cr = NULL;
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

bool NotIterator::HasNext() const {
  return lastDocId <= maxDocId;
}

// Our len is the child's len? TBD it might be better to just return 0

size_t NotIterator::Len() const {
  return len;
}

//---------------------------------------------------------------------------------------------

t_docId NotIterator::LastDocId() const {
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
  mode = IndexIteratorMode::Sorted;

  if (child && child->mode == IndexIteratorMode::Unsorted) {
    childCT = child->GetCriteriaTester();
    RS_LOG_ASSERT(childCT, "childCT should not be NULL");
    _Read = &NotIterator::ReadUnsorted;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

OptionalIterator::~OptionalIterator() {
  if (child) {
    delete child;
  }
  if (childCT) {
    delete childCT;
  }
  delete virt;
}

//---------------------------------------------------------------------------------------------

int OptionalIterator::SkipTo(t_docId docId, IndexResult **hit) {
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
    IndexResult *r = current;
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

size_t OptionalIterator::NumEstimated() const {
  return maxDocId;
}

//---------------------------------------------------------------------------------------------

int OptionalIterator::ReadUnsorted(IndexResult **hit) {
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

//---------------------------------------------------------------------------------------------

// Read has no meaning in the sense of an OPTIONAL iterator, so we just read the next record from
// our child

int OptionalIterator::ReadSorted(IndexResult **hit) {
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

//---------------------------------------------------------------------------------------------

// We always have next, in case anyone asks... ;)

bool OptionalIterator::HasNext() const {
  return lastDocId <= maxDocId;
}

//---------------------------------------------------------------------------------------------

void OptionalIterator::Abort() {
  if (child) {
    child->Abort();
  }
}

//---------------------------------------------------------------------------------------------

// Our len is the child's len? TBD it might be better to just return 0

size_t OptionalIterator::Len() const {
  return child ? child->Len() : 0;
}

//---------------------------------------------------------------------------------------------

t_docId OptionalIterator::LastDocId() const {
  return lastDocId;
}

//---------------------------------------------------------------------------------------------

void OptionalIterator::Rewind() {
  lastDocId = 0;
  virt->docId = 0;
  if (child) {
    child->Rewind();
  }
}

//---------------------------------------------------------------------------------------------

OptionalIterator::OptionalIterator(IndexIterator *it, t_docId maxDocId_, double weight_) {
  virt = new VirtualResult(weight);
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
  mode = IndexIteratorMode::Sorted;

  if (child && child->mode == IndexIteratorMode::Unsorted) {
    childCT = child->GetCriteriaTester();
    RS_LOG_ASSERT(childCT, "childCT should not be NULL");
    _Read = &OptionalIterator::ReadUnsorted;
  }
  if (!child) {
    child = new EmptyIterator();
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Read reads the next consecutive id, unless we're at the end

int WildcardIterator::Read(IndexResult **hit) {
  if (currentId > topId) {
    return INDEXREAD_EOF;
  }
  current->docId = currentId++;
  if (hit) {
    *hit = current;
  }
  return INDEXREAD_OK;
}

//---------------------------------------------------------------------------------------------

// Skipto for wildcard iterator - always succeeds, but this should normally not happen as it has no meaning

int WildcardIterator::SkipTo(t_docId docId, IndexResult **hit) {
  if (currentId > topId) return INDEXREAD_EOF;

  if (docId == 0) return Read(hit);

  currentId = docId;
  current->docId = docId;
  if (hit) {
    *hit = current;
  }
  return INDEXREAD_OK;
}

//---------------------------------------------------------------------------------------------

void WildcardIterator::Abort() {
  currentId = topId + 1;
}

//---------------------------------------------------------------------------------------------

// We always have next, in case anyone asks... ;)

bool WildcardIterator::HasNext() const {
  return currentId <= topId;
}

//---------------------------------------------------------------------------------------------

// Our len is the len of the index...

size_t WildcardIterator::Len() const {
  return topId;
}

//---------------------------------------------------------------------------------------------

t_docId WildcardIterator::LastDocId() const {
  return currentId;
}

//---------------------------------------------------------------------------------------------

void WildcardIterator::Rewind() {
  currentId = 1;
}

//---------------------------------------------------------------------------------------------

size_t WildcardIterator::NumEstimated() const {
  return SIZE_MAX;
}

//---------------------------------------------------------------------------------------------

WildcardIterator::WildcardIterator(t_docId maxId) {
  currentId = 1;
  topId = maxId;

  current = new VirtualResult(1);
  current->freq = 1;
  current->fieldMask = RS_FIELDMASK_ALL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

static EOFIterator eofIterator;

IndexIterator *NewEmptyIterator() {
  return &eofIterator;
}

#endif // 0

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
