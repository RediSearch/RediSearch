#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include "spec.h"
#include <math.h>
#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/param.h>
#include "rmalloc.h"

static int UI_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit);
static int UI_Next(void *ctx);
static inline int UI_ReadUnsorted(void *ctx, RSIndexResult **hit);
static int UI_ReadSorted(void *ctx, RSIndexResult **hit);
static size_t UI_ExpectedResultsAmount(void *ctx);
static IndexCriteriaTester* UI_GetCriteriaTester(void *ctx);
static size_t UI_Len(void *ctx);

static int II_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit);
static int II_Next(void *ctx);
static int II_ReadUnsorted(void *ctx, RSIndexResult **hit);
static IndexCriteriaTester* II_GetCriteriaTester(void *ctx);
static int II_ReadSorted(void *ctx, RSIndexResult **hit);
static size_t II_ExpectedResultsAmount(void *ctx);
static size_t II_Len(void *ctx);
static t_docId II_LastDocId(void *ctx);

#define CURRENT_RECORD(ii) (ii)->base.current

typedef struct {
  IndexIterator base;
  /**
   * We maintain two iterator arrays. One is the original iterator list, and
   * the other is the list of currently active iterators. When an iterator
   * reaches EOF, it is set to NULL in the `its` list, but is still retained in
   * the `origits` list, for the purpose of supporting things like Rewind() and
   * Free()
   */
  IndexIterator **its;
  IndexIterator **origits;
  uint32_t num;
  uint32_t norig;
  uint32_t currIt;
  t_docId minDocId;

  // If set to 1, we exit skips after the first hit found and not merge further results
  int quickExit;
  size_t expectedResutlsAmount;
  double weight;
  uint64_t len;
} UnionIterator;

static inline t_docId UI_LastDocId(void *ctx) {
  return ((UnionIterator *)ctx)->minDocId;
}

static void UI_SyncIterList(UnionIterator *ui) {
  ui->num = ui->norig;
  memcpy(ui->its, ui->origits, sizeof(*ui->its) * ui->norig);
  for (size_t ii = 0; ii < ui->num; ++ii) {
    ui->its[ii]->minId = 0;
  }
}

/**
 * Removes the exhausted iterator from the active list, so that future
 * reads will no longer iterate over it
 */
static size_t UI_RemoveExhausted(UnionIterator *it, size_t badix) {
  // e.g. assume we have 10 entries, and we want to remove index 8, which means
  // one more valid entry at the end. This means we use
  // source: its + 8 + 1
  // destination: its + 8
  // number: it->len (10) - (8) - 1 == 1
  memmove(it->its + badix, it->its + badix + 1, sizeof(*it->its) * (it->num - badix - 1));
  it->num--;
  // Repeat the same index again, because we have a new iterator at the same
  // position
  return badix - 1;
}

static void UI_Abort(void *ctx) {
  UnionIterator *it = ctx;
  IITER_SET_EOF(&it->base);
  for (int i = 0; i < it->num; i++) {
    if (it->its[i]) {
      it->its[i]->Abort(it->its[i]->ctx);
    }
  }
}

static void UI_Rewind(void *ctx) {
  UnionIterator *ui = ctx;
  IITER_CLEAR_EOF(&ui->base);
  ui->minDocId = 0;
  CURRENT_RECORD(ui)->docId = 0;

  UI_SyncIterList(ui);

  // rewind all child iterators
  for (size_t i = 0; i < ui->num; i++) {
    ui->its[i]->minId = 0;
    ui->its[i]->Rewind(ui->its[i]->ctx);
  }
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt, int quickExit,
                                double weight) {
  // create union context
  UnionIterator *ctx = calloc(1, sizeof(UnionIterator));
  ctx->origits = its;
  ctx->weight = weight;
  ctx->num = num;
  ctx->norig = num;
  IITER_CLEAR_EOF(&ctx->base);
  CURRENT_RECORD(ctx) = NewUnionResult(num, weight);
  ctx->len = 0;
  ctx->quickExit = quickExit;
  ctx->its = calloc(ctx->num, sizeof(*ctx->its));
  ctx->expectedResutlsAmount = 0;
  ctx->currIt = 0;

  // bind the union iterator calls
  IndexIterator *it = &ctx->base;
  it->mode = MODE_SORTED;
  it->ctx = ctx;
  it->GetCriteriaTester = UI_GetCriteriaTester;
  it->ExpectedResultsAmount = UI_ExpectedResultsAmount;
  it->LastDocId = UI_LastDocId;
  it->Read = UI_ReadSorted;
  it->SkipTo = UI_SkipTo;
  it->HasNext = NULL;
  it->Free = UnionIterator_Free;
  it->Len = UI_Len;
  it->Abort = UI_Abort;
  it->Rewind = UI_Rewind;
  UI_SyncIterList(ctx);

  for(int i = 0 ; i < num ; ++i){
    ctx->expectedResutlsAmount += its[i]->ExpectedResultsAmount(its[i]->ctx);
  }

#define MAX_RESULTS_FOR_UNSORTED_MODE 1000
  if(ctx->expectedResutlsAmount > MAX_RESULTS_FOR_UNSORTED_MODE){
    it->mode = MODE_UNSORTED;
    it->Read = UI_ReadUnsorted;
  }

  return it;
}

typedef struct UnionCriteriaTesterCtx{
  IndexCriteriaTester** chiledren;
  int chiledrenLen;
}UnionCriteriaTesterCtx;

static int UI_TextCriteria(void *ctx, t_docId id){
  UnionCriteriaTesterCtx* ctc = ctx;
  for(int i = 0 ; i < ctc->chiledrenLen ; ++i){
    if(ctc->chiledren[i]->TextCriteria(ctc->chiledren[i]->ctx, id)){
      return 1;
    }
  }
  return 0;
}

static void UI_TextCriteriaFree(struct IndexCriteriaTester* ct){
  UnionCriteriaTesterCtx* ctc = ct->ctx;
  for(int i = 0 ; i < ctc->chiledrenLen ; ++i){
    ctc->chiledren[i]->Free(ctc->chiledren[i]);
  }
  rm_free(ctc->chiledren);
  rm_free(ctc);
  rm_free(ct);
}

static IndexCriteriaTester* UI_GetCriteriaTester(void *ctx){
  UnionIterator *ui = ctx;
  UnionCriteriaTesterCtx* ctc = rm_malloc(sizeof(UnionCriteriaTesterCtx));
  ctc->chiledrenLen = ui->num;
  ctc->chiledren = rm_malloc(ctc->chiledrenLen * sizeof(IndexCriteriaTester*));
  for(int i = 0 ; i < ctc->chiledrenLen ; ++i){
    ctc->chiledren[i] = ui->its[i]->GetCriteriaTester(ui->its[i]->ctx);
  }
  IndexCriteriaTester* ct = rm_malloc(sizeof(*ct));
  ct->ctx = ctc;
  ct->TextCriteria = UI_TextCriteria;
  ct->Free = UI_TextCriteriaFree;
  return ct;
}

static size_t UI_ExpectedResultsAmount(void *ctx){
  UnionIterator *ui = ctx;
  return ui->expectedResutlsAmount;
}

static inline int UI_ReadUnsorted(void *ctx, RSIndexResult **hit) {
  UnionIterator *ui = ctx;
  int rc = INDEXREAD_OK;
  RSIndexResult *res = NULL;
  while(ui->currIt < ui->num){
    rc = ui->origits[ui->currIt]->Read(ui->origits[ui->currIt]->ctx, &res);
    if(rc == INDEXREAD_OK){
      *hit = res;
      return rc;
    }
    ++ui->currIt;
  }
  return INDEXREAD_EOF;
}

static inline int UI_ReadSorted(void *ctx, RSIndexResult **hit) {
  UnionIterator *ui = ctx;
  // nothing to do
  if (ui->num == 0 || !IITER_HAS_NEXT(&ui->base)) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  AggregateResult_Reset(CURRENT_RECORD(ui));

  do {

    // find the minimal iterator
    t_docId minDocId = UINT32_MAX;
    IndexIterator *minIt = NULL;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    unsigned nits = ui->num;

    for (unsigned i = 0; i < nits; i++) {
      IndexIterator *it = ui->its[i];
      RSIndexResult *res = IITER_CURRENT_RECORD(it);
      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      // printf("ui->docIds[%d]: %d, ui->minDocId: %d\n", i, ui->docIds[i], ui->minDocId);
      while (it->minId <= ui->minDocId && rc != INDEXREAD_EOF) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          rc = it->Read(it->ctx, &res);
          it->minId = res->docId;
        }
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      } else {
        // Remove this from the active list
        i = UI_RemoveExhausted(ui, i);
        nits = ui->num;
        continue;
      }

      if (rc == INDEXREAD_OK && res->docId <= minDocId) {
        minDocId = res->docId;
        minIt = it;
      }
    }

    // take the minimum entry and collect all results matching to it
    if (minIt) {
      UI_SkipTo(ui, minIt->minId, hit);
      // return INDEXREAD_OK;
      ui->minDocId = minIt->minId;
      ui->len++;
      return INDEXREAD_OK;
    }

  } while (numActive > 0);
  IITER_SET_EOF(&ui->base);

  return INDEXREAD_EOF;
}

static int UI_Next(void *ctx) {
  // RSIndexResult h = NewIndexResult();
  return UI_ReadSorted(ctx, NULL);
}

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF
if
at EOF
*/
static int UI_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  UnionIterator *ui = ctx;
  assert(ui->base.mode == MODE_SORTED);

  // printf("UI %p skipto %d\n", ui, docId);

  if (docId == 0) {
    return UI_ReadSorted(ctx, hit);
  }

  if (!IITER_HAS_NEXT(&ui->base)) {
    return INDEXREAD_EOF;
  }

  // reset the current hitf
  AggregateResult_Reset(CURRENT_RECORD(ui));
  CURRENT_RECORD(ui)->weight = ui->weight;
  int numActive = 0;
  int found = 0;
  int rc = INDEXREAD_EOF;
  unsigned num = ui->num;
  const int quickExit = ui->quickExit;
  t_docId minDocId = UINT32_MAX;
  IndexIterator *it;
  RSIndexResult *res;
  RSIndexResult *minResult = NULL;
  // skip all iterators to docId
  for (unsigned i = 0; i < num; i++) {
    it = ui->its[i];
    // this happens for non existent words
    res = NULL;
    // If the requested docId is larger than the last read id from the iterator,
    // we need to read an entry from the iterator, seeking to this docId
    if (it->minId < docId) {
      if ((rc = it->SkipTo(it->ctx, docId, &res)) == INDEXREAD_EOF) {
        i = UI_RemoveExhausted(ui, i);
        num = ui->num;
        continue;
      }
      if (res) {
        it->minId = res->docId;
      }

    } else {
      // if the iterator is ahead of docId - we avoid reading the entry
      // in this case, we are either past or at the requested docId, no need to actually read
      rc = (it->minId == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
      res = IITER_CURRENT_RECORD(it);
    }

    // if we've read successfully, update the minimal docId we've found
    if (it->minId && rc != INDEXREAD_EOF) {
      if (it->minId < minDocId || !minResult) {
        minResult = res;
        minDocId = it->minId;
      }
      // sminDocId = MIN(ui->docIds[i], minDocId);
    }

    // we found a hit - continue to all results matching the same docId
    if (rc == INDEXREAD_OK) {

      // add the result to the aggregate result we are holding
      if (hit) {
        AggregateResult_AddChild(CURRENT_RECORD(ui), res ? res : IITER_CURRENT_RECORD(it));
      }
      ui->minDocId = it->minId;
      ++found;
    }
    ++numActive;
    // If we've found a single entry and we are iterating in quick exit mode - exit now
    if (found && quickExit) break;
  }

  // all iterators are at the end
  if (numActive == 0) {
    IITER_SET_EOF(&ui->base);
    return INDEXREAD_EOF;
  }

  // copy our aggregate to the upstream hit
  *hit = CURRENT_RECORD(ui);
  if (found > 0) {
    return INDEXREAD_OK;
  }
  if (minResult) {
    *hit = minResult;
    AggregateResult_AddChild(CURRENT_RECORD(ui), minResult);
  }
  // not found...
  ui->minDocId = minDocId;
  return INDEXREAD_NOTFOUND;
}

void UnionIterator_Free(IndexIterator *itbase) {
  if (itbase == NULL) return;

  UnionIterator *ui = itbase->ctx;
  for (int i = 0; i < ui->norig; i++) {
    IndexIterator *it = ui->origits[i];
    if (it) {
      it->Free(it);
    }
  }

  IndexResult_Free(CURRENT_RECORD(ui));
  free(ui->its);
  free(ui->origits);
  free(ui);
}

static size_t UI_Len(void *ctx) {
  return ((UnionIterator *)ctx)->len;
}

/* The context used by the intersection methods during iterating an intersect
 * iterator */
typedef struct {
  IndexIterator base;
  IndexIterator **its;
  IndexIterator *bestIt;
  IndexCriteriaTester** testers;
  t_docId *docIds;
  int *rcs;
  int num;
  size_t len;
  int maxSlop;
  int inOrder;
  // the last read docId from any child
  t_docId lastDocId;
  // the last id that was found on all children
  t_docId lastFoundId;

  // RSIndexResult *result;
  DocTable *docTable;
  t_fieldMask fieldMask;
  double weight;
  size_t expectedResutlsAmount;
} IntersectIterator;

void IntersectIterator_Free(IndexIterator *it) {
  if (it == NULL) return;
  IntersectIterator *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i] != NULL) {
      ui->its[i]->Free(ui->its[i]);
    }
    // IndexResult_Free(&ui->currentHits[i]);
  }

  for (int i = 0; i < array_len(ui->testers); i++) {
    ui->testers[i]->Free(ui->testers[i]);
  }
  free(ui->docIds);
  IndexResult_Free(it->current);
  array_free(ui->its);
  array_free(ui->testers);
  free(it);
}

static void II_Abort(void *ctx) {
  IntersectIterator *it = ctx;
  it->base.isValid = 0;
  for (int i = 0; i < it->num; i++) {
    if (it->its[i]) {
      it->its[i]->Abort(it->its[i]->ctx);
    }
  }
}

static void II_Rewind(void *ctx) {
  IntersectIterator *ii = ctx;
  ii->base.isValid = 1;
  ii->lastDocId = 0;

  // rewind all child iterators
  for (int i = 0; i < ii->num; i++) {
    ii->docIds[i] = 0;
    if (ii->its[i]) {
      ii->its[i]->Rewind(ii->its[i]->ctx);
    }
  }
}

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *dt,
                                   t_fieldMask fieldMask, int maxSlop, int inOrder, double weight) {
#define INITIAL_LEN 10
  // printf("Creating new intersection iterator with fieldMask=%llx\n", fieldMask);
  IntersectIterator *ctx = calloc(1, sizeof(*ctx));
  ctx->its = array_new(IndexIterator *, INITIAL_LEN);
  ctx->testers = array_new(IndexCriteriaTester *, INITIAL_LEN);
  ctx->bestIt = NULL;
  IndexIterator **unsortedIts = array_new(IndexIterator *, INITIAL_LEN);;
  ctx->num = 0;
  ctx->lastDocId = 0;
  ctx->lastFoundId = 0;
  ctx->len = 0;
  ctx->maxSlop = maxSlop;
  ctx->inOrder = inOrder;
  ctx->fieldMask = fieldMask;
  ctx->weight = weight;
  ctx->docIds = calloc(num, sizeof(t_docId));
  ctx->docTable = dt;
  ctx->expectedResutlsAmount = UINT32_MAX;

  ctx->base.isValid = 1;
  ctx->base.current = NewIntersectResult(num, weight);

  // bind the iterator calls
  IndexIterator *it = &ctx->base;
  it->ctx = ctx;
  it->LastDocId = II_LastDocId;
  it->ExpectedResultsAmount = II_ExpectedResultsAmount;
  it->GetCriteriaTester = II_GetCriteriaTester;
  it->Read = II_ReadSorted;
  it->SkipTo = II_SkipTo;
  it->Len = II_Len;
  it->Free = IntersectIterator_Free;
  it->Abort = II_Abort;
  it->Rewind = II_Rewind;
  it->GetCurrent = NULL;
  it->HasNext = NULL;
  it->mode = MODE_SORTED;

  int allSorted = 1;
  IndexIterator *bestIterator;
  for(int i = 0 ; i < num ; ++i){
    if(!its[i]){
      // a null iterator is handled as an iterator with no results
      ctx->expectedResutlsAmount = 0;
      ctx->its = array_append(ctx->its, NULL);
      ctx->num++;
      continue;
    }
    size_t amount = its[i]->ExpectedResultsAmount(its[i]->ctx);
    if(amount < ctx->expectedResutlsAmount){
      ctx->expectedResutlsAmount = amount;
      ctx->bestIt = its[i];
    }
    if(its[i]->mode == MODE_SORTED){
      ctx->its = array_append(ctx->its, its[i]);
      ctx->num++;
    }else{
      unsortedIts = array_append(unsortedIts, its[i]);
    }
  }

  if(array_len(ctx->its) == 0){
    for(size_t i = 0 ; i < array_len(unsortedIts) ; ++i){
      if(unsortedIts[i] != ctx->bestIt){
        ctx->testers = array_append(ctx->testers, unsortedIts[i]->GetCriteriaTester(unsortedIts[i]->ctx));
        unsortedIts[i]->Free(unsortedIts[i]);
      }
    }
    it->mode = MODE_UNSORTED;
    it->Read = II_ReadUnsorted;
  }else{
    for(size_t i = 0 ; i < array_len(unsortedIts) ; ++i){
      ctx->testers = array_append(ctx->testers, unsortedIts[i]->GetCriteriaTester(unsortedIts[i]->ctx));
      unsortedIts[i]->Free(unsortedIts[i]);
    }
  }

  return it;
}

static int II_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  /* A seek with docId 0 is equivalent to a read */
  if (docId == 0) {
    return II_ReadSorted(ctx, hit);
  }
  IntersectIterator *ic = ctx;
  AggregateResult_Reset(ic->base.current);
  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < ic->num; i++) {
    IndexIterator *it = ic->its[i];

    if (!it || !IITER_HAS_NEXT(it)) return INDEXREAD_EOF;

    RSIndexResult *res = IITER_CURRENT_RECORD(it);
    rc = INDEXREAD_OK;

    // only read if we are not already at the seek to position
    if (ic->docIds[i] != docId) {
      rc = it->SkipTo(it->ctx, docId, &res);
      if (rc != INDEXREAD_EOF) {
        if (res) ic->docIds[i] = res->docId;
      }
    }

    if (rc == INDEXREAD_EOF) {
      // we are at the end!
      ic->base.isValid = 0;
      return rc;
    } else if (rc == INDEXREAD_OK) {

      // YAY! found!
      AggregateResult_AddChild(ic->base.current, res);
      ic->lastDocId = docId;

      ++nfound;
    } else if (ic->docIds[i] > ic->lastDocId) {
      ic->lastDocId = ic->docIds[i];
      break;
    }
  }

  // unless we got an EOF - we put the current record into hit

  // if the requested id was found on all children - we return OK
  if (nfound == ic->num) {
    // printf("Skipto %d hit @%d\n", docId, ic->current->docId);

    // Update the last found id
    ic->lastFoundId = ic->base.current->docId;
    if (hit) *hit = ic->base.current;
    return INDEXREAD_OK;
  }

  // Not found - but we need to read the next valid result into hit
  rc = II_ReadSorted(ic, hit);
  // this might have brought us to our end, in which case we just terminate
  if (rc == INDEXREAD_EOF) return INDEXREAD_EOF;

  // otherwise - not found
  return INDEXREAD_NOTFOUND;
}

static int II_Next(void *ctx) {
  return II_ReadSorted(ctx, NULL);
}

static int II_ReadUnsorted(void *ctx, RSIndexResult **hit){
  IntersectIterator *ic = ctx;
  int rc = INDEXREAD_OK;
  RSIndexResult *res = NULL;
  while(1){
    rc = ic->bestIt->Read(ic->bestIt->ctx, &res);
    if(rc == INDEXREAD_EOF){
      return INDEXREAD_EOF;
      *hit = res;
      return rc;
    }
    int isMatch = 1;
    for(size_t i = 0 ; i < array_len(ic->testers) ; ++i){
      if(!ic->testers[i]->TextCriteria(ic->testers[i]->ctx, res->docId)){
        isMatch = 0;
        break;
      }
    }
    if(!isMatch){
      continue;
    }
    *hit = res;
    return rc;
  }
}

typedef struct IICriteriaTesterCtx{
  IndexCriteriaTester** chiledren;
}IICriteriaTesterCtx;

static int II_TextCriteria(void *ctx, t_docId id){
  IICriteriaTesterCtx* ctc = ctx;
  for(size_t i = 0 ; i < array_len(ctc->chiledren) ; ++i){
    if(!ctc->chiledren[i]->TextCriteria(ctc->chiledren[i]->ctx, id)){
      return 0;
    }
  }
  return 1;
}

static void II_TextCriteriaFree(struct IndexCriteriaTester* ct){
  IICriteriaTesterCtx* ctc = ct->ctx;
  for(size_t i = 0 ; i < array_len(ctc->chiledren) ; ++i){
    ctc->chiledren[i]->Free(ctc->chiledren[i]);
  }
  array_free(ctc->chiledren);
  rm_free(ctc);
  rm_free(ct);
}

static IndexCriteriaTester* II_GetCriteriaTester(void *ctx){
  IntersectIterator *ic = ctx;
  IICriteriaTesterCtx* ctc = rm_malloc(sizeof(*ctc));
  for(size_t i = 0 ; i < array_len(ic->its) ; ++i){
    ic->testers = array_append(ic->testers, ic->its[i]->GetCriteriaTester(ic->its[i]));
  }
  ctc->chiledren = ic->testers;
  ic->testers = array_new(IndexCriteriaTester*, 0);
  IndexCriteriaTester* ct = rm_malloc(sizeof(*ct));
  ct->ctx = ctc;
  ct->TextCriteria = II_TextCriteria;
  ct->Free = II_TextCriteriaFree;
  return ct;
}

static size_t II_ExpectedResultsAmount(void *ctx){
  IntersectIterator *ic = ctx;
  return ic->expectedResutlsAmount;
}

static int II_ReadSorted(void *ctx, RSIndexResult **hit) {
  IntersectIterator *ic = ctx;

  if (ic->num == 0) return INDEXREAD_EOF;
  AggregateResult_Reset(ic->base.current);

  int nh = 0;
  int i = 0;

  do {

    nh = 0;
    AggregateResult_Reset(ic->base.current);

    for (i = 0; i < ic->num; i++) {
      IndexIterator *it = ic->its[i];

      if (!it) goto eof;

      RSIndexResult *h = IITER_CURRENT_RECORD(it);
      // skip to the next
      int rc = INDEXREAD_OK;
      if (ic->docIds[i] != ic->lastDocId || ic->lastDocId == 0) {

        if (i == 0 && ic->docIds[i] >= ic->lastDocId) {
          rc = it->Read(it->ctx, &h);
        } else {
          rc = it->SkipTo(it->ctx, ic->lastDocId, &h);
        }
        // printf("II %p last docId %d, it %d read docId %d(%d), rc %d\n", ic, ic->lastDocId, i,
        //        h->docId, it->LastDocId(it->ctx), rc);

        if (rc == INDEXREAD_EOF) goto eof;
        ic->docIds[i] = h->docId;
      }

      if (ic->docIds[i] > ic->lastDocId) {
        ic->lastDocId = ic->docIds[i];
        break;
      }
      if (rc == INDEXREAD_OK) {
        ++nh;
        AggregateResult_AddChild(ic->base.current, h);
      } else {
        ic->lastDocId++;
      }
    }

    if (nh == ic->num) {
      // printf("II %p HIT @ %d\n", ic, ic->current->docId);
      // sum up all hits
      if (hit != NULL) {
        *hit = ic->base.current;
      }
      // Update the last valid found id
      ic->lastFoundId = ic->base.current->docId;

      // advance the doc id so next time we'll read a new record
      ic->lastDocId++;

      // // make sure the flags are matching.
      if ((ic->base.current->fieldMask & ic->fieldMask) == 0) {
        // printf("Field masks don't match!\n");
        continue;
      }

      // If we need to match slop and order, we do it now, and possibly skip the result
      if (ic->maxSlop >= 0) {
        // printf("Checking SLOP... (%d)\n", ic->maxSlop);
        if (!IndexResult_IsWithinRange(ic->base.current, ic->maxSlop, ic->inOrder)) {
          // printf("Not within range!\n");
          continue;
        }
      }

//      for(size_t i = 0 ; i < array_len(ic->testers) ; ++i){
//        if(!ic->testers[i]->TextCriteria(ic->testers[i]->ctx, ic->lastFoundId)){
//          continue;
//        }
//      }

      ic->len++;
      // printf("Returning OK\n");
      return INDEXREAD_OK;
    }
  } while (1);
eof:
  ic->base.isValid = 0;
  return INDEXREAD_EOF;
}

static t_docId II_LastDocId(void *ctx) {
  // return last FOUND id, not last read id form any child
  return ((IntersectIterator *)ctx)->lastFoundId;
}

static size_t II_Len(void *ctx) {
  return ((IntersectIterator *)ctx)->len;
}

/* A Not iterator works by wrapping another iterator, and returning OK for misses, and NOTFOUND for
 * hits */
typedef struct {
  IndexIterator base;
  IndexIterator *child;
  IndexCriteriaTester* childCT;
  t_docId lastDocId;
  t_docId maxDocId;
  size_t len;
  double weight;
} NotIterator, NotContext;

static void NI_Abort(void *ctx) {
  NotContext *nc = ctx;
  if (nc->child) {
    nc->child->Abort(nc->child->ctx);
  }
}

static void NI_Rewind(void *ctx) {
  NotContext *nc = ctx;
  nc->lastDocId = 0;
  nc->base.current->docId = 0;
  if (nc->child) {
    nc->child->Rewind(nc->child->ctx);
  }
}
static void NI_Free(IndexIterator *it) {

  NotContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  if (nc->childCT) {
    nc->childCT->Free(nc->childCT);
  }
  IndexResult_Free(nc->base.current);
  free(it);
}

/* SkipTo for NOT iterator. If we have a match - return NOTFOUND. If we don't or we're at the end -
 * return OK */
static int NI_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  NotContext *nc = ctx;

  // do not skip beyond max doc id
  if (docId > nc->maxDocId) {
    return INDEXREAD_EOF;
  }
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }

  // Get the child's last read docId
  t_docId childId = nc->child->LastDocId(nc->child->ctx);

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (childId > docId) {
    goto ok;
  }

  // If the child docId is the one we are looking for, it's an anti match!
  if (childId == docId) {
    nc->base.current->docId = docId;
    nc->lastDocId = docId;
    *hit = nc->base.current;
    return INDEXREAD_NOTFOUND;
  }

  // read the next entry from the child
  int rc = nc->child->SkipTo(nc->child->ctx, docId, hit);

  // OK means not found
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }

ok:
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->base.current->docId = docId;
  nc->lastDocId = docId;
  *hit = nc->base.current;
  return INDEXREAD_OK;
}

typedef struct NI_CriteriaTesterCtx{
  IndexCriteriaTester* child;
}NI_CriteriaTesterCtx;

static int NI_TextCriteria(void *ctx, t_docId id){
  NI_CriteriaTesterCtx* ctc = ctx;
  return !ctc->child->TextCriteria(ctc->child->ctx, id);
}
static void NI_TextCriteriaFree(struct IndexCriteriaTester* ct){
  NI_CriteriaTesterCtx* ctc = ct->ctx;
  ctc->child->Free(ctc->child);
  rm_free(ctc);
  rm_free(ct);
}

static IndexCriteriaTester* NI_GetCriteriaTester(void *ctx){
  NotContext *nc = ctx;
  NI_CriteriaTesterCtx* ctc = rm_malloc(sizeof(*ctc));
  ctc->child = nc->base.GetCriteriaTester(nc->base.ctx);
  IndexCriteriaTester* ct = rm_malloc(sizeof(*ct));
  ct->ctx = ctc;
  ct->TextCriteria = NI_TextCriteria;
  ct->Free = NI_TextCriteriaFree;
  return ct;
}

static size_t NI_ExpectedResultsAmount(void *ctx){
  NotContext *nc = ctx;
  return nc->maxDocId;
}

static int NI_ReadUnsorted(void *ctx, RSIndexResult **hit) {
  NotContext *nc = ctx;
  while(nc->lastDocId > nc->maxDocId){
    if(!nc->childCT->TextCriteria(nc->childCT->ctx, nc->lastDocId)){
      nc->base.current->docId = nc->lastDocId;
      *hit = nc->base.current;
      ++nc->lastDocId;
      return INDEXREAD_OK;
    }
    ++nc->lastDocId;
  }
  return INDEXREAD_EOF;
}

/* Read from a NOT iterator. This is applicable only if the only or leftmost node of a query is a
 * NOT node. We simply read until max docId, skipping docIds that exist in the child*/
static int NI_ReadSorted(void *ctx, RSIndexResult **hit) {
  NotContext *nc = ctx;
  if (nc->lastDocId > nc->maxDocId) return INDEXREAD_EOF;

  RSIndexResult *cr = NULL;
  // if we have a child, get the latest result from the child
  if (nc->child) {
    cr = IITER_CURRENT_RECORD(nc->child);

    if (cr == NULL || cr->docId == 0) {
      nc->child->Read(nc->child->ctx, &cr);
    }
  }

  // advance our reader by one, and let's test if it's a valid value or not
  nc->base.current->docId++;

  // If we don't have a child result, or the child result is ahead of the current counter,
  // we just increment our virtual result's id until we hit the child result's
  // in which case we'll read from the child and bypass it by one.
  if (cr == NULL || cr->docId > nc->base.current->docId) {
    goto ok;
  }

  while (cr->docId == nc->base.current->docId) {
    // advance our docId to the next possible id
    nc->base.current->docId++;

    // read the next entry from the child
    if (nc->child->Read(nc->child->ctx, &cr) == INDEXREAD_EOF) {
      break;
    }
  }

  // make sure we did not overflow
  if (nc->base.current->docId > nc->maxDocId) {
    return INDEXREAD_EOF;
  }

ok:
  // Set the next entry and return ok
  nc->lastDocId = nc->base.current->docId;
  if (hit) *hit = nc->base.current;
  ++nc->len;

  return INDEXREAD_OK;
}

/* We always have next, in case anyone asks... ;) */
static int NI_HasNext(void *ctx) {
  NotContext *nc = ctx;

  return nc->lastDocId <= nc->maxDocId;
}

/* Our len is the child's len? TBD it might be better to just return 0 */
static size_t NI_Len(void *ctx) {
  NotContext *nc = ctx;
  return nc->len;
}

/* Last docId */
static t_docId NI_LastDocId(void *ctx) {
  NotContext *nc = ctx;

  return nc->lastDocId;
}

IndexIterator *NewNotIterator(IndexIterator *it, t_docId maxDocId, double weight) {

  NotContext *nc = malloc(sizeof(*nc));
  nc->base.current = NewVirtualResult(weight);
  nc->base.current = NewVirtualResult(weight);
  nc->base.current->fieldMask = RS_FIELDMASK_ALL;
  nc->base.current->docId = 0;
  nc->child = it;
  nc->childCT = NULL;
  nc->lastDocId = 0;
  nc->maxDocId = maxDocId;
  nc->len = 0;
  nc->weight = weight;

  IndexIterator *ret = &nc->base;
  ret->ctx = nc;
  ret->GetCriteriaTester = NI_GetCriteriaTester;
  ret->ExpectedResultsAmount = NI_ExpectedResultsAmount;
  ret->Free = NI_Free;
  ret->HasNext = NI_HasNext;
  ret->LastDocId = NI_LastDocId;
  ret->Len = NI_Len;
  ret->Read = NI_ReadSorted;
  ret->SkipTo = NI_SkipTo;
  ret->Abort = NI_Abort;
  ret->Rewind = NI_Rewind;
  ret->GetCurrent = NULL;
  ret->mode = MODE_SORTED;

  if(nc->child && nc->child->mode == MODE_UNSORTED){
    nc->childCT = nc->child->GetCriteriaTester(nc->child->ctx);
    ret->Read = NI_ReadUnsorted;
  }

  return ret;
}

/**********************************************************
 * Optional clause iterator
 **********************************************************/

typedef struct {
  IndexIterator base;
  IndexIterator *child;
  IndexCriteriaTester* childCT;
  RSIndexResult *virt;
  t_fieldMask fieldMask;
  t_docId lastDocId;
  t_docId maxDocId;
  double weight;
} OptionalMatchContext, OptionalIterator;

static void OI_Free(IndexIterator *it) {
  OptionalMatchContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  if(nc->childCT){
    nc->childCT->Free(nc->childCT);
  }
  IndexResult_Free(nc->virt);
  free(it);
}

static int OI_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  OptionalMatchContext *nc = ctx;
  if (nc->lastDocId > nc->maxDocId) return INDEXREAD_EOF;
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }
  RSIndexResult *res = IITER_CURRENT_RECORD(nc->child);

  // if the child's current is already at our docId - just copy it to our current and hit's
  if (docId == (nc->lastDocId = res->docId)) {
    *hit = nc->base.current = res;
    return INDEXREAD_OK;
  }
  // read the next entry from the child
  int rc = nc->child->SkipTo(nc->child->ctx, docId, &nc->base.current);

  // OK means ok - pass the entry with the value
  if (rc == INDEXREAD_OK) {
    *hit = nc->base.current;
    return INDEXREAD_OK;
  }

ok:

  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->base.current = nc->virt;
  nc->lastDocId = nc->base.current->docId = docId;
  *hit = nc->base.current;
  return INDEXREAD_OK;
}

static int OI_TextCriteria(void *ctx, t_docId id){
  return 1;
}

static void OI_TextCriteriaFree(struct IndexCriteriaTester* ct){
  rm_free(ct);
}

static IndexCriteriaTester* OI_GetCriteriaTester(void *ctx){
  IndexCriteriaTester* tester = rm_malloc(sizeof(*tester));
  tester->ctx = NULL;
  tester->TextCriteria = OI_TextCriteria;
  tester->Free = OI_TextCriteriaFree;
  return tester;
}

static size_t OI_ExpectedResultsAmount(void *ctx){
  OptionalMatchContext *nc = ctx;
  return nc->maxDocId;
}

static int OI_ReadUnsorted(void *ctx, RSIndexResult **hit) {
  OptionalMatchContext *nc = ctx;
  if (nc->lastDocId >= nc->maxDocId) return INDEXREAD_EOF;
  nc->lastDocId++;
  nc->base.current = nc->virt;
  nc->base.current->docId = nc->lastDocId;
  *hit = nc->base.current;
  if(nc->childCT->TextCriteria(nc->childCT->ctx, nc->lastDocId)){
    nc->base.current->weight = nc->weight * 2; // we increase the weight cause we found the id
  }else{
    nc->base.current->weight = nc->weight * 2; // we do increase the weight cause id was not found
  }
  return INDEXREAD_OK;
}

/* Read has no meaning in the sense of an OPTIONAL iterator, so we just read the next record from
 * our child */
static int OI_ReadSorted(void *ctx, RSIndexResult **hit) {
  OptionalMatchContext *nc = ctx;
  if (nc->lastDocId >= nc->maxDocId) return INDEXREAD_EOF;
  nc->lastDocId++;
  if(!nc->child->current ||
      nc->lastDocId >  nc->child->current->docId){
    nc->child->Read(nc->child->ctx, &nc->base.current);
  }
  if(nc->lastDocId ==  nc->child->current->docId){
    *hit =  nc->child->current;
  }else{
    nc->base.current = nc->virt;
    nc->base.current->docId = nc->lastDocId;
    *hit = nc->base.current;
  }
  return INDEXREAD_OK;
}

/* We always have next, in case anyone asks... ;) */
static int OI_HasNext(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return (nc->lastDocId <= nc->maxDocId);
}

static void OI_Abort(void *ctx) {
  OptionalMatchContext *nc = ctx;
  if (nc->child) {
    nc->child->Abort(nc->child->ctx);
  }
}

/* Our len is the child's len? TBD it might be better to just return 0 */
static size_t OI_Len(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return nc->child ? nc->child->Len(nc->child->ctx) : 0;
}

/* Last docId */
static t_docId OI_LastDocId(void *ctx) {
  OptionalMatchContext *nc = ctx;

  return nc->lastDocId;
}

static void OI_Rewind(void *ctx) {
  OptionalMatchContext *nc = ctx;
  nc->lastDocId = 0;
  nc->virt->docId = 0;
  if (nc->child) {
    nc->child->Rewind(nc->child->ctx);
  }
}

IndexIterator *NewOptionalIterator(IndexIterator *it, t_docId maxDocId, double weight) {

  OptionalMatchContext *nc = malloc(sizeof(*nc));
  nc->virt = NewVirtualResult(weight);
  nc->virt->freq = 0;
  nc->virt->fieldMask = RS_FIELDMASK_ALL;
  nc->base.current = nc->virt;
  nc->child = it;
  nc->childCT = NULL;
  nc->lastDocId = 0;
  nc->maxDocId = maxDocId;
  nc->weight = weight;

  IndexIterator *ret = &nc->base;
  ret->ctx = nc;
  ret->GetCriteriaTester = OI_GetCriteriaTester;
  ret->ExpectedResultsAmount = OI_ExpectedResultsAmount;
  ret->Free = OI_Free;
  ret->HasNext = OI_HasNext;
  ret->LastDocId = OI_LastDocId;
  ret->Len = OI_Len;
  ret->Read = OI_ReadSorted;
  ret->SkipTo = OI_SkipTo;
  ret->Abort = OI_Abort;
  ret->Rewind = OI_Rewind;
  ret->mode = MODE_SORTED;

  if(nc->child->mode == MODE_UNSORTED){
    nc->childCT = nc->child->GetCriteriaTester(nc->child->ctx);
    ret->Read = OI_ReadUnsorted;
  }

  return ret;
}

/* Wildcard iterator, matchin ALL documents in the index. This is used for one thing only -
 * purely negative queries. If the root of the query is a negative expression, we cannot process
 * it
 * without a positive expression. So we create a wildcard iterator that basically just iterates
 * all
 * the incremental document ids, and matches every skip within its range. */
typedef struct {
  IndexIterator base;
  t_docId topId;
  t_docId current;
} WildcardIterator, WildcardIteratorCtx;

/* Free a wildcard iterator */
static void WI_Free(IndexIterator *it) {

  WildcardIteratorCtx *nc = it->ctx;
  IndexResult_Free(CURRENT_RECORD(nc));
  free(it);
}

/* Read reads the next consecutive id, unless we're at the end */
static int WI_Read(void *ctx, RSIndexResult **hit) {
  WildcardIteratorCtx *nc = ctx;
  if (nc->current > nc->topId) {
    return INDEXREAD_EOF;
  }
  CURRENT_RECORD(nc)->docId = nc->current++;
  if (hit) {
    *hit = CURRENT_RECORD(nc);
  }
  return INDEXREAD_OK;
}

/* Skipto for wildcard iterator - always succeeds, but this should normally not happen as it has
 * no
 * meaning */
static int WI_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  // printf("WI_Skipto %d\n", docId);
  WildcardIteratorCtx *nc = ctx;

  if (nc->current > nc->topId) return INDEXREAD_EOF;

  if (docId == 0) return WI_Read(ctx, hit);

  nc->current = docId;
  CURRENT_RECORD(nc)->docId = docId;
  if (hit) {
    *hit = CURRENT_RECORD(nc);
  }
  return INDEXREAD_OK;
}

static void WI_Abort(void *ctx) {
  WildcardIteratorCtx *nc = ctx;
  nc->current = nc->topId + 1;
}

/* We always have next, in case anyone asks... ;) */
static int WI_HasNext(void *ctx) {
  WildcardIteratorCtx *nc = ctx;

  return nc->current <= nc->topId;
}

/* Our len is the len of the index... */
static size_t WI_Len(void *ctx) {
  WildcardIteratorCtx *nc = ctx;
  return nc->topId;
}

/* Last docId */
static t_docId WI_LastDocId(void *ctx) {
  WildcardIteratorCtx *nc = ctx;

  return nc->current;
}

static void WI_Rewind(void *p) {
  WildcardIteratorCtx *ctx = p;
  ctx->current = 1;
}

/* Create a new wildcard iterator */
IndexIterator *NewWildcardIterator(t_docId maxId) {
  WildcardIteratorCtx *c = malloc(sizeof(*c));
  c->current = 1;
  c->topId = maxId;

  CURRENT_RECORD(c) = NewVirtualResult(1);
  CURRENT_RECORD(c)->freq = 1;
  CURRENT_RECORD(c)->fieldMask = RS_FIELDMASK_ALL;

  IndexIterator *ret = &c->base;
  ret->ctx = c;
  ret->Free = WI_Free;
  ret->HasNext = WI_HasNext;
  ret->LastDocId = WI_LastDocId;
  ret->Len = WI_Len;
  ret->Read = WI_Read;
  ret->SkipTo = WI_SkipTo;
  ret->Abort = WI_Abort;
  ret->Rewind = WI_Rewind;
  ret->GetCurrent = NULL;
  return ret;
}
