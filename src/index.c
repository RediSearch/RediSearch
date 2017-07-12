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

inline t_docId UI_LastDocId(void *ctx) {
  return ((UnionContext *)ctx)->minDocId;
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt, int quickExit) {
  // create union context
  UnionContext *ctx = calloc(1, sizeof(UnionContext));
  ctx->its = its;
  ctx->num = num;
  ctx->docTable = dt;
  ctx->atEnd = 0;
  ctx->docIds = calloc(num, sizeof(t_docId));
  ctx->current = NewUnionResult(num);
  ctx->len = 0;
  ctx->quickExit = quickExit;
  // bind the union iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = UI_LastDocId;
  it->Current = UI_Current;
  it->Read = UI_Read;
  it->SkipTo = UI_SkipTo;
  it->HasNext = UI_HasNext;
  it->Free = UnionIterator_Free;
  it->Len = UI_Len;
  return it;
}

RSIndexResult *UI_Current(void *ctx) {
  return ((UnionContext *)ctx)->current;
}

inline int UI_Read(void *ctx, RSIndexResult **hit) {
  UnionContext *ui = ctx;
  // nothing to do
  if (ui->num == 0 || ui->atEnd) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  AggregateResult_Reset(ui->current);

  do {

    // find the minimal iterator
    t_docId minDocId = __UINT32_MAX__;
    int minIdx = -1;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    for (int i = 0; i < ui->num; i++) {
      IndexIterator *it = ui->its[i];
      if (it == NULL || !it->HasNext(it->ctx)) continue;
      RSIndexResult *res = it->Current(it->ctx);

      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      // printf("ui->docIds[%d]: %d, ui->minDocId: %d\n", i, ui->docIds[i], ui->minDocId);
      while (ui->docIds[i] <= ui->minDocId && rc != INDEXREAD_EOF) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          rc = it->Read(it->ctx, &res);
          ui->docIds[i] = res->docId;
        }
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      } else {
        continue;
      }

      if (rc == INDEXREAD_OK && res->docId <= minDocId) {
        minDocId = res->docId;
        minIdx = i;
      }
    }

    // take the minimum entry and collect all results matching to it
    if (minIdx != -1) {

      UI_SkipTo(ui, ui->docIds[minIdx], hit);
      // return INDEXREAD_OK;
      ui->minDocId = ui->docIds[minIdx];
      ui->len++;
      return INDEXREAD_OK;
    }

  } while (numActive > 0);
  ui->atEnd = 1;

  return INDEXREAD_EOF;
}

int UI_Next(void *ctx) {
  // RSIndexResult h = NewIndexResult();
  return UI_Read(ctx, NULL);
}

// return 1 if at least one sub iterator has next
int UI_HasNext(void *ctx) {

  UnionContext *u = ctx;
  return !u->atEnd;
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
int UI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {
  UnionContext *ui = ctx;

  // printf("UI %p skipto %d\n", ui, docId);

  if (docId == 0) {
    return UI_Read(ctx, hit);
  }

  if (ui->atEnd) {
    return INDEXREAD_EOF;
  }

  AggregateResult_Reset(ui->current);
  int numActive = 0;
  int found = 0;
  int rc = INDEXREAD_EOF;
  const int num = ui->num;
  const int quickExit = ui->quickExit;
  t_docId minDocId = __UINT32_MAX__;
  IndexIterator *it;
  RSIndexResult *res;
  // skip all iterators to docId
  for (int i = 0; i < num; i++) {
    // this happens for non existent words
    if (NULL == (it = ui->its[i])) continue;
    if (!it->HasNext(it->ctx)) continue;

    res = NULL;

    // If the requested docId is larger than the last read id from the iterator,
    // we need to read an entry from the iterator, seeking to this docId
    if (ui->docIds[i] < docId) {
      if ((rc = it->SkipTo(it->ctx, docId, &res)) == INDEXREAD_EOF) {
        continue;
      }
      ui->docIds[i] = res->docId;

    } else {
      // if the iterator is at an end - we avoid reading the entry
      // in this case, we are either past or at the requested docId, no need to actually read
      rc = (ui->docIds[i] == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    // if we've read successfully, update the minimal docId we've found
    if (ui->docIds[i] && rc != INDEXREAD_EOF) {
      minDocId = MIN(ui->docIds[i], minDocId);
    }

    // we found a hit - continue to all results matching the same docId
    if (rc == INDEXREAD_OK) {

      // add the result to the aggregate result we are holding
      if (hit) {
        AggregateResult_AddChild(ui->current, res ? res : it->Current(it->ctx));
      }
      ui->minDocId = ui->docIds[i];
      ++found;
    }
    ++numActive;
    // If we've found a single entry and we are iterating in quick exit mode - exit now
    if (found && quickExit) break;
  }

  // all iterators are at the end
  if (numActive == 0) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  // copy our aggregate to the upstream hit

  // if we only have one record, we cane just push it upstream not wrapped in our own record,
  // this will speed up evaluating offsets
  if (found == 1 && ui->current->agg.numChildren == 1) {
    *hit = ui->current->agg.children[0];
  } else {
    *hit = ui->current;
  }
  if (found > 0) {
    return INDEXREAD_OK;
  }

  // not found...
  ui->minDocId = minDocId;
  return INDEXREAD_NOTFOUND;
}

void UnionIterator_Free(IndexIterator *it) {
  if (it == NULL) return;

  UnionContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i]) {
      ui->its[i]->Free(ui->its[i]);
    }
  }

  free(ui->docIds);
  IndexResult_Free(ui->current);
  free(ui->its);
  free(ui);
  free(it);
}

size_t UI_Len(void *ctx) {
  return ((UnionContext *)ctx)->len;
}

void IntersectIterator_Free(IndexIterator *it) {
  if (it == NULL) return;
  IntersectContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i] != NULL) {
      ui->its[i]->Free(ui->its[i]);
    }
    // IndexResult_Free(&ui->currentHits[i]);
  }
  free(ui->docIds);
  IndexResult_Free(ui->current);
  free(ui->its);
  free(it->ctx);
  free(it);
}

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *dt,
                                   t_fieldMask fieldMask, int maxSlop, int inOrder) {

  IntersectContext *ctx = calloc(1, sizeof(IntersectContext));
  ctx->its = its;
  ctx->num = num;
  ctx->lastDocId = 0;
  ctx->len = 0;
  ctx->maxSlop = maxSlop;
  ctx->inOrder = inOrder;
  ctx->fieldMask = fieldMask;
  ctx->atEnd = 0;
  ctx->docIds = calloc(num, sizeof(t_docId));
  ctx->current = NewIntersectResult(num);
  ctx->docTable = dt;

  // bind the iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = II_LastDocId;
  it->Read = II_Read;
  it->SkipTo = II_SkipTo;
  it->Current = II_Current;
  it->HasNext = II_HasNext;
  it->Len = II_Len;
  it->Free = IntersectIterator_Free;
  return it;
}

RSIndexResult *II_Current(void *ctx) {
  return ((IntersectContext *)ctx)->current;
}

int II_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {

  /* A seek with docId 0 is equivalent to a read */
  if (docId == 0) {
    return II_Read(ctx, hit);
  }
  IntersectContext *ic = ctx;
  AggregateResult_Reset(ic->current);
  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < ic->num; i++) {
    IndexIterator *it = ic->its[i];

    if (!it || !it->HasNext(it->ctx)) return INDEXREAD_EOF;

    RSIndexResult *res = it->Current(it->ctx);
    rc = INDEXREAD_OK;

    // only read if we're not already at the final position
    if (ic->docIds[i] != ic->lastDocId || ic->lastDocId == 0) {
      rc = it->SkipTo(it->ctx, docId, &res);
      if (rc != INDEXREAD_EOF) {
        ic->docIds[i] = res->docId;
      }
    }

    if (rc == INDEXREAD_EOF) {
      // we are at the end!
      ic->atEnd = 1;
      return rc;
    } else if (rc == INDEXREAD_OK) {
      // YAY! found!
      AggregateResult_AddChild(ic->current, res);
      ic->lastDocId = docId;

      ++nfound;
    } else if (ic->docIds[i] > ic->lastDocId) {
      ic->lastDocId = ic->docIds[i];
      break;
    }
  }

  if (nfound == ic->num) {
    if (hit) {
      *hit = ic->current;
    }
    return INDEXREAD_OK;
  }
  // we add the actual last doc id we read, so if anyone is looking at what we returned it would
  // look sane
  if (hit) {
    (*hit)->docId = 0;  // ic->lastDocId;
  }

  return INDEXREAD_NOTFOUND;
}

int II_Next(void *ctx) {
  return II_Read(ctx, NULL);
}

int II_Read(void *ctx, RSIndexResult **hit) {
  IntersectContext *ic = (IntersectContext *)ctx;

  if (ic->num == 0) return INDEXREAD_EOF;
  AggregateResult_Reset(ic->current);

  int nh = 0;
  int i = 0;

  do {

    nh = 0;
    AggregateResult_Reset(ic->current);

    for (i = 0; i < ic->num; i++) {
      IndexIterator *it = ic->its[i];

      if (!it) goto eof;

      RSIndexResult *h = it->Current(it->ctx);
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
        AggregateResult_AddChild(ic->current, h);
      } else {
        ic->lastDocId++;
      }
    }

    if (nh == ic->num) {
      // printf("II %p HIT @ %d\n", ic, ic->current->docId);
      // sum up all hits
      if (hit != NULL) {
        *hit = ic->current;
      }
      // advance the doc id so next time we'll read a new record
      ic->lastDocId++;

      // // make sure the flags are matching.
      if ((ic->current->fieldMask & ic->fieldMask) == 0) {
        continue;
      }

      // If we need to match slop and order, we do it now, and possibly skip the result
      if (ic->maxSlop >= 0) {
        if (!IndexResult_IsWithinRange(ic->current, ic->maxSlop, ic->inOrder)) {
          continue;
        }
      }

      ic->len++;
      return INDEXREAD_OK;
    }
  } while (1);
eof:
  ic->atEnd = 1;
  return INDEXREAD_EOF;
}

int II_HasNext(void *ctx) {
  IntersectContext *ic = ctx;
  // printf("%p %d\n", ic, ic->atEnd);
  return ic->atEnd;
}

t_docId II_LastDocId(void *ctx) {
  return ((IntersectContext *)ctx)->lastDocId;
}

size_t II_Len(void *ctx) {
  return ((IntersectContext *)ctx)->len;
}

void NI_Free(IndexIterator *it) {

  NotContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  IndexResult_Free(nc->current);
  free(it->ctx);
  free(it);
}

/* SkipTo for NOT iterator. If we have a match - return NOTFOUND. If we don't or we're at the end -
 * return OK */
int NI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {

  NotContext *nc = ctx;
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }
  nc->lastDocId = nc->child->LastDocId(nc->child->ctx);

  // if the child's iterator is ahead of the current docId, we can assume the docId is not there and
  // return a pseudo okay
  if (nc->lastDocId > docId) {
    goto ok;
  }

  // if the last read docId is the one we are looking for, it's an anti match!
  if (nc->lastDocId == docId) {
    return INDEXREAD_NOTFOUND;
  }

  // read the next entry
  int rc = nc->child->SkipTo(nc->child->ctx, docId, hit);

  // OK means not found
  if (rc == INDEXREAD_OK) {
    return INDEXREAD_NOTFOUND;
  }

ok:
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->current->docId = docId;
  *hit = nc->current;
  return INDEXREAD_OK;
}

/* Read has no meaning in the sense of a NOT iterator, so we just return EOF */
int NI_Read(void *ctx, RSIndexResult **hit) {
  return INDEXREAD_EOF;
}

/* We always have next, in case anyone asks... ;) */
int NI_HasNext(void *ctx) {
  return 1;
}

/* Return the current hit */
RSIndexResult *NI_Current(void *ctx) {
  NotContext *nc = ctx;
  return nc->current;
}

/* Our len is the child's len? TBD it might be better to just return 0 */
size_t NI_Len(void *ctx) {
  NotContext *nc = ctx;
  return nc->child ? nc->child->Len(nc->child->ctx) : 0;
}

/* Last docId */
t_docId NI_LastDocId(void *ctx) {
  NotContext *nc = ctx;

  return nc->lastDocId;
}

IndexIterator *NewNotIterator(IndexIterator *it) {

  NotContext *nc = malloc(sizeof(*nc));
  nc->current = NewVirtualResult();
  nc->current->fieldMask = RS_FIELDMASK_ALL;
  nc->child = it;
  nc->lastDocId = 0;

  IndexIterator *ret = malloc(sizeof(*it));
  ret->ctx = nc;
  ret->Current = NI_Current;
  ret->Free = NI_Free;
  ret->HasNext = NI_HasNext;
  ret->LastDocId = NI_LastDocId;
  ret->Len = NI_Len;
  ret->Read = NI_Read;
  ret->SkipTo = NI_SkipTo;
  return ret;
}

/**********************************************************
 * Optional clause iterator
 **********************************************************/

void OI_Free(IndexIterator *it) {

  OptionalMatchContext *nc = it->ctx;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  IndexResult_Free(nc->virt);
  free(it->ctx);
  free(it);
}

/* SkipTo for NOT iterator. If we have a match - return NOTFOUND. If we don't or we're at the end -
 * return OK */
int OI_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {

  OptionalMatchContext *nc = ctx;
  // If we don't have a child it means the sub iterator is of a meaningless expression.
  // So negating it means we will always return OK!
  if (!nc->child) {
    goto ok;
  }
  RSIndexResult *res = nc->child->Current(nc->child->ctx);
  // if the child's current is already at our docId - just copy it to our current and hit's
  if (docId == (nc->lastDocId = res->docId)) {
    *hit = nc->current = res;
    return INDEXREAD_OK;
  }
  // read the next entry from the child
  int rc = nc->child->SkipTo(nc->child->ctx, docId, &nc->current);

  // OK means ok - pass the entry with the value
  if (rc == INDEXREAD_OK) {
    *hit = nc->current;
    return INDEXREAD_OK;
  }

ok:
  nc->current = nc->virt;
  // NOT FOUND or end means OK. We need to set the docId on the hit we will bubble up
  nc->lastDocId = nc->current->docId = docId;
  *hit = nc->current;
  return INDEXREAD_OK;
}

/* Read has no meaning in the sense of an OPTIONAL iterator, so we just read the next record from
 * our child */
int OI_Read(void *ctx, RSIndexResult **hit) {
  OptionalMatchContext *nc = ctx;
  if (nc->child) {
    if (nc->child->Read(nc->child->ctx, &nc->current) == INDEXREAD_OK) {
      if (hit) {
        *hit = nc->current;
      }
      return INDEXREAD_OK;
    }
  }
  return INDEXREAD_EOF;
}

/* We always have next, in case anyone asks... ;) */
int OI_HasNext(void *ctx) {
  return 1;
}

/* Return the current hit */
RSIndexResult *OI_Current(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return nc->current;
}

/* Our len is the child's len? TBD it might be better to just return 0 */
size_t OI_Len(void *ctx) {
  OptionalMatchContext *nc = ctx;
  return nc->child ? nc->child->Len(nc->child->ctx) : 0;
}

/* Last docId */
t_docId OI_LastDocId(void *ctx) {
  OptionalMatchContext *nc = ctx;

  return nc->lastDocId;
}

IndexIterator *NewOptionalIterator(IndexIterator *it) {

  OptionalMatchContext *nc = malloc(sizeof(*nc));
  nc->virt = NewVirtualResult();
  nc->virt->fieldMask = RS_FIELDMASK_ALL;
  nc->current = nc->virt;
  nc->child = it;
  nc->lastDocId = 0;

  IndexIterator *ret = malloc(sizeof(*it));
  ret->ctx = nc;
  ret->Current = OI_Current;
  ret->Free = OI_Free;
  ret->HasNext = OI_HasNext;
  ret->LastDocId = OI_LastDocId;
  ret->Len = OI_Len;
  ret->Read = OI_Read;
  ret->SkipTo = OI_SkipTo;
  return ret;
}
