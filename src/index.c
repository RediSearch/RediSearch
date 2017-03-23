#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include "spec.h"
#include <math.h>
#include <assert.h>
#include <sys/param.h>
#include "rmalloc.h"

inline t_docId UI_LastDocId(void *ctx) {
  return ((UnionContext *)ctx)->minDocId;
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt) {
  // create union context
  UnionContext *ctx = calloc(1, sizeof(UnionContext));
  ctx->its = its;
  ctx->num = num;
  ctx->docTable = dt;
  ctx->atEnd = 0;
  ctx->docIds = calloc(num, sizeof(t_docId));
  ctx->current = NewUnionResult(num);
  ctx->len = 0;
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

int UI_Read(void *ctx, RSIndexResult **hit) {
  UnionContext *ui = ctx;
  // nothing to do
  if (ui->num == 0 || ui->atEnd) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  int numActive = 0;
  AggregateResult_Reset(&ui->current->agg);

  do {
    // find the minimal iterator
    t_docId minDocId = __UINT32_MAX__;
    int minIdx = -1;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    for (int i = 0; i < ui->num; i++) {
      IndexIterator *it = ui->its[i];
      if (it == NULL) continue;
      RSIndexResult *res = it->Current(it->ctx);

      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      while (ui->docIds[i] <= ui->minDocId && rc != INDEXREAD_EOF) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          rc = it->Read(it->ctx, &res);
        }
        ui->docIds[i] = res->docId;
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      }

      if (rc == INDEXREAD_OK && res->docId < minDocId) {
        minDocId = res->docId;
        minIdx = i;
      }
    }

    // take the minimum entry and yield it
    if (minIdx != -1) {
      // return UI_SkipTo(ctx, ui->currentHits[minIdx].docId, hit);
      // if (hit) {
      //   AggregateResult_Reset(&hit->agg);

      //   // AggregateResult_AddChild(hit, &ui->currentHits[minIdx]);
      // }
      printf("UI %p read docId %d OK\n", ui, ui->docIds[minIdx]);
      UI_SkipTo(ui, ui->docIds[minIdx], hit);
      // return INDEXREAD_OK;

      ui->minDocId = ui->docIds[minIdx];
      ui->len++;
      // printf("UI %p read docId %d OK\n", ui, ui->minDocId);
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
int UI_SkipTo(void *ctx, u_int32_t docId, RSIndexResult **hit) {
  if (docId == 0) {
    return UI_Read(ctx, hit);
  }
  UnionContext *ui = ctx;
  printf("UI %p skipto %d\n", ui, docId);

  AggregateResult_Reset(&ui->current->agg);
  int n = 0;
  int found = 0;
  int rc = INDEXREAD_EOF;
  t_docId minDocId = __UINT32_MAX__;
  // skip all iterators to docId
  for (int i = 0; i < ui->num; i++) {
    // this happens for non existent words
    IndexIterator *it = ui->its[i];
    if (it == NULL) continue;
    RSIndexResult *res = it->Current(it->ctx);

    if (ui->docIds[i] < docId || docId == 0) {

      if ((rc = it->SkipTo(it->ctx, docId, &res)) == INDEXREAD_EOF) {
        continue;
      }

    } else {
      rc = ui->docIds[i] == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    if (ui->docIds[i] && rc != INDEXREAD_EOF) {
      minDocId = MIN(ui->docIds[i], minDocId);
    }

    // we found a hit - no need to continue
    if (rc == INDEXREAD_OK) {
      if (hit) {
        // AggregateResult_Reset(&ui->currentHits[i].agg);
        AggregateResult_AddChild(ui->current, res);
      }
      ui->minDocId = res->docId;
      printf("UI %p skipped to docId %d OK!!!\n", ui, docId);
      found++;
    }
    n++;
  }

  // all iterators are at the end
  if (n == 0) {
    ui->atEnd = 1;
    return INDEXREAD_EOF;
  }

  *hit = ui->current;
  if (found > 0) {
    return INDEXREAD_OK;
  }

  ui->minDocId = minDocId;
  AggregateResult_Reset(&(*hit)->agg);
  (*hit)->docId = minDocId;
  printf("UI %p skipped to docId %d NOT FOUND, minDocId now %d\n", ui, docId, ui->minDocId);
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
  free(ui->current);
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
  free(ui->current);
  free(ui->its);
  free(it->ctx);
  free(it);
}

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, DocTable *dt, u_char fieldMask,
                                   int maxSlop, int inOrder) {

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

int II_SkipTo(void *ctx, u_int32_t docId, RSIndexResult **hit) {

  /* A seek with docId 0 is equivalent to a read */
  if (docId == 0) {
    return II_Read(ctx, hit);
  }
  IntersectContext *ic = ctx;
  AggregateResult_Reset(&ic->current->agg);

  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < ic->num; i++) {
    IndexIterator *it = ic->its[i];
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
    (*hit)->docId = ic->lastDocId;
  }

  return INDEXREAD_NOTFOUND;
}

int II_Next(void *ctx) {
  return II_Read(ctx, NULL);
}

int II_Read(void *ctx, RSIndexResult **hit) {
  IntersectContext *ic = (IntersectContext *)ctx;

  if (ic->num == 0) return INDEXREAD_EOF;
  AggregateResult_Reset(&ic->current->agg);

  int nh = 0;
  int i = 0;

  do {
    nh = 0;
    AggregateResult_Reset(&ic->current->agg);

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
      // printf("II %p HIT @ %d\n", ic, ic->currentHits[0].docId);
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
