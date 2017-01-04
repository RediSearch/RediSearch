#include "forward_index.h"
#include "index.h"
#include "varint.h"
#include <math.h>
#include <sys/param.h>
inline int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;
  // LG_DEBUG("ir %p size %d, offset %d. has next? %d\n", ir, ir->header.size,
  // ir->buf->offset,
  // ir->header.size > ir->buf->offset);
  return ir->header.size > ir->buf->offset;
}

inline int IR_GenericRead(IndexReader *ir, t_docId *docId, float *freq, u_char *flags,
                          VarintVector *offsets) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }

  *docId = ReadVarint(ir->buf) + ir->lastId;
  int len = ReadVarint(ir->buf);
  size_t pos = BufferOffset(ir->buf);

  int quantizedScore = ReadVarint(ir->buf);
  if (freq != NULL) {
    *freq = (float)(quantizedScore ? quantizedScore : 1) / FREQ_QUANTIZE_FACTOR;
    // printf("READ Quantized score %d, freq %f\n", quantizedScore, *freq);
  }

  BufferReadByte(ir->buf, (char *)flags);

  size_t offsetsLen = len - (BufferOffset(ir->buf) - pos);

  // If needed - read offset vectors
  if (offsets != NULL && !ir->singleWordMode) {
    offsets->cap = offsetsLen;
    offsets->data = ir->buf->pos;
    offsets->pos = offsets->data;
    offsets->ctx = NULL;
    offsets->offset = 0;
    offsets->type = BUFFER_READ;
  }

  BufferSkip(ir->buf, offsetsLen);
  ir->lastId = *docId;
  return INDEXREAD_OK;
}

inline int IR_TryRead(IndexReader *ir, t_docId *docId, t_docId expectedDocId) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }

  *docId = ReadVarint(ir->buf) + ir->lastId;
  int len = ReadVarint(ir->buf);

  ir->lastId = *docId;
  BufferSkip(ir->buf, len);

  if (*docId != expectedDocId && expectedDocId != 0) {
    return INDEXREAD_NOTFOUND;
  }

  return INDEXREAD_OK;
}

// inline double tfidf(float freq, u_int32_t docFreq) {
//   double idf = logb(
//       1.0F + TOTALDOCS_PLACEHOLDER / (docFreq ? docFreq : (double)1)); // IDF
//   // LG_DEBUG("FREQ: %f  IDF: %.04f, TFIDF: %f",freq, idf, freq*idf);
//   return freq * idf;
// }

int IR_Read(void *ctx, IndexResult *e) {
  float freq;
  IndexReader *ir = ctx;
  IndexRecord rec = {.term = ir->term};

  if (ir->useScoreIndex && ir->scoreIndex) {
    ScoreIndexEntry *ent = ScoreIndex_Next(ir->scoreIndex);
    if (ent == NULL) {
      return INDEXREAD_EOF;
    }

    IR_Seek(ir, ent->offset, ent->docId);
  }

  VarintVector *offsets = NULL;
  if (!ir->singleWordMode) {
    offsets = &rec.offsets;
  }

  int rc = IR_GenericRead(ir, &rec.docId, &rec.tf, &rec.flags, offsets);

  // add the record to the current result
  if (rc == INDEXREAD_OK) {
    if (!(rec.flags & ir->fieldMask)) {
      return INDEXREAD_NOTFOUND;
    }

    ++ir->len;

    IndexResult_PutRecord(e, &rec);
  }

  // LG_DEBUG("Read docId %d, rc %d", e->docId, rc);
  return rc;
}

int IR_Next(void *ctx) {
  static t_docId docId;
  // static float freq;
  static u_char flags;
  return IR_GenericRead(ctx, &docId, NULL, &flags, NULL);
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
  // LG_DEBUG("Seeking to %d, lastId %d", offset, docId);
  BufferSeek(ir->buf, offset);
  ir->lastId = docId;
}

// int IndexResult_LoadMetadata(IndexResult *h, DocTable *dt) {

//   int rc = 0;
//   if ((rc = DocTable_GetMetadata(dt, h->docId, &h->metadata)) ==
//       REDISMODULE_OK) {
//     h->hasMetadata = 1;
//   }
//   return rc;
// }

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF
if
at EOF
*/
int IR_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {
  IndexReader *ir = ctx;

  SkipEntry *ent = SkipIndex_Find(ir->skipIdx, docId, &ir->skipIdxPos);

  if (ent != NULL || ir->skipIdx == NULL || ir->skipIdx->len == 0 ||
      docId <= ir->skipIdx->entries[0].docId) {

    if (ent != NULL && ent->offset > BufferOffset(ir->buf)) {
      IR_Seek(ir, ent->offset, ent->docId);
    }

    int rc;
    t_docId lastId = ir->lastId, readId = 0;
    t_offset offset = ir->buf->offset;

    do {

      // do a quick-read until we hit or pass the desired document
      if ((rc = IR_TryRead(ir, &readId, docId)) == INDEXREAD_EOF) {
        return rc;
      }
      // rewind 1 document and re-read it...
      if (rc == INDEXREAD_OK || readId > docId) {
        IR_Seek(ir, offset, lastId);
        IR_Read(ir, hit);
        return rc;
      }
      lastId = readId;
      offset = ir->buf->offset;
    } while (rc != INDEXREAD_EOF);
  }

  return INDEXREAD_EOF;
}

size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;

  // in single word optimized mode we only know the size of the record from
  // the header.
  if (ir->singleWordMode) {
    return ir->header.numDocs;
  }

  // otherwise we use our counter
  return ir->len;
}

IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si, DocTable *dt,
                            int singleWordMode, u_char fieldMask) {
  return NewIndexReaderBuf(NewBuffer(data, datalen, BUFFER_READ), si, dt, singleWordMode, NULL,
                           fieldMask, NULL);
}

IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si, DocTable *dt, int singleWordMode,
                               ScoreIndex *sci, u_char fieldMask, Term *term) {
  IndexReader *ret = malloc(sizeof(IndexReader));
  ret->buf = buf;
  indexReadHeader(buf, &ret->header);
  ret->term = term;

  if (term) {
    // compute IDF based on num of docs in the header
    ret->term->idf = logb(
        1.0F + TOTALDOCS_PLACEHOLDER / (ret->header.numDocs ? ret->header.numDocs : (double)1));
  }

  ret->lastId = 0;
  ret->skipIdxPos = 0;
  ret->skipIdx = NULL;
  ret->docTable = dt;
  ret->len = 0;
  ret->singleWordMode = singleWordMode;
  // only use score index on single words, no field filter and large entries
  ret->useScoreIndex = sci != NULL && singleWordMode && fieldMask == 0xff &&
                       ret->header.numDocs > SCOREINDEX_DELETE_THRESHOLD;
  ret->scoreIndex = sci;
  // LG_DEBUG("Load offsets %d, si: %p", singleWordMode, si);
  ret->skipIdx = si;
  ret->fieldMask = fieldMask;

  return ret;
}

void IR_Free(IndexReader *ir) {
  membufferRelease(ir->buf);
  if (ir->scoreIndex) {
    ScoreIndex_Free(ir->scoreIndex);
  }
  SkipIndex_Free(ir->skipIdx);
  Term_Free(ir->term);
  free(ir);
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->Read = IR_Read;
  ri->SkipTo = IR_SkipTo;
  ri->LastDocId = IR_LastDocId;
  ri->HasNext = IR_HasNext;
  ri->Free = ReadIterator_Free;
  ri->Len = IR_NumDocs;
  return ri;
}

size_t IW_Len(IndexWriter *w) {
  return BufferLen(w->bw.buf);
}

void writeIndexHeader(IndexWriter *w) {
  size_t offset = w->bw.buf->offset;
  BufferSeek(w->bw.buf, 0);
  IndexHeader h = {offset, w->lastId, w->ndocs};
  LG_DEBUG(
      "Writing index header. offest %d , lastId %d, ndocs %d, will seek "
      "to %zd",
      h.size, h.lastId, w->ndocs, offset);
  w->bw.Write(w->bw.buf, &h, sizeof(IndexHeader));
  BufferSeek(w->bw.buf, offset);
}

IndexWriter *NewIndexWriter(size_t cap) {
  return NewIndexWriterBuf(NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE)),
                           NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE)),
                           NewScoreIndexWriter(NewBufferWriter(NewMemoryBuffer(2, BUFFER_WRITE))));
}

IndexWriter *NewIndexWriterBuf(BufferWriter bw, BufferWriter skipIdnexWriter,
                               ScoreIndexWriter siw) {
  IndexWriter *w = malloc(sizeof(IndexWriter));
  w->bw = bw;
  w->skipIndexWriter = skipIdnexWriter;
  w->ndocs = 0;
  w->lastId = 0;
  w->scoreWriter = siw;

  IndexHeader h = {0, 0, 0};
  if (indexReadHeader(w->bw.buf, &h)) {
    if (h.size > 0) {
      w->lastId = h.lastId;
      w->ndocs = h.numDocs;
      BufferSeek(w->bw.buf, h.size);

      return w;
    }
  }

  writeIndexHeader(w);
  BufferSeek(w->bw.buf, sizeof(IndexHeader));

  return w;
}

int indexReadHeader(Buffer *b, IndexHeader *h) {
  if (b->cap > sizeof(IndexHeader)) {
    BufferSeek(b, 0);
    return BufferRead(b, h, sizeof(IndexHeader));
  }
  return 0;
}

void IW_WriteSkipIndexEntry(IndexWriter *w) {
  SkipEntry se = {w->lastId, BufferOffset(w->bw.buf)};
  Buffer *b = w->skipIndexWriter.buf;

  u_int32_t num = 1 + (w->ndocs / SKIPINDEX_STEP);
  size_t off = b->offset;

  BufferSeek(b, 0);
  w->skipIndexWriter.Write(b, &num, sizeof(u_int32_t));

  if (off > 0) {
    BufferSeek(b, off);
  }
  w->skipIndexWriter.Write(b, &se, sizeof(SkipEntry));
}

/* Write a forward-index entry to an index writer */
size_t IW_WriteEntry(IndexWriter *w, ForwardIndexEntry *ent) {
  // VVW_Truncate(ent->vw);
  size_t ret = 0;
  VarintVector *offsets = ent->vw->bw.buf;

  ScoreIndexWriter_AddEntry(&w->scoreWriter, ent->freq, BufferOffset(w->bw.buf), w->lastId);
  // quantize the score to compress it to max 4 bytes
  // freq is between 0 and 1
  int quantizedScore = floorl(ent->freq * ent->docScore * (double)FREQ_QUANTIZE_FACTOR);

  size_t offsetsSz = offsets->offset;
  // // calculate the overall len
  size_t len = varintSize(quantizedScore) + 1 + offsetsSz;

  // Write docId
  ret += WriteVarint(ent->docId - w->lastId, &w->bw);
  // encode len

  ret += WriteVarint(len, &w->bw);
  // encode freq
  ret += WriteVarint(quantizedScore, &w->bw);
  // encode flags
  ret += w->bw.Write(w->bw.buf, &ent->flags, 1);
  // write offsets size
  // ret += WriteVarint(offsetsSz, &w->bw);
  ret += w->bw.Write(w->bw.buf, offsets->data, offsetsSz);

  w->lastId = ent->docId;
  if (w->ndocs % SKIPINDEX_STEP == 0) {
    IW_WriteSkipIndexEntry(w);
  }

  w->ndocs++;
  return ret;
}

size_t IW_Close(IndexWriter *w) {
  // w->bw.Truncate(w->bw.buf, 0);

  // write the header at the beginning
  writeIndexHeader(w);

  return w->bw.buf->cap;
}

void IW_Free(IndexWriter *w) {
  w->skipIndexWriter.Release(w->skipIndexWriter.buf);
  w->scoreWriter.bw.Release(w->scoreWriter.bw.buf);

  w->bw.Release(w->bw.buf);
  free(w);
}

inline t_docId IR_LastDocId(void *ctx) {
  return ((IndexReader *)ctx)->lastId;
}

inline t_docId UI_LastDocId(void *ctx) {
  return ((UnionContext *)ctx)->minDocId;
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt) {
  // create union context
  UnionContext *ctx = calloc(1, sizeof(UnionContext));
  ctx->its = its;
  ctx->num = num;
  ctx->docTable = dt;
  ctx->currentHits = calloc(num, sizeof(IndexResult));
  for (int i = 0; i < num; i++) {
    ctx->currentHits[i] = NewIndexResult();
  }
  ctx->len = 0;
  // bind the union iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = UI_LastDocId;
  it->Read = UI_Read;
  it->SkipTo = UI_SkipTo;
  it->HasNext = UI_HasNext;
  it->Free = UnionIterator_Free;
  it->Len = UI_Len;
  return it;
}

int UI_Read(void *ctx, IndexResult *hit) {
  UnionContext *ui = ctx;
  // nothing to do
  if (ui->num == 0) {
    return 0;
  }

  int numActive = 0;
  do {
    // find the minimal iterator
    t_docId minDocId = __UINT32_MAX__;
    int minIdx = -1;
    numActive = 0;
    int rc = INDEXREAD_EOF;
    for (int i = 0; i < ui->num; i++) {
      IndexIterator *it = ui->its[i];

      if (it == NULL) continue;

      rc = INDEXREAD_OK;
      // if this hit is behind the min id - read the next entry
      if (ui->currentHits[i].docId <= ui->minDocId || ui->minDocId == 0) {
        rc = INDEXREAD_NOTFOUND;
        // read while we're not at the end and perhaps the flags do not match
        while (rc == INDEXREAD_NOTFOUND) {
          ui->currentHits[i].numRecords = 0;
          rc = it->Read(it->ctx, &ui->currentHits[i]);
        }
      }

      if (rc != INDEXREAD_EOF) {
        numActive++;
      }

      if (rc == INDEXREAD_OK && ui->currentHits[i].docId < minDocId) {
        minDocId = ui->currentHits[i].docId;
        minIdx = i;
      }
      //      }
    }

    // take the minimum entry and yield it
    if (minIdx != -1) {
      if (hit) {
        if (hit) {
          hit->numRecords = 0;
          IndexResult_Add(hit, &ui->currentHits[minIdx]);
        }
      }

      ui->minDocId = ui->currentHits[minIdx].docId;
      ui->len++;
      return INDEXREAD_OK;
    }

  } while (numActive > 0);

  return INDEXREAD_EOF;
}

int UI_Next(void *ctx) {
  // IndexResult h = NewIndexResult();
  return UI_Read(ctx, NULL);
}

// return 1 if at least one sub iterator has next
int UI_HasNext(void *ctx) {

  UnionContext *u = ctx;
  for (int i = 0; i < u->num; i++) {
    IndexIterator *it = u->its[i];

    if (it && it->HasNext(it->ctx)) {
      return 1;
    }
  }
  return 0;
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
int UI_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {
  if (docId == 0) {
    return UI_Read(ctx, hit);
  }
  UnionContext *ui = ctx;

  int n = 0;
  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < ui->num; i++) {
    // this happens for non existent words
    if (ui->its[i] == NULL) continue;

    if (ui->currentHits[i].docId < docId || docId == 0) {

      ui->currentHits[i].numRecords = 0;
      if ((rc = ui->its[i]->SkipTo(ui->its[i]->ctx, docId, &ui->currentHits[i])) == INDEXREAD_EOF) {
        continue;
      }

    } else {
      if (hit) {
        hit->numRecords = 0;
        hit->docId = ui->currentHits[i].docId;
      }
      rc = ui->currentHits[i].docId == docId ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
    }

    // advance the minimal docId for reads
    if (ui->minDocId < ui->currentHits[i].docId || rc == INDEXREAD_EOF) {
      ui->minDocId = ui->currentHits[i].docId;
    }

    // we found a hit - no need to continue
    if (rc == INDEXREAD_OK) {
      if (hit) {
        hit->numRecords = 0;
        IndexResult_Add(hit, &ui->currentHits[i]);
      }
      return rc;
    }
    n++;
  }

  // all iterators are at the end
  if (n == 0) {
    return INDEXREAD_EOF;
  }
  return INDEXREAD_NOTFOUND;
}

void UnionIterator_Free(IndexIterator *it) {
  if (it == NULL) return;

  UnionContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i]) {
      ui->its[i]->Free(ui->its[i]);
    }
    IndexResult_Free(&ui->currentHits[i]);
  }

  free(ui->currentHits);
  free(ui->its);
  free(ui);
  free(it);
}

size_t UI_Len(void *ctx) {
  return ((UnionContext *)ctx)->len;
}

void ReadIterator_Free(IndexIterator *it) {
  if (it == NULL) {
    return;
  }

  IR_Free(it->ctx);
  free(it);
}

void IntersectIterator_Free(IndexIterator *it) {
  if (it == NULL) return;
  IntersectContext *ui = it->ctx;
  for (int i = 0; i < ui->num; i++) {
    if (ui->its[i] != NULL) {
      ui->its[i]->Free(ui->its[i]);
    }
    IndexResult_Free(&ui->currentHits[i]);
  }

  free(ui->currentHits);
  free(ui->its);
  free(it->ctx);
  free(it);
}

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, int exact, DocTable *dt,
                                   u_char fieldMask) {
  // create context
  IntersectContext *ctx = calloc(1, sizeof(IntersectContext));
  ctx->its = its;
  ctx->num = num;
  ctx->lastDocId = 0;
  ctx->len = 0;
  ctx->exact = exact;
  ctx->fieldMask = fieldMask;
  ctx->atEnd = 0;
  ctx->currentHits = calloc(num, sizeof(IndexResult));
  for (int i = 0; i < num; i++) {
    ctx->currentHits[i] = NewIndexResult();
  }
  ctx->docTable = dt;

  // bind the iterator calls
  IndexIterator *it = malloc(sizeof(IndexIterator));
  it->ctx = ctx;
  it->LastDocId = II_LastDocId;
  it->Read = II_Read;
  it->SkipTo = II_SkipTo;
  it->HasNext = II_HasNext;
  it->Len = II_Len;
  it->Free = IntersectIterator_Free;
  return it;
}

int II_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit) {
  IntersectContext *ic = ctx;

  int nfound = 0;

  int rc = INDEXREAD_EOF;
  // skip all iterators to docId
  for (int i = 0; i < ic->num; i++) {
    IndexIterator *it = ic->its[i];
    rc = INDEXREAD_OK;

    // only read if we're not already at the final position
    if (ic->currentHits[i].docId != ic->lastDocId || ic->lastDocId == 0) {
      ic->currentHits[i].numRecords = 0;
      rc = it->SkipTo(it->ctx, docId, &ic->currentHits[i]);
    }

    if (rc == INDEXREAD_EOF) {
      // we are at the end!
      ic->atEnd = 1;
      return rc;
    } else if (rc == INDEXREAD_OK) {
      // YAY! found!
      ic->lastDocId = docId;
      ++nfound;
    } else if (ic->currentHits[i].docId > ic->lastDocId) {
      ic->lastDocId = ic->currentHits[i].docId;
      break;
    }
  }

  if (nfound == ic->num) {
    if (hit) {
      hit->numRecords = 0;
      for (int i = 0; i < ic->num; i++) {
        IndexResult_Add(hit, &ic->currentHits[i]);
      }
    }
    return INDEXREAD_OK;
  }
  // we add the actual last doc id we read, so if anyone is looking at what we returned it would
  // look sane
  if (hit) {
    hit->docId = ic->lastDocId;
  }

  return INDEXREAD_NOTFOUND;
}

int II_Next(void *ctx) {
  return II_Read(ctx, NULL);
}

int II_Read(void *ctx, IndexResult *hit) {
  IntersectContext *ic = (IntersectContext *)ctx;

  if (ic->num == 0) return INDEXREAD_EOF;

  int nh = 0;
  int i = 0;

  do {
    nh = 0;
    for (i = 0; i < ic->num; i++) {
      IndexResult *h = &ic->currentHits[i];
      // skip to the next

      int rc = INDEXREAD_OK;
      if (h->docId != ic->lastDocId || ic->lastDocId == 0) {
        h->numRecords = 0;
        if (ic->its[i] == NULL ||
            (rc = ic->its[i]->SkipTo(ic->its[i]->ctx, ic->lastDocId, h)) == INDEXREAD_EOF) {
          ic->atEnd = 1;

          return INDEXREAD_EOF;
        }
      }

      if (h->docId > ic->lastDocId) {
        ic->lastDocId = h->docId;
        break;
      }
      if (rc == INDEXREAD_OK) {
        ++nh;
      } else {
        ic->lastDocId++;
      }
    }

    if (nh == ic->num) {
      // sum up all hits
      if (hit != NULL) {
        hit->numRecords = 0;
        for (int i = 0; i < nh; i++) {
          IndexResult_Add(hit, &ic->currentHits[i]);
        }
      }

      // advance to the next iterator
      ic->currentHits[0].numRecords = 0;
      if (ic->its[0]->Read(ic->its[0]->ctx, &ic->currentHits[0]) == INDEXREAD_EOF) {
        // if we're at the end we don't want to return EOF right now,
        // but advancing docId makes sure we'll read the first iterator again
        // in the next round
        ic->lastDocId++;
      } else {
        if (ic->currentHits[0].docId > ic->lastDocId) {
          ic->lastDocId = ic->currentHits[0].docId;
        } else {
          ic->lastDocId++;
        }
      }

      if ((hit->flags & ic->fieldMask) == 0) {
        // printf("Skipping %d\n", hit->docId);
        continue;
      }

      // In exact mode, make sure the minimal distance is the number of words
      if (ic->exact && hit != NULL) {
        int md = IndexResult_MinOffsetDelta(hit);

        if (md > ic->num - 1) {
          continue;
        }
      }

      ic->len++;
      return INDEXREAD_OK;
    }

  } while (1);
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