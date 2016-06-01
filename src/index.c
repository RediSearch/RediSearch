#include "index.h"
#include "varint.h"
#include "forward_index.h"
#include <sys/param.h>
#include <math.h>

inline int IR_HasNext(void *ctx) {
    IndexReader *ir = ctx;
    //LG_DEBUG("ir %p size %d, offset %d. has next? %d\n", ir, ir->header.size, ir->buf->offset, ir->header.size > ir->buf->offset);
    return ir->header.size > ir->buf->offset; 

}

inline int IR_GenericRead(IndexReader *ir, t_docId *docId, float *freq, u_char *flags, 
                            VarintVector *offsets, t_docId expectedDocId) {
    if (!IR_HasNext(ir)) {
        return INDEXREAD_EOF;
    }
    
    *docId = ReadVarint(ir->buf) + ir->lastId;
    int len = ReadVarint(ir->buf);
   
    // if (expectedDocId != 0 && *docId != expectedDocId) {
        
    //     BufferSkip(ir->buf, len);
    //     ir->lastId = *docId;
    //     return INDEXREAD_OK;
    // }
    
    int quantizedScore = ReadVarint(ir->buf);
    if (freq != NULL) {
        *freq = (float)quantizedScore/FREQ_QUANTIZE_FACTOR;
        //LG_DEBUG("READ Quantized score %d, freq %f", quantizedScore, *freq);
    }
    
    BufferReadByte(ir->buf, (char*)flags);
    
    size_t offsetsLen = ReadVarint(ir->buf); 
        
    // If needed - read offset vectors
    if (offsets != NULL && !ir->singleWordMode && 
        (expectedDocId == 0 || *docId==expectedDocId)) {
            
        offsets->cap = offsetsLen;
        offsets->data = ir->buf->pos;
        offsets->pos = offsets->data;
        offsets->ctx = NULL;
        offsets->offset = 0;
        offsets->type = BUFFER_READ;
    } 
    
    BufferSkip(ir->buf, offsetsLen);
    //printf("IR %p READ %d, was at %d", ir, *docId, ir->lastId);

    ir->lastId = *docId;
    return INDEXREAD_OK;
}

#define TOTALDOCS_PLACEHOLDER (double)10000000
inline double tfidf(float freq, u_int32_t docFreq) {
    double idf =logb(1.0F + TOTALDOCS_PLACEHOLDER/(docFreq ? docFreq : (double)1)); //IDF  
    //LG_DEBUG("FREQ: %f  IDF: %.04f, TFIDF: %f",freq, idf, freq*idf);
    return freq*idf;
}

int IR_Read(void *ctx, IndexHit *e) {
    
    float freq;
    IndexReader *ir = ctx;
    
    if (ir->useScoreIndex && ir->scoreIndex) {
        
        ScoreIndexEntry *ent = ScoreIndex_Next(ir->scoreIndex);
        if (ent == NULL) {
            return INDEXREAD_EOF;
        }
        
        IR_Seek(ir, ent->offset, ent->docId);
        
    }
    
    VarintVector *offsets = NULL;
    if (!ir->singleWordMode) {
        offsets = &e->offsetVecs[0];
        e->numOffsetVecs = 1; 
    }
    
    int rc = IR_GenericRead(ir, &e->docId, &freq, &e->flags, 
                            offsets, 0);
        
    // add tf-idf score of the entry to the hit
    if (rc == INDEXREAD_OK) {
        //LG_DEBUG("docId %d Flags 0x%x, field mask 0x%x, intersection: %x", e->docId, e->flags, ir->fieldMask, e->flags & ir->fieldMask);
        if (!(e->flags & ir->fieldMask)) {
            //LG_DEBUG("Skipping %d", e->docId);
            return INDEXREAD_NOTFOUND;
        }

        e->totalFreq += tfidf(freq, ir->header.numDocs);
    }
    e->type = H_RAW;
    
    //LG_DEBUG("Read docId %d, rc %d",e->docId, rc);
    return rc;
}


int IR_Next(void *ctx) {
    
    static t_docId docId;
    //static float freq;
    static u_char flags;
    return IR_GenericRead(ctx,&docId, NULL, &flags, NULL, 0);
        
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    //LG_DEBUG("Seeking to %d, lastId %d", offset, docId);
    BufferSeek(ir->buf, offset);
    ir->lastId = docId;
}


void IndexHit_Init(IndexHit *h) {
    memset(h, 0, sizeof(IndexHit));
     h->docId = 0;
     h->numOffsetVecs = 0;
    // h->flags = 0;
     h->totalFreq = 0; 
     h->type = H_RAW;
     //h->metadata = (DocumentMetadata){0,0};
     h->hasMetadata = 0;
}

IndexHit NewIndexHit() {
    IndexHit h;
    IndexHit_Init(&h);
    return h;
}


int IndexHit_LoadMetadata(IndexHit *h, DocTable *dt) {
    
    int rc = 0;
    if ((rc = DocTable_GetMetadata(dt, h->docId, &h->metadata)) == REDISMODULE_OK) {
        h->hasMetadata = 1;
    }
    return rc;
}

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF if at EOF
*/
int IR_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit) {  
    IndexReader *ir = ctx;
    
    SkipEntry *ent = SkipIndex_Find(ir->skipIdx, docId, &ir->skipIdxPos);
    
    if (ent != NULL || ir->skipIdx == NULL || ir->skipIdx->len == 0 
    || docId <= ir->skipIdx->entries[0].docId) {
        
        if (ent != NULL && ent->offset > BufferOffset(ir->buf)) {
            IR_Seek(ir, ent->offset, ent->docId);
        }
        
        int rc;
        while(INDEXREAD_EOF != (rc = IR_Read(ir, hit))) {
            // we found the doc we were looking for!
            if (ir->lastId == docId) {
                return rc;
                
            } else if (ir->lastId > docId) {
                //LG_DEBUG("Wanted %d got %d, not found!", docId, ir->lastId); 
                // this is not the droid you are looking for 
                return INDEXREAD_NOTFOUND;
            }
        }
        
    }
    
    return INDEXREAD_EOF;
}

u_int32_t IR_NumDocs(IndexReader *ir) {
    return ir->header.numDocs;
}


IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si, DocTable *dt, 
                            int singleWordMode, u_char fieldMask) {
    return NewIndexReaderBuf(NewBuffer(data, datalen, BUFFER_READ), si, dt, singleWordMode, NULL, fieldMask);
} 

IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si, DocTable *dt, int singleWordMode, 
                                ScoreIndex *sci, u_char fieldMask) {
    
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->buf = buf;
    
    
    indexReadHeader(buf, &ret->header);
    
    ret->lastId = 0;
    ret->skipIdxPos = 0;
    ret->skipIdx = NULL;
    ret->docTable = dt;
    ret->singleWordMode = singleWordMode;
    // only use score index on single words, no field filter and large entries
    ret->useScoreIndex = sci != NULL && singleWordMode && fieldMask == 0xff && ret->header.numDocs > SCOREINDEX_DELETE_THRESHOLD;
    ret->scoreIndex = sci;
    //LG_DEBUG("Load offsets %d, si: %p", singleWordMode, si);
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
    return ri;
}





size_t IW_Len(IndexWriter *w) {
    return BufferLen(w->bw.buf);
}

void writeIndexHeader(IndexWriter *w) {
    size_t offset = w->bw.buf->offset;
    BufferSeek(w->bw.buf, 0);
    IndexHeader h = {offset, w->lastId, w->ndocs};
    LG_DEBUG("Writing index header. offest %d , lastId %d, ndocs %d, will seek to %zd", h.size, h.lastId, w->ndocs, offset);
    w->bw.Write(w->bw.buf, &h, sizeof(IndexHeader));
    BufferSeek(w->bw.buf, offset);

}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE));
    w->skipIndexWriter = NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE));
    w->scoreWriter = NewScoreIndexWriter(NewBufferWriter(NewMemoryBuffer(2, BUFFER_WRITE)));
    w->scoreWriter.header.numEntries = 0;
    w->scoreWriter.header.lowestIndex = 0;
    w->scoreWriter.header.lowestScore = 0;
    w->ndocs = 0;
    w->lastId = 0;
    writeIndexHeader(w);
    BufferSeek(w->bw.buf, sizeof(IndexHeader));
    return w;
}

IndexWriter *NewIndexWriterBuf(BufferWriter bw, BufferWriter skipIdnexWriter, ScoreIndexWriter siw) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = bw;
    w->skipIndexWriter = skipIdnexWriter;
    w->ndocs = 0;
    w->lastId = 0;
    w->scoreWriter = siw;
    
    IndexHeader h = {0, 0, 0};
    if (indexReadHeader(w->bw.buf, &h) && h.size > 0) {
        w->lastId = h.lastId;
        w->ndocs = h.numDocs;
        BufferSeek(w->bw.buf, h.size);
        
    } else {
        writeIndexHeader(w);
        BufferSeek(w->bw.buf, sizeof(IndexHeader));    
    }
    
    return w;
}

int indexReadHeader(Buffer *b, IndexHeader *h) {
    
    if (b->cap > sizeof(IndexHeader)) {
        
        BufferSeek(b, 0);
        BufferRead(b, h, sizeof(IndexHeader));
        LG_DEBUG("read buffer header. size %d, lastId %d at pos %zd", h->size, h->lastId, b->offset);
        //BufferSeek(b, pos);
        return 1;     
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

void IW_GenericWrite(IndexWriter *w, t_docId docId, float freq, 
                    u_char flags, VarintVector *offsets) {

    ScoreIndexWriter_AddEntry(&w->scoreWriter, freq, BufferOffset(w->bw.buf), w->lastId);
    // quantize the score to compress it to max 4 bytes
    // freq is between 0 and 1
    int quantizedScore = floorl(freq * (double)FREQ_QUANTIZE_FACTOR);
    LG_DEBUG("docId %d, flags %x, Score %f, quantized score %d", docId, flags, freq, quantizedScore);
    
    size_t offsetsSz = VV_Size(offsets);
    // // calculate the overall len
    size_t len = varintSize(quantizedScore) + 1 + varintSize(offsetsSz) + offsetsSz;
    
    
    // Write docId
    WriteVarint(docId - w->lastId, &w->bw);
    // encode len

    WriteVarint(len, &w->bw);
    //encode freq
    WriteVarint(quantizedScore, &w->bw);
    //encode flags
    w->bw.Write(w->bw.buf, &flags, 1);
    //write offsets size
    WriteVarint(offsetsSz, &w->bw);
    w->bw.Write(w->bw.buf, offsets->data, offsetsSz);
    
    
    
    w->lastId = docId;
    if (w->ndocs % SKIPINDEX_STEP == 0) {
        IW_WriteSkipIndexEntry(w);        
    }
    
     
    w->ndocs++;
    
    
    
}

/* Write a forward-index entry to an index writer */ 
void IW_WriteEntry(IndexWriter *w, ForwardIndexEntry *ent) {
    VVW_Truncate(ent->vw);
    VarintVector *offsets = ent->vw->bw.buf;
   
    IW_GenericWrite(w, ent->docId, ent->freq*ent->docScore, ent->flags, offsets);
}



size_t IW_Close(IndexWriter *w) {
   

    //w->bw.Truncate(w->bw.buf, 0);
    
    // write the header at the beginning
     writeIndexHeader(w);
     
    
    return w->bw.buf->cap;
}

void IW_Free(IndexWriter *w) {
    w->skipIndexWriter.Release(w->skipIndexWriter.buf);
    w->bw.Release(w->bw.buf);
    free(w);
}


inline t_docId IR_LastDocId(void* ctx) {
    return ((IndexReader *)ctx)->lastId;
}


inline t_docId UI_LastDocId(void *ctx) {
    return ((UnionContext*)ctx)->minDocId;
}

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *dt) {
    
    // create union context
    UnionContext *ctx = calloc(1, sizeof(UnionContext));
    ctx->its =its;
    ctx->num = num;
    ctx->docTable = dt;
    ctx->currentHits = calloc(num, sizeof(IndexHit));
    
    // bind the union iterator calls
    IndexIterator *it = malloc(sizeof(IndexIterator));
    it->ctx = ctx;
    it->LastDocId = UI_LastDocId;
    it->Read = UI_Read;
    it->SkipTo = UI_SkipTo;
    it->HasNext = UI_HasNext;
    it->Free = UnionIterator_Free;
    return it;
    
}


int UI_Read(void *ctx, IndexHit *hit) {
    
    UnionContext *ui = ctx;
    // nothing to do
    if (ui->num == 0) {
        return 0;
    }
    
    
    int minIdx = 0;
    do {
        // find the minimal iterator 
        t_docId minDocId = __UINT32_MAX__;
        minIdx = -1;
        for (int i = 0; i < ui->num; i++) {
            IndexIterator *it = ui->its[i];

            if (it == NULL) continue;
            
            if (it->HasNext(it->ctx)) {
                // if this hit is behind the min id - read the next entry
                if (ui->currentHits[i].docId <= ui->minDocId || ui->minDocId == 0) {
                    if (it->Read(it->ctx, &ui->currentHits[i]) != INDEXREAD_OK) {
                        continue;
                    }
                }
                if (ui->currentHits[i].docId < minDocId) {
                    minDocId = ui->currentHits[i].docId;
                    minIdx = i;
                }
            }
        }
        
        // not found a new minimal docId
        if (minIdx == -1) {
            return INDEXREAD_EOF;
        }
        
        
        
        *hit = ui->currentHits[minIdx];
        hit->type = H_UNION;
        ui->minDocId = ui->currentHits[minIdx].docId;
        return INDEXREAD_OK;
    
    }while(minIdx >= 0);

    return INDEXREAD_EOF;
    
}

 int UI_Next(void *ctx) {
     IndexHit h = NewIndexHit();
     return UI_Read(ctx, &h);
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
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF if at EOF
*/
 int UI_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit) {
     UnionContext *ui = ctx;
     
     int n = 0;
     int rc = INDEXREAD_EOF;
     // skip all iterators to docId
     for (int i = 0; i < ui->num; i++) {
         // this happens for non existent words
         if (ui->its[i] == NULL) continue;
         
         if ((rc = ui->its[i]->SkipTo(ui->its[i]->ctx, docId, &ui->currentHits[i])) == INDEXREAD_EOF) {
             continue;
         }
         
        // advance the minimal docId for reads 
        if (ui->minDocId < ui->currentHits[i].docId || rc == INDEXREAD_EOF) {
            ui->minDocId = ui->currentHits[i].docId;
        }
             
         *hit = ui->currentHits[i];
         hit->type = H_UNION;
         // we found a hit - no need to continue
         if (rc == INDEXREAD_OK) {
             break;
         }
         n++;
     }
     //printf("UI %p skip to %d, rc %d, n: %d\n", ui, docId, rc,n);
     if (rc == INDEXREAD_OK) {
         return rc;
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
      }
     
     free(ui->currentHits);
     free(ui->its);
     free(ui);
     free(it);
 }
  

void ReadIterator_Free(IndexIterator *it) {
    if (it==NULL) return;
       
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
    ctx->its =its;
    ctx->num = num;
    ctx->lastDocId = 0;
    ctx->exact = exact;
    ctx->fieldMask = fieldMask;
    ctx->currentHits = calloc(num, sizeof(IndexHit));
    for (int i = 0; i < num; i++) {
        IndexHit_Init(&ctx->currentHits[i]);
    }
    ctx->docTable = dt;

    
    // bind the iterator calls
    IndexIterator *it = malloc(sizeof(IndexIterator));
    it->ctx = ctx;
    it->LastDocId = II_LastDocId;
    it->Read = II_Read;
    it->SkipTo = II_SkipTo;
    it->HasNext = II_HasNext;
    it->Free = IntersectIterator_Free;
    return it;
}
 
 
int II_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit) {
    
    IntersectContext *ic = ctx;
     
     int nfound = 0;
     
     int rc = INDEXREAD_EOF;
     // skip all iterators to docId
     for (int i = 0; i < ic->num; i++) {
         IndexIterator *it = ic->its[i];
         
         rc = it->SkipTo(it->ctx, docId, &ic->currentHits[i]);
         if (rc == INDEXREAD_EOF) {
             return rc;
         } else if (rc == INDEXREAD_OK) {
             // YAY! found!
             ic->lastDocId = docId;
             
             ++nfound;
         } else if (ic->currentHits[i].docId > ic->lastDocId){
             ic->lastDocId = ic->currentHits[i].docId;
         }
         
     }
     if (nfound == ic->num) {
         
         
         
         *hit = ic->currentHits[0];
         return INDEXREAD_OK;
     }
     
     
     return INDEXREAD_NOTFOUND;
}


int II_Next(void *ctx) {
    //IndexHit h;
    return II_Read(ctx, NULL);
}

int II_Read(void *ctx, IndexHit *hit) {
    IntersectContext *ic = (IntersectContext*)ctx;
    
    if (ic->num == 0) return INDEXREAD_EOF;
    
    int nh = 0;
    int i = 0;
    do {
        //LG_DEBUG("II %p last docId %d", ic,     ic->lastDocId);
        nh = 0;    
        for (i = 0; i < ic->num; i++) {
            
            IndexHit *h = &ic->currentHits[i];
            //LG_DEBUG("h->docId: %d, ic->lastDocId: %d", h->docId, ic->lastDocId);
            // skip to the next
            int rc = INDEXREAD_OK;
            if (h->docId != ic->lastDocId || ic->lastDocId == 0) {
                
                if (ic->its[i] == NULL || 
                    (rc = ic->its[i]->SkipTo(ic->its[i]->ctx, ic->lastDocId, h)) == INDEXREAD_EOF) {
                    return INDEXREAD_EOF;
                }
            } 
            
            // LG_DEBUG("i %d rc: %d flags %d, fieldmask %d hasNext? %d\n", i, rc, h->flags, 
            //           ic->fieldMask, ic->its[i]->HasNext(ic->its[i]->ctx));
            
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
        
        //LG_DEBUG("nh: %d/%d", nh, ic->num);
        if (nh == ic->num) {
            
            // sum up all hits
            if (hit != NULL) {
                hit->numOffsetVecs = 0;
                hit->flags = 0xff;
                hit->type = H_INTERSECTED;
                hit->docId = ic->currentHits[0].docId;
                for (int i = 0; i < nh; i++) {
                    IndexHit *hh = &ic->currentHits[i];
                    
                    hit->flags &= hh->flags;
                    hit->totalFreq += hh->totalFreq;
                    
                    int n = 0;
                    // Copy the offset vectors of the hits
                    while(hit->numOffsetVecs < MAX_INTERSECT_WORDS && n < hh->numOffsetVecs) {
                        hit->offsetVecs[hit->numOffsetVecs++] = hh->offsetVecs[n++];
                    }
                } 
                
               
            }
            
            // advance to the next iterator
            if (ic->its[0]->Read(ic->its[0]->ctx, &ic->currentHits[0]) == INDEXREAD_EOF) {
                // if we're at the end we don't want to return EOF right now,
                // but advancing docId makes sure we'll read the first iterator again in the next round
                ic->lastDocId++;
            } else {
                if (ic->currentHits[0].docId > ic->lastDocId) {
                    ic->lastDocId = ic->currentHits[0].docId;    
                } else {
                    ic->lastDocId++;
                }
            }
            
            //LG_DEBUG("Flags %x, field mask %x, intersection: %x", hit->flags, ic->fieldMask, hit->flags & ic->fieldMask);
            if ((hit->flags & ic->fieldMask) == 0) {
                LG_DEBUG("Skipping %d", hit->docId);
                continue;
            }
            
            // In exact mode, make sure the minimal distance is the number of words
             if (ic->exact && hit != NULL) {
                 int md = VV_MinDistance(hit->offsetVecs, hit->numOffsetVecs);
                 
                 if (md > ic->num - 1) {
                    continue;
                 }
                 hit->type = H_EXACT;
             } 
             
             //LG_DEBUG("INTERSECT @%d", hit->docId );
             
             return INDEXREAD_OK;
        }
        
    }while(1);

    return INDEXREAD_EOF;
}

int II_HasNext(void *ctx) {
    IntersectContext *ic = ctx;
    for (int i = 0; i < ic->num; i++) {
        IndexIterator *it = ic->its[i];
        if (it == NULL || !it->HasNext(it->ctx)) {
            //printf("II %p it %d (%p) has no next", ic, i, it);
            return 0;
        }
    }
    //printf ("II %p has next", ic);
    return 1;
}

t_docId II_LastDocId(void *ctx) {
    return ((IntersectContext *)ctx)->lastDocId;
}
