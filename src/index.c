#include "index.h"
#include "varint.h"
#include "forward_index.h"
#include <sys/param.h>
#include <math.h>


#define SKIPINDEX_STEP 15

inline int IR_HasNext(void *ctx) {
    IndexReader *ir = ctx;
    return ir->header.size > ir->buf->offset;
}
inline int IR_GenericRead(IndexReader *ir, t_docId *docId, u_int16_t *freq, u_char *flags, 
                            VarintVector *offsets, t_docId expectedDocId) {
    if (!IR_HasNext(ir)) {
        return INDEXREAD_EOF;
    }
    
    *docId = ReadVarint(ir->buf) + ir->lastId;
    int len = ReadVarint(ir->buf);
    
    if (*docId != expectedDocId) {
        BufferSkip(ir->buf, len);
        return INDEXREAD_OK;
    }
    
    *freq = ReadVarint(ir->buf);
    BufferReadByte(ir->buf, (char*)flags);
    
    size_t offsetsLen = ReadVarint(ir->buf); 
    //Buffer *b = NewBuffer(ir->br.buf->data, offsetsLen, BUFFER_READ);
    if (offsets != NULL && ir->loadOffsets && (expectedDocId == 0 || *docId==expectedDocId)) {
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

#define TOTALDOCS_PLACEHOLDER (double)10000000
inline double tfidf(u_int16_t freq, u_int32_t docFreq) {
    
    double tf = (float)freq/(float)FREQ_QUANTIZE_FACTOR;
    return tf*logb(1.0F + TOTALDOCS_PLACEHOLDER/(double)(docFreq ? docFreq : 1)); //IDF
    
    //LG_INFO("FREQ: %d TF: %.04f, IDF: %.04f, TFIDF: %f\n",freq, tf, idf, tf*idf); 
    //return tf*idf;
}

int IR_Read(void *ctx, IndexHit *e) {
    u_int16_t freq;
    
    IndexReader *ir = ctx;
    // if the entry doesn't have an offset vector, allocate one for it
    // if (e->numOffsetVecs == 0) {
    //     e->offsetVecs = malloc(1*sizeof(VarintVector*));
    //     e->numOffsetVecs++;
    //     e->offsetVecs[0] = calloc(1, sizeof(VarintVector));
    // }
    
    int rc = IR_GenericRead(ir, &e->docId, &freq, &e->flags, NULL, 0);
    
    // add tf-idf score of the entry to the hit
    if (rc == INDEXREAD_OK) {
        e->totalFreq += tfidf(freq, ir->header.numDocs);
    }
    
    return rc;
}

int IR_ReadDocId(void *ctx, IndexHit *e, t_docId expectedDocId) {
    u_int16_t freq;
    
    IndexReader *ir = ctx;
    // if the entry doesn't have an offset vector, allocate one for it
    // if (e->numOffsetVecs == 0) {
    //     e->offsetVecs = malloc(1*sizeof(VarintVector*));
    //     e->numOffsetVecs++;
    //     e->offsetVecs[0] = calloc(1, sizeof(VarintVector));
    // }
    
    int rc = IR_GenericRead(ir, &e->docId, &freq, &e->flags, NULL, expectedDocId);
    
    // add tf-idf score of the entry to the hit
    if (rc == INDEXREAD_OK && e->docId == expectedDocId) {
        e->totalFreq += tfidf(freq, ir->header.numDocs);
    }
    
    return rc;
}

int IR_Next(void *ctx) {
    
    static t_docId docId;
    static u_int16_t freq;
    static u_char flags;
    return IR_GenericRead(ctx,&docId, &freq, &flags, NULL, 0);
        
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    BufferSeek(ir->buf, offset);
    ir->lastId = docId;
}


void IndexHit_Init(IndexHit *h) {
    memset(h, 0, sizeof(IndexHit));
     h->docId = 0;
    // h->flags = 0;
     h->numOffsetVecs = 0;
     h->offsetVecs = NULL;
     h->totalFreq = 0; 
     //h->metadata = (DocumentMetadata){0,0};
     h->hasMetadata = 0;
}

IndexHit NewIndexHit() {
    IndexHit h;
    IndexHit_Init(&h);
    return h;
}


void IndexHit_AppendOffsetVecs(IndexHit *h, VarintVector **offsetVecs, int numOffsetVecs) {
    if (numOffsetVecs == 0) return;
    
    int nv = h->numOffsetVecs + numOffsetVecs;
    h->offsetVecs = realloc(h->offsetVecs, nv*sizeof(VarintVector*));
    int n = 0;
    for (int i = h->numOffsetVecs; i < nv; i++) {
        h->offsetVecs[i] = offsetVecs[n++];
    }
    h->numOffsetVecs = nv;
}

int IndexHit_LoadMetadata(IndexHit *h, DocTable *dt) {
    
    int rc = 0;
    if ((rc = DocTable_GetMetadata(dt, h->docId, &h->metadata)) == REDISMODULE_OK) {
        h->hasMetadata = 1;
    }
    return rc;
}

void IndexHit_Terminate(IndexHit *h) {
    if (h->offsetVecs) {
        for (int i = 0; i < h->numOffsetVecs; i++) {
            free(h->offsetVecs[i]);
        }
        free(h->offsetVecs);
    }
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
        
        while(IR_ReadDocId(ir, hit, docId) != INDEXREAD_EOF) {
            // we found the doc we were looking for!
            if (ir->lastId == docId) {
                return INDEXREAD_OK;
                
            } else if (ir->lastId > docId) { 
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


IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si, DocTable *dt, int loadOffsets) {
    return NewIndexReaderBuf(NewBuffer(data, datalen, BUFFER_READ), si, dt, loadOffsets);
} 

IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si, DocTable *dt, int loadOffsets) {
    
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->buf = buf;
    
    
    indexReadHeader(buf, &ret->header);
    
    ret->lastId = 0;
    ret->skipIdxPos = 0;
    ret->skipIdx = NULL;
    ret->docTable = dt;
    ret->loadOffsets = loadOffsets;
    if (si != NULL) {
        ret->skipIdx = si;
    }
    
    return ret;
} 


void IR_Free(IndexReader *ir) {
    membufferRelease(ir->buf);
    free(ir);
}


IndexIterator *NewIndexIterator(IndexReader *ir) {
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
    LG_DEBUG("Writing index header. offest %d , lastId %d, ndocs %d, will seek to %zd\n", h.size, h.lastId, w->ndocs, offset);
    w->bw.Write(w->bw.buf, &h, sizeof(IndexHeader));
    BufferSeek(w->bw.buf, offset);

}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE));
    w->skipIndexWriter = NewBufferWriter(NewMemoryBuffer(cap, BUFFER_WRITE));
    w->ndocs = 0;
    w->lastId = 0;
    writeIndexHeader(w);
    BufferSeek(w->bw.buf, sizeof(IndexHeader));
    return w;
}

IndexWriter *NewIndexWriterBuf(BufferWriter bw, BufferWriter skipIdnexWriter) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = bw;
    w->skipIndexWriter = skipIdnexWriter;
    w->ndocs = 0;
    w->lastId = 0;
    
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
        LG_DEBUG("read buffer header. size %d, lastId %d at pos %zd\n", h->size, h->lastId, b->offset);
        //BufferSeek(b, pos);
        return 1;     
    } 
    return 0;
    
}


SkipIndex NewSkipIndex(Buffer *b) {
    SkipIndex ret;
    
    u_int32_t len = 0;
    BufferRead(b, &len, sizeof(len));
    
    ret.entries = (SkipEntry*)b->pos;
    ret.len = len;
    return ret;
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

void IW_GenericWrite(IndexWriter *w, t_docId docId, u_int16_t freq, 
                    u_char flags, VarintVector *offsets) {

    
    size_t offsetsSz = VV_Size(offsets);
    // // calculate the overall len
    size_t len = varintSize(freq) + 1 + varintSize(offsetsSz) + offsetsSz;
    size_t lensize = varintSize(len);
    len += lensize;
    //just in case we jumped one order of magnitude
    len += varintSize(len) - lensize;
    
    // Write docId
    WriteVarint(docId - w->lastId, &w->bw);
    // encode len
    WriteVarint(len, &w->bw);
    //encode freq
    WriteVarint(freq, &w->bw);
    //encode flags
    w->bw.Write(w->bw.buf, &flags, 1);
    //write offsets size
    WriteVarint(offsetsSz, &w->bw);
    w->bw.Write(w->bw.buf, offsets->data, offsets->cap);
    
    
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
   
    IW_GenericWrite(w, ent->docId, ent->freq, ent->flags, offsets);
}



size_t IW_Close(IndexWriter *w) {
   

    //w->bw.Truncate(w->bw.buf, 0);
    
    // write the header at the beginning
     writeIndexHeader(w);
     
    
    return w->bw.buf->cap;
}

// void IW_MakeSkipIndex(IndexWriter *iw, Buffer *buf) {
//     IndexReader *ir = NewIndexReader(iw->bw.buf->data, iw->bw.buf->cap, NULL, NULL);
    
//     int nents = ir->header.numDocs/SKIPINDEX_STEP;
    
//     BufferWriter bw = NewBufferWriter(buf);
//     int i = 0, idx = 0;
//     while (IR_Next(ir) != INDEXREAD_EOF && idx < nents) {
        
//        if (++i % SKIPINDEX_STEP == 0) {
          
//            SkipEntry se = {ir->lastId, ir->buf->offset};
//            bw.Write(buf, &se, sizeof(SkipEntry));
//            //LG_DEBUG("skipindex[%d]: docId %d, offset %d\n", idx, ir->lastId, entries[idx].offset);
//            idx++;
//        }
          
//     }
    
//     bw.Truncate(buf, 0);
//     bw.Release(buf);
//     iw->skipIdx.entries = (SkipEntry*)buf->data;
//     iw->skipIdx.len = idx;
     
// }


void IW_Free(IndexWriter *w) {
    w->skipIndexWriter.Release(w->skipIndexWriter.buf);
    w->bw.Release(w->bw.buf);
    free(w);
}

inline int iw_isPos(SkipIndex *idx, u_int i, t_docId docId) {
    if (idx->entries[i].docId < docId &&
        (i < idx->len - 1 && idx->entries[i+1].docId >= docId)) {
            return 1;
   }
   return 0;
}

SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int *offset) {
    
    if (idx == NULL || idx->len == 0 || docId < idx->entries[0].docId) {
        return NULL;
    } 
    if (docId > idx->entries[idx->len-1].docId) {
        *offset = idx->len - 1; 
        return &idx->entries[idx->len-1];
    }
    u_int top = idx->len, bottom = *offset;
    u_int i = bottom;
    int newi;
    while (bottom < top) {
        //printf("top %d, bottom: %d idx %d, i %d, docId %d\n", top, bottom, idx->entries[i].docId, i, docId );
       if (iw_isPos(idx, i, docId)) {
           //LG_DEBUG("IS POS!\n");
           *offset = i;
           return &idx->entries[i];
       }
       //LG_DEBUG("NOT POS!\n");
       
       if (docId <= idx->entries[i].docId) {
           top = i ;
       } else {
           bottom = i ;
       }
       newi = (bottom + top)/2;
       //LG_DEBUG("top %d, bottom: %d, new i: %d\n", top, bottom, newi);
       if (newi == i) {
           break;
       }
       i = newi;
    }
    // if (i == 0) {
    //     return &idx->entries[0];
    // }
    return NULL;
}


inline t_docId IR_LastDocId(void* ctx) {
    return ((IndexReader *)ctx)->lastId;
}

int IR_Intersect2(IndexIterator **argv, int argc, IntersectHandler onIntersect, void *ctx) {
    
    // nothing to do
    if (argc <= 0) {
        return 0;
    }
    
    IndexHit hits[argc];
    for (int i =0; i < argc; i++) {
        IndexHit_Init(&hits[i]);
    }
    
    t_docId currentDoc = 0;
    int count = 0;
    int nh = 0;
    do {
        nh = 0;    
        
        for (int i = 0; i < argc; i++) {
            IndexHit *h = &hits[i];
            // skip to the next
            if (h->docId != currentDoc || currentDoc == 0) {
                if (argv[i]->SkipTo(argv[i]->ctx, currentDoc, h) == INDEXREAD_EOF) {
                    return count;
                }
            }
            if (h->docId != currentDoc) {
                currentDoc = h->docId;
                break;
            }
            currentDoc = h->docId;
            nh++;
        }
        
        if (nh == argc) {
            onIntersect(ctx, hits, argc);
            ++count;
            
            if (argv[0]->Read(argv[0]->ctx, &hits[0]) == INDEXREAD_EOF) {
               return count;
            }
            currentDoc = hits[0].docId;
        }
    }while(1);

    return count;
    
}




int cmpHits(const void *h1, const void *h2) {
    return ((IndexHit*)h1)->docId - ((IndexHit*)h2)->docId; 
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
    if (ui->num <= 0) {
        return 0;
    }
    
    t_docId minDocId = __UINT32_MAX__;
    int minIdx = 0;
    
    do {
        minIdx = -1;
        for (int i = 0; i < ui->num; i++) {
            IndexIterator *it = ui->its[i];

            if (it == NULL) continue;
            
            if (it->HasNext(it->ctx)) {
                // if this hit is behind the min id - read the next entry
                if (ui->currentHits[i].docId <= ui->minDocId || ui->minDocId == 0) {
                    if (it->Read(it->ctx, &ui->currentHits[i]) == INDEXREAD_EOF) {
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
         if (it == NULL) continue;
         
         if (it->HasNext(it->ctx)) {
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
         rc = ui->its[i]->SkipTo(ui->its[i]->ctx, docId, &ui->currentHits[i]);
         if (rc == INDEXREAD_EOF) {
             continue;
         } else if (rc == INDEXREAD_OK) {
             // YAY! found!
             ui->minDocId = docId;
             *hit = ui->currentHits[i];
         }
        n++;
     }
     if (rc == INDEXREAD_OK) {
         return rc;
     }
     // all iterators are at the end
     if (n == 0) {
         return INDEXREAD_EOF;
     }
     
     
     ui->minDocId = __UINT32_MAX__;
     int minIdx = -1;
     // copy the lowest one to *hit
     for (int i =0; i < ui->num; i++) {
         if (!ui->its[i]->HasNext(ui->its[i]->ctx)) {
            continue;
         }
            
         if (ui->currentHits[i].docId < ui->minDocId) {
             ui->minDocId = ui->currentHits[i].docId; 
             minIdx = i;
         }
     }
     if (minIdx == -1) {
         return INDEXREAD_EOF;
     }
     
     ui->minDocId = ui->currentHits[minIdx].docId;
     *hit = ui->currentHits[minIdx];
     return INDEXREAD_NOTFOUND;
 }
 
 void UnionIterator_Free(IndexIterator *it) {
     if (it == NULL) return;
     
     
     UnionContext *ui = it->ctx;
     for (int i = 0; i < ui->num; i++) {
         ui->its[i]->Free(ui->its[i]);
     }
     free(it->ctx);
     free(it);
 }
 
 void IntersectIterator_Free(IndexIterator *it) {
     if (it == NULL) return;
     IntersectContext *ui = it->ctx;
     for (int i = 0; i < ui->num; i++) {
         ui->its[i]->Free(ui->its[i]);
     }
     free(it->ctx);
     free(it);
 }

void ReadIterator_Free(IndexIterator *it) {
    if (it==NULL) return;
    IR_Free(it->ctx);
    free(it);
}
 
 
 
 IndexIterator *NewIntersecIterator(IndexIterator **its, int num, int exact, DocTable *dt) {
     // create context
    IntersectContext *ctx = calloc(1, sizeof(IntersectContext));
    ctx->its =its;
    ctx->num = num;
    ctx->lastDocId = 0;
    ctx->exact = exact;
    ctx->currentHits = calloc(num, sizeof(IndexHit));
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
    
    int nh = 0;
    int i = 0;
    do {
        nh = 0;    
        for (i = 0; i < ic->num; i++) {
            
            IndexHit *h = &ic->currentHits[i];
            // skip to the next
            if (h->docId != ic->lastDocId || ic->lastDocId == 0) {
                IndexIterator *it = ic->its[i];
                
                if (it == NULL || !it->HasNext(it->ctx)) {
                    return INDEXREAD_EOF;
                }
                if (it->SkipTo(it->ctx, ic->lastDocId, h) == INDEXREAD_EOF) {
                    return INDEXREAD_EOF;
                }
            }
            if (h->docId != ic->lastDocId) {
                ic->lastDocId = h->docId;
                break;
            }
            ic->lastDocId = h->docId;
            ++nh;
        }
        
        if (nh == ic->num) {
            
            // sum up all hits
            if (hit != NULL) {
                for (int i = 0; i < nh; i++) {
                    IndexHit *hh = &ic->currentHits[i];
                    hit->docId = hh->docId;
                    hit->flags |= hh->flags;
                    hit->totalFreq += hh->totalFreq;
                    IndexHit_AppendOffsetVecs(hit, hh->offsetVecs, hh->numOffsetVecs);
                } 
            }
            
            // advance to the next iterator
            if (ic->its[0]->Read(ic->its[0]->ctx, &ic->currentHits[0]) == INDEXREAD_EOF) {
                // if we're at the end we don't want to return EOF right now,
                // but advancing docI makes sure we'll read the first iterator again in the next round
                ic->lastDocId++;
            } else {
                ic->lastDocId = ic->currentHits[0].docId;    
            }
            
            
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
            return 0;
        }
    }
    return 1;
}

t_docId II_LastDocId(void *ctx) {
    return ((IntersectContext *)ctx)->lastDocId;
}
