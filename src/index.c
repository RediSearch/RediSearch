#include "index.h"
#include "varint.h"
#include "forward_index.h"
#include <sys/param.h>

#define SKIPINDEX_STEP 100


int IR_Read(void *ctx, IndexHit *e) {
    IndexReader *ir = ctx;
    if (!IR_HasNext(ir)) {
        return INDEXREAD_EOF;
    }
    
    e->docId = ReadVarint(ir->buf) + ir->lastId;
    e->len = ReadVarint(ir->buf);
    e->freq = ReadVarint(ir->buf);
    BufferReadByte(ir->buf, (char *)&e->flags);
    
    size_t offsetsLen = ReadVarint(ir->buf); 
    //Buffer *b = NewBuffer(ir->br.buf->data, offsetsLen, BUFFER_READ);
    
    e->offsets.data = ir->buf->data;
    e->offsets.cap = offsetsLen;
    e->offsets.offset = 0;
    e->offsets.pos = e->offsets.data;
    e->offsets.type = BUFFER_READ;
    
    BufferSkip(ir->buf, offsetsLen);
    ir->lastId = e->docId;
    return INDEXREAD_OK;
}


inline int IR_HasNext(void *ctx) {
    IndexReader *ir = ctx;
    if (!BufferAtEnd(ir->buf) && ir->header.size > ir->buf->offset) {
        return 1;
    }
    return 0;
}

int IR_Next(void *ctx) {
    IndexReader *ir = ctx;
    if (!IR_HasNext(ir)) {
        return INDEXREAD_EOF;
    }
    
    
    char *pos = ir->buf->pos;
    ir->lastId = ReadVarint(ir->buf) + ir->lastId;
    u_int16_t len = ReadVarint(ir->buf);
    
    size_t skip = len - (ir->buf->pos - pos);
    BufferSkip(ir->buf, skip);
    return INDEXREAD_OK;
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    BufferSeek(ir->buf, offset);
    ir->lastId = docId;
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
        
        while(IR_Read(ir, hit) != INDEXREAD_EOF) {
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


IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si) {
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->buf = NewBuffer(data, datalen, BUFFER_READ);
    BufferRead(ret->buf, &ret->header, sizeof(IndexHeader));
    
    ret->lastId = 0;
    ret->skipIdxPos = 0;
    if (si != NULL) {
        ret->skipIdx = si;
    }
    return ret;
} 

IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si) {
    
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->buf = buf;
    
    indexReadHeader(buf, &ret->header);

    ret->lastId = 0;
    ret->skipIdxPos = 0;
    ret->skipIdx = NULL;
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
    return ri;
}





size_t IW_Len(IndexWriter *w) {
    return BufferLen(w->bw.buf);
}

void writeIndexHeader(IndexWriter *w) {
    size_t offset = w->bw.buf->offset;
    BufferSeek(w->bw.buf, 0);
    IndexHeader h = {offset, w->lastId};
    LG_DEBUG("Writing index header. offest %d , lastId %d\n", h.size, h.lastId);
    w->bw.Write(w->bw.buf, &h, sizeof(h));
    BufferSeek(w->bw.buf, offset);

}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = NewBufferWriter(cap);
    w->ndocs = 0;
    w->lastId = 0;
    writeIndexHeader(w);
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
IndexWriter *NewIndexWriterBuf(BufferWriter bw) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = bw;
    w->ndocs = 0;
    w->lastId = 0;
    
    IndexHeader h = {0, 0};
    if (indexReadHeader(w->bw.buf, &h) && h.size > 0) {
        w->lastId = h.lastId;
        BufferSeek(w->bw.buf, h.size);
    } else {
        writeIndexHeader(w);
        BufferSeek(w->bw.buf, sizeof(h));    
    }
    
    return w;
}


void IW_Write(IndexWriter *w, IndexHit *e) {
    
    size_t offsetsSz = VV_Size(&e->offsets);
    // calculate the overall len
    size_t len = varintSize(e->docId - w->lastId) + varintSize(e->freq) + 1 + varintSize(offsetsSz) + offsetsSz;
    size_t lensize = varintSize(len);
    len += lensize;
    //just in case we jumped one order of magnitude
    len += varintSize(len) - lensize;
    WriteVarint(e->docId - w->lastId, &w->bw);
    w->lastId = e->docId;
    // encode len
    WriteVarint(len, &w->bw);
    //encode freq
    WriteVarint(e->freq, &w->bw);
    //encode flags
    w->bw.Write(w->bw.buf, &e->flags, sizeof(e->flags));
    
    //write offsets size
    WriteVarint(offsetsSz, &w->bw);
    w->bw.Write(w->bw.buf, e->offsets.data, e->offsets.cap);
    
    w->ndocs++;
}

void IW_WriteEntry(IndexWriter *w, ForwardIndexEntry *ent) {
    
    LG_DEBUG("Writing entry %s\n", ent->term);
    VVW_Truncate(ent->vw);
    VarintVector *offsets = ent->vw->v;
    
    
    size_t offsetsSz = VV_Size(offsets);
    // calculate the overall len
    size_t len = varintSize(ent->docId - w->lastId) + varintSize(ent->freq) + 1 + varintSize(offsetsSz) + offsetsSz;
    size_t lensize = varintSize(len);
    len += lensize;
    //just in case we jumped one order of magnitude
    len += varintSize(len) - lensize;
    WriteVarint(ent->docId - w->lastId, &w->bw);
    w->lastId = ent->docId;
    // encode len
    WriteVarint(len, &w->bw);
    //encode freq
    WriteVarint(ent->freq, &w->bw);
    //encode flags
    w->bw.Write(w->bw.buf, &ent->flags, sizeof(ent->flags));
    
    //write offsets size
    WriteVarint(offsetsSz, &w->bw);
    w->bw.Write(w->bw.buf, offsets->data, offsets->cap);
    
    w->ndocs++;
}



size_t IW_Close(IndexWriter *w) {
   

    //w->bw.Truncate(w->bw.buf, 0);
    
    // write the header at the beginning
     writeIndexHeader(w);
    
    
    return w->bw.buf->cap;
}

void IW_MakeSkipIndex(IndexWriter *iw, int step) {
    IndexReader *ir = NewIndexReader(iw->bw.buf->data, iw->bw.buf->cap, NULL);

    SkipEntry *entries = calloc(ir->header.size/step, sizeof(SkipEntry));
    
    int i = 0, idx = 0;
    while (IR_Next(ir) != INDEXREAD_EOF) {
       if (++i % step == 0) {
           
           entries[idx].docId = ir->lastId;
           entries[idx].offset = BufferOffset(ir->buf);
           //LG_DEBUG("skipindex[%d]: docId %d, offset %d\n", idx, ir->lastId, entries[idx].offset);
           idx++;
       }
          
    }
    
    iw->skipIdx.entries = entries;
    iw->skipIdx.len = idx;
     
}


void IW_Free(IndexWriter *w) {
    free(w->skipIdx.entries);
    w->bw.Release(w->bw.buf);
    free(w);
}

int iw_isPos(SkipIndex *idx, u_int i, t_docId docId) {
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
        //LG_DEBUG("top %d, bottom: %d idx %d, i %d, docId %d\n", top, bottom, idx->entries[i].docId, i, docId );
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

int IR_Intersect(IndexReader *r, IndexReader *other, IntersectHandler onIntersect, void *ctx) {
    
    IndexHit *hits = calloc(2, sizeof(IndexHit));
    IndexHit *h1= &hits[0];
    IndexHit *h2= &hits[1];
    int firstSeek = 1;
    int count = 0;
    
    if(IR_Read(r, h1) == INDEXREAD_EOF)  {
        return INDEXREAD_EOF;
    }
    
    do {
        if (!firstSeek && h1->docId == h2->docId) {
            count++;
            onIntersect(ctx, hits, 2);
            firstSeek = 0;
            goto readnext;
        } 
        
        firstSeek = 0;
        int rc = IR_SkipTo(other, h1->docId, h2);
        //LG_INFO("%d %d, rc %d\n", r->lastId, other->lastId, rc);
        switch(rc) {
            case INDEXREAD_EOF:
            return count;
            case INDEXREAD_NOTFOUND:
            
            if (IR_SkipTo(r, h2->docId, h1) == INDEXREAD_EOF) {
                return count;
            }
            continue;
            
            case INDEXREAD_OK:
              //LG_INFO("Intersection! @ %d <> %d\n", h1->docId, h2->docId);
              onIntersect(ctx, hits, 2);
              ++count;
              
        } 
           
readnext:
        if(IR_Read(r, h1) == INDEXREAD_EOF) break;  
    } while(1);
    
    return count;
    
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
        hits[i].docId = 0;
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

IndexIterator *NewUnionIterator(IndexIterator **its, int num) {
    
    // create union context
    UnionContext *ctx = calloc(1, sizeof(UnionContext));
    ctx->its =its;
    ctx->num = num;
    ctx->currentHits = calloc(num, sizeof(IndexHit));
    
    // bind the union iterator calls
    IndexIterator *it = malloc(sizeof(IndexIterator));
    it->ctx = ctx;
    it->LastDocId = UI_LastDocId;
    it->Read = UI_Read;
    it->SkipTo = UI_SkipTo;
    it->HasNext = UI_HasNext;
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
     IndexHit h;
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
 
 void IndexIterator_Free(IndexIterator *it) {
     UnionContext *ui = it->ctx;
     for (int i = 0; i < ui->num; i++) {
         IndexIterator_Free(ui->its[i]);
     }
     free(it->ctx);
     free(it);
 }
 
 
 
 
 IndexIterator *NewIntersecIterator(IndexIterator **its, int num, int exact) {
     // create context
    IntersectContext *ctx = calloc(1, sizeof(IntersectContext));
    ctx->its =its;
    ctx->num = num;
    ctx->lastDocId = 0;
    ctx->exact = exact;
    ctx->currentHits = calloc(num, sizeof(IndexHit));

    
    // bind the iterator calls
    IndexIterator *it = malloc(sizeof(IndexIterator));
    it->ctx = ctx;
    it->LastDocId = II_LastDocId;
    it->Read = II_Read;
    it->SkipTo = II_SkipTo;
    it->HasNext = II_HasNext;
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
             *hit = ic->currentHits[i];
             nfound++;
         } else if (ic->currentHits[i].docId > ic->lastDocId){
             ic->lastDocId = ic->currentHits[i].docId;
         }
         
     }
     if (nfound == ic->num) {
         return INDEXREAD_OK;
     }
     
     
     return INDEXREAD_NOTFOUND;
}


int II_Next(void *ctx) {
    IndexHit h;
    return II_Read(ctx, &h);
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
            *hit = ic->currentHits[0];
            
            // advance to the next iterator
            ic->its[0]->Read(ic->its[0]->ctx, &ic->currentHits[0]);
            
            ic->lastDocId = ic->currentHits[0].docId;
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
