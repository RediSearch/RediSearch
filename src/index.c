#include "index.h"
#include "varint.h"
#include <sys/param.h>

#define SKIPINDEX_STEP 100


int IR_Read(void *ctx, IndexHit *e) {
    IndexReader *ir = ctx;
    if (BufferAtEnd(ir->br.buf)) {
        return INDEXREAD_EOF;
    }
    
    char *pos = ir->br.buf->pos;
    e->docId = ReadVarint(&ir->br) + ir->lastId;
    e->len = ReadVarint(&ir->br);
    //e->freq = ReadVarint(&ir->br);
    //ir->br.ReadByte(ir->br.buf, (char *)&e->flags);
    
    //size_t offsetsLen = ReadVarint(&ir->br); 
   
    //e->offsets =  NewBuffer(ir->br.buf->data, offsetsLen, BUFFER_READ);
    //ir->br.Skip(ir->br.buf, offsetsLen);
    ir->lastId = e->docId;
    size_t skip = e->len - (ir->br.buf->pos - pos);
    ir->br.Skip(ir->br.buf, skip);
    return INDEXREAD_OK;
}


inline int IR_HasNext(void *ctx) {
    IndexReader *ir = ctx;
    return !BufferAtEnd(ir->br.buf);
}

int IR_Next(void *ctx) {
    IndexReader *ir = ctx;
    if (!IR_HasNext(ir)) {
        return INDEXREAD_EOF;
    }
    
    
    char *pos = ir->br.buf->pos;
    ir->lastId = ReadVarint(&ir->br) + ir->lastId;
    u_int16_t len = ReadVarint(&ir->br);
    
    size_t skip = len - (ir->br.buf->pos - pos);
    ir->br.Skip(ir->br.buf, skip);
    return INDEXREAD_OK;
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    ir->br.Seek(ir->br.buf, offset);
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
    SkipEntry *ent = SkipIndex_Find(&ir->skipIdx, docId, &ir->skipIdxPos);
    
    //LG_DEBUG("docId %d, ir first docId %d, ent: %p\n",  docId,  ir->skipIdx.entries[0].docId, ent);
    if (ent != NULL || ir->skipIdx.len == 0 || docId <= ir->skipIdx.entries[0].docId) {
        if (ent != NULL && ent->offset > BufferOffset(ir->br.buf)) {
            //LG_DEBUG("Got entry %d,%d\n", ent->docId, ent->offset);
            IR_Seek(ir, ent->offset, ent->docId);
        }
        //LG_DEBUG("After seek - ir docId: %d\n", ir->lastId);
        
        while(IR_Read(ir, hit) != INDEXREAD_EOF) {
            //LG_DEBUG("finding %d, at %d %d\n", docId, hit->docId, ir->lastId);
            if (ir->lastId == docId) {
                return INDEXREAD_OK;
            } else if (ir->lastId > docId) {
                return INDEXREAD_NOTFOUND;
            }
        }
    }
    
    return INDEXREAD_EOF;
}


IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si) {
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->br = NewBufferReader(data, datalen);
    ret->br.Read(ret->br.buf, &ret->header, sizeof(IndexHeader));
    
    ret->lastId = 0;
    ret->skipIdxPos = 0;
    
    if (si != NULL) {
        ret->skipIdx = *si;
    }
    return ret;
} 


IndexIterator *NewIndexTerator(IndexReader *ir) {
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
    IndexHeader h = {w->ndocs};
    w->bw.Write(w->bw.buf, &h, sizeof(h));
}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->bw = NewBufferWriter(cap);
    w->ndocs = 0;
    w->lastId = 0;
    writeIndexHeader(w);
    return w;
}




void IW_Write(IndexWriter *w, IndexHit *e) {
    
    size_t offsetsSz = VV_Size(e->offsets);
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
    w->bw.Write(w->bw.buf, e->offsets->data, e->offsets->cap);
    
    w->ndocs++;
}


size_t IW_Close(IndexWriter *w) {
   

    w->bw.Truncate(w->bw.buf, 0);
    
    // write the header at the beginning
    membufferSeek(w->bw.buf, 0);
    writeIndexHeader(w);
    membufferSeek(w->bw.buf, w->bw.buf->cap);

    IW_MakeSkipIndex(w, SKIPINDEX_STEP);
    return w->bw.buf->cap;
}

void IW_MakeSkipIndex(IndexWriter *iw, int step) {
    IndexReader *ir = NewIndexReader(iw->bw.buf->data, iw->bw.buf->cap, NULL);

    SkipEntry *entries = calloc(ir->header.size/step, sizeof(SkipEntry));
    
    int i = 0, idx = 0;
    while (IR_Next(ir) != INDEXREAD_EOF) {
       if (++i % step == 0) {
           
           entries[idx].docId = ir->lastId;
           entries[idx].offset = ir->br.buf->pos - ir->br.buf->data;
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
    
    if (idx->len == 0 || docId < idx->entries[0].docId) {
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
         if (u->its[i]->HasNext(u->its[i]->ctx)) {
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
     int rc;
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
     free(it->ctx);
     free(it);
 }