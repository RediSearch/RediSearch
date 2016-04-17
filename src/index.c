#include "index.h"
#include <sys/param.h>

int IR_Read(void *ctx, IndexHit *e) {
    IndexReader *ir = ctx;
    if (ir->pos >= ir->data + ir->datalen) {
        return INDEXREAD_EOF;
    }
    
    e->docId = decodeVarint(&(ir->pos)) + ir->lastId;
    e->len = decodeVarint(&(ir->pos));
    e->freq = decodeVarint(&(ir->pos));
    e->flags = *ir->pos++;
    e->offsets = (VarintVector*)ir->pos;
    ir->pos += VV_Size(e->offsets);
    ir->lastId = e->docId;
    return INDEXREAD_OK;
}


inline int IR_HasNext(void *ctx) {
    IndexReader *ir = ctx;
    if (ir->pos >= ir->data + ir->datalen) {
        return 0;
    }
        
    return 1;
}

int IR_Next(void *ctx) {
    IndexReader *ir = ctx;
    if (ir->pos >= ir->data + ir->datalen) {
        return INDEXREAD_EOF;
    }
    
    u_char *pos = ir->pos;
    ir->lastId = decodeVarint(&(ir->pos)) + ir->lastId;
    u_int16_t len = decodeVarint(&(ir->pos));
    
    ir->pos = pos + len;
   
    return INDEXREAD_OK;
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    ir->lastId = docId;
    ir->pos = ir->data + offset;// + sizeof(IndexHeader);
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
        if (ent != NULL) {
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
    ret->header = (IndexHeader *)data;
    ret->data = data;
    ret->datalen = datalen;
    ret->lastId = 0;
    ret->skipIdxPos = 0;
    ret->pos = (u_char*)data + sizeof(IndexHeader);
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
    return w->pos - w->buf;
}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->buf = malloc(cap + sizeof(IndexHeader));
    w->pos = w->buf + sizeof(IndexHeader);
    w->cap = cap + sizeof(IndexHeader);
    w->ndocs = 0;
    w->lastId = 0;
    return w;
}




void IW_Write(IndexWriter *w, IndexHit *e) {
    
    size_t len = varintSize(e->docId - w->lastId) + varintSize(e->freq) + 1 + VV_Size(e->offsets);
    size_t lensize = varintSize(len);
    len += lensize;
    //just in case we jumped one order of magnitude
    len += varintSize(len) - lensize;
    
    
    if (IW_Len(w) + len > w->cap) {
        size_t offset = IW_Len(w);
      
        w->cap *=2;
        w->buf = (u_char*)realloc(w->buf, w->cap);
        w->pos = w->buf + offset;
    }
    
    
    // encode docId
    w->pos += encodeVarint(e->docId - w->lastId, w->pos);
    w->lastId = e->docId;
    // encode len
    w->pos += encodeVarint(len, w->pos);
    //encode freq
    w->pos += encodeVarint(e->freq, w->pos);
    //encode flags
    *(w->pos++) = e->flags;
    //write 
    memcpy(w->pos, e->offsets, VV_Size(e->offsets));
    w->pos += VV_Size(e->offsets);
    w->ndocs++;
}


size_t IW_Close(IndexWriter *w) {
    IndexHeader *h = (IndexHeader*)w->buf;
    h->size = w->ndocs;
    w->cap = IW_Len(w);
    w->buf = realloc(w->buf, w->cap);
    IW_MakeSkipIndex(w, SKIPINDEX_STEP);
    return w->cap;
}

void IW_MakeSkipIndex(IndexWriter *iw, int step) {
    IndexReader *ir = NewIndexReader(iw->buf, IW_Len(iw), NULL);
    
    
    SkipEntry *entries = calloc(ir->header->size/step, sizeof(SkipEntry));
    
    int i = 0, idx = 0;
    while (IR_Next(ir) != INDEXREAD_EOF) {
       if (++i % step == 0) {
           
           entries[idx].docId = ir->lastId;
           entries[idx].offset = ir->pos - ir->data;
           //LG_DEBUG("skipindex[%d]: docId %d, offset %d\n", idx, ir->lastId, entries[idx].offset);
           idx++;
       }
          
    }
    
    iw->skipIdx.entries = entries;
    iw->skipIdx.len = idx;
     
}

void IW_Free(IndexWriter *w) {
    free(w->skipIdx.entries);
    free(w->buf);
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
    
    
    if (idx->len == 0 || docId < idx->entries[0].docId) {
        return NULL;
    } if (docId > idx->entries[idx->len-1].docId) {
        *offset = idx->len - 1; 
        return &idx->entries[idx->len-1];
    }
    u_int top = idx->len, bottom = *offset;
    u_int i = *offset;
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
            //LG_INFO("Intersection! @ %d <> %d\n", h1->docId, h2->docId);
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

t_docId IR_LastDocId(void* ctx) {
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


t_docId UI_LastDocId(void *ctx) {
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