#include "index.h"
#include <sys/param.h>

int IR_Read(IndexReader *ir, IndexHit *e) {
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

int IR_Next(IndexReader *ir) {
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


int IR_SkipTo(IndexReader *ir, u_int32_t docId, IndexHit *hit) {
    
    SkipEntry *ent = SkipIndex_Find(&ir->skipIdx, docId, &ir->skipIdxPos);
    LG_DEBUG("docId %d, ir first docId %d, ent: %p\n",  docId,  ir->skipIdx.entries[0].docId, ent);
    if (ent != NULL || ir->skipIdx.len == 0 || docId <= ir->skipIdx.entries[0].docId) {
        if (ent != NULL) {
            LG_DEBUG("Got entry %d,%d\n", ent->docId, ent->offset);
            IR_Seek(ir, ent->offset, ent->docId);
        }
        LG_DEBUG("After seek - ir docId: %d\n", ir->lastId);
        
        while(IR_Read(ir, hit) != INDEXREAD_EOF) {
            LG_DEBUG("finding %d, at %d %d\n", docId, hit->docId, ir->lastId);
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


#define SKIPINDEX_STEP 5

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
           LG_DEBUG("skipindex[%d]: docId %d, offset %d\n", idx, ir->lastId, entries[idx].offset);
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
        
    if (docId < idx->entries[0].docId) {
        return NULL;
    } if (docId > idx->entries[idx->len-1].docId) {
        *offset = idx->len - 1; 
        return &idx->entries[idx->len-1];
    }
    u_int top = idx->len, bottom = *offset;
    u_int i = *offset;
    int newi;
    while (bottom < top) {
        LG_DEBUG("top %d, bottom: %d idx %d, i %d, docId %d\n", top, bottom, idx->entries[i].docId, i, docId );
       if (iw_isPos(idx, i, docId)) {
           LG_DEBUG("IS POS!\n");
           *offset = i;
           return &idx->entries[i];
       }
       LG_DEBUG("NOT POS!\n");
       
       if (docId <= idx->entries[i].docId) {
           top = i ;
       } else {
           bottom = i ;
       }
       newi = (bottom + top)/2;
       LG_DEBUG("top %d, bottom: %d, new i: %d\n", top, bottom, newi);
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
            LG_INFO("Intersection! @ %d <> %d\n", h1->docId, h2->docId);
            count++;
            onIntersect(ctx, hits, 2);
            firstSeek = 0;
            goto readnext;
        } 
        
        firstSeek = 0;
        int rc = IR_SkipTo(other, h1->docId, h2);
        LG_INFO("%d %d, rc %d\n", r->lastId, other->lastId, rc);
        switch(rc) {
            case INDEXREAD_EOF:
            return count;
            case INDEXREAD_NOTFOUND:
            
            if (IR_SkipTo(r, h2->docId, h1) == INDEXREAD_EOF) {
                return count;
            }
            continue;
            
            case INDEXREAD_OK:
              LG_INFO("Intersection! @ %d <> %d\n", h1->docId, h2->docId);
              onIntersect(ctx, hits, 2);
              ++count;
              
        } 
           
readnext:
        if(IR_Read(r, h1) == INDEXREAD_EOF) break;  
    } while(1);
    
    return count;
    
}

int IR_Intersect2(IndexReader **argv, int argc, IntersectHandler onIntersect, void *ctx) {
    
    // nothing to do
    if (argc <= 0) {
        return 0;
    }
    
    IndexHit *hits = calloc(argc, sizeof(IndexHit));
    
    
    t_docId currentDoc = 0;
    int count = 0;
    
    int nh = 0;
    do {
        nh = 0;
        
        for (int i = 0; i < argc; i++) {
            IndexHit *h = &hits[i];
            
            // skip to the next
            if (h->docId != currentDoc || currentDoc == 0) {
                
                if (IR_SkipTo(argv[i], currentDoc, h) == INDEXREAD_EOF) {
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
            count++;
            
            if (IR_Read(argv[0], &hits[0]) == INDEXREAD_EOF) {
                return count;
            }
            currentDoc = hits[0].docId;
        }
    }while(1);
    
    return count;
    
}