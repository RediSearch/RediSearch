#include "index.h"

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

void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
    ir->lastId = docId;
    ir->pos = ir->data + offset + sizeof(IndexHeader);
}


int IR_SkipTo(IndexReader *ir, u_int32_t docId, IndexHit *hit) {
    
    SkipEntry *ent = SkipIndex_Find(&ir->skipIdx, docId, 0);
    if (ent != NULL || ir->skipIdx.len == 0) {
        if (ent != NULL) {
            IR_Seek(ir, ent->offset, ent->docId);
            
        }
        
        while(IR_Read(ir, hit) != INDEXREAD_EOF) {
            printf("%d\n", hit->docId);
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

size_t IW_Close(IndexWriter *w) {
    IndexHeader *h = (IndexHeader*)w->buf;
    h->size = w->ndocs;
    w->cap = IW_Len(w);
    w->buf = realloc(w->buf, w->cap);
    IW_MakeSkipIndex(w, 100);
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
           printf("skipindex[%d]: docId %d, offset %d\n", idx, ir->lastId, entries[idx].offset);
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

int iw_isPos(SkipIndex *idx, u_int i, t_docId docId) {
    if ((i == 0 || idx->entries[i].docId < docId) &&
            (i < idx->len - 1 && idx->entries[i+1].docId >= docId)) {
                return 1;
   }
   return 0;
}

SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int offset) {
    
    u_int top = idx->len, bottom = offset;
    u_int i = (top+bottom)/2;
    int newi;
    while (top > bottom) {
       if (iw_isPos(idx, i, docId)) {
           return &idx->entries[i];
       }
       
       if (docId < idx->entries[i].docId) {
           top = i;
       } else {
           bottom = i;
       }
       newi = (top+bottom)/2;
       if (newi == i) {
           break;
       }
       i = newi;
    }
    if (i == 0) {
        return &idx->entries[0];
    }
    return NULL;
}

int IR_Intersect(IndexReader *r, IndexReader *other) {
    
    IndexHit h1 = {0,0,0}, h2={0,0,0};
    int count = 0;
    while (IR_Read(r, &h1) != INDEXREAD_EOF) {
        int rc = IR_SkipTo(other, h1.docId, &h2);
        printf("%d %d, rc %d\n", r->lastId, other->lastId, rc);
        switch(rc) {
            case INDEXREAD_EOF:
            return count;
            case INDEXREAD_NOTFOUND:
            
            //if (IR_Read(r, &h1) == INDEXREAD_EOF) return count;
            break;
            case INDEXREAD_OK:
              printf("Intersection! @ %d <> %d\n", h1.docId, h2.docId);
              ++count;
              break;   
        } 

    }
    
    return 0;
    
}