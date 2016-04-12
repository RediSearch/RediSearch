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
    // e->freq = decodeVarint(&(ir->pos));
    // e->flags = *ir->pos++;
    // e->offsets = (VarintVector*)ir->pos;
    // ir->pos += VV_Size(e->offsets);
    // ir->lastId = e->docId;
    return INDEXREAD_OK;
}

IndexReader *NewIndexReader(void *data, size_t datalen) {
    IndexReader *ret = malloc(sizeof(IndexReader));
    ret->header = (IndexHeader *)data;
    ret->data = data;
    ret->datalen = datalen;
    ret->lastId = 0;
    ret->pos = (u_char*)data + sizeof(IndexHeader);
    return ret;
} 


size_t IW_Len(IndexWriter *w) {
    return w->pos - w->buf;
}

IndexWriter *NewIndexWriter(size_t cap) {
    IndexWriter *w = malloc(sizeof(IndexWriter));
    w->buf = malloc(cap + sizeof(IndexHeader));
    w->pos = w->buf + sizeof(IndexHeader);
    w->cap = cap;
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
    return w->cap;
}