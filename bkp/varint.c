#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "varint.h"


int decodeVarint(u_char **bufp)
{
	u_char *buf = *bufp;
	u_char c = *buf++;
	int val = c & 127;
	while (c & 128) {
		val += 1;
        
		if (!val || MSB(val, 7))
			return 0; /* overflow */
		c = *buf++;
		val = (val << 7) + (c & 127);
	}
	*bufp = buf;
	return val;
}



int encodeVarint(int value, unsigned char *buf)
{
	unsigned char varint[16];
	unsigned pos = sizeof(varint) - 1;
	varint[pos] = value & 127;
	while (value >>= 7)
		varint[--pos] = 128 | (--value & 127);
	if (buf)
		memcpy(buf, varint + pos, sizeof(varint) - pos);
	return sizeof(varint) - pos;
}



VarintVectorIterator VarIntVector_iter(VarintVector *v) {
     VarintVectorIterator ret = {v, (u_char*)v, 0};
     return ret;
}

int VV_HasNext(VarintVectorIterator *vi) {
    return VV_Size(vi->v) > vi->index;
}

int VV_Next(VarintVectorIterator *vi) {
  if (VV_HasNext(vi)) {
      int i = decodeVarint(&(vi->pos));
      vi->index++;
      return i;
  }  
  
  return -1;
}

u_char VV_Size(VarintVector *vv) {
    return *((u_char*)vv);
}


void VVW_Free(VarintVectorWriter *w) {
    free(w->v);
    free(w);
}



VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
    VarintVectorWriter *w = calloc(1, sizeof(VarintVectorWriter));
    w->cap = cap;
    w->len = 1;
    w->v = malloc(cap);
    w->pos = w->v + 1;
}

int VVW_Write(VarintVectorWriter *w, int i) {
    if (w->len + MAX_VARINT_LEN > w->cap) {
        w->cap *= 2;
        
        size_t diff = w->pos - w->v;
        w->v = realloc(w->v, w->cap);
        w->pos = w->v + diff;
    }
    size_t n = encodeVarint(i, w->pos);
    w->len += n;
    w->pos += n;
    
}

// Make sure the 
void VVW_Finalize(VarintVectorWriter *w) {
    
    *(u_char *)w->v = w->len;
    w->v = realloc(w->v, w->len+1);
}