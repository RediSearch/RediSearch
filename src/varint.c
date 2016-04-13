#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "varint.h"


static int msb = (int)(~0ULL << 25);

int decodeVarint(u_char **bufp)
{
	u_char *buf = *bufp;
	u_char c = *buf++;
	int val = c & 127;
    
	while (c & 128) {
		++val;
        
		if (!val || val & msb)
			return 0; /* overflow */
		c = *buf++;
		val = (val << 7) + (c & 127);
	}
	*bufp = buf;
	return val;
}

size_t varintSize(int value) {
    size_t outputSize = 1;
    while (value > 127) {
        value >>= 7;
        outputSize++;
    }
    
    return outputSize;
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
     VarintVectorIterator ret = {
         index: 0, lastValue: 0, pos: 0, v: 0
     };
     ret.index = 0;
     ret.lastValue = 0;
     ret.pos = v+sizeof(u_int16_t);
     ret.v = v;
     
     return ret;
}


int VV_HasNext(VarintVectorIterator *vi) {
    return VV_Size(vi->v) > vi->pos - vi->v;
}


int VV_Next(VarintVectorIterator *vi) {
  if (VV_HasNext(vi)) {
      int i = decodeVarint(&(vi->pos)) + vi->lastValue;
      vi->lastValue = i;
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
    VarintVectorWriter *w = malloc(sizeof(VarintVectorWriter));
    w->cap = cap;
    w->len = sizeof(u_int16_t);
    w->nmemb = 0;
    w->v = calloc(1, cap);
    w->pos = w->v + sizeof(u_int16_t);
    w->lastValue = 0;
    return w;
}

/**
Write an integer to the vector. 
@param w a vector writer
@param i the integer we want to write
@retur 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VVW_Write(VarintVectorWriter *w, int i) {
    // we cap the size at 65535
    if (w->len + MAX_VARINT_LEN > __UINT16_MAX__) {
        return 0;
    } 
    
    // see if we need to realloc due to missing capacity
    if (w->pos + MAX_VARINT_LEN > w->v + w->cap) {
        w->cap *= 2;
        size_t diff = w->pos - w->v;
        w->v = realloc(w->v, w->cap);
        w->pos = w->v + diff;
    }
    size_t n = encodeVarint(i - w->lastValue, w->pos);
    
    w->len +=n;
    w->nmemb += 1;
    w->pos += n;
    w->lastValue = i;
    *(u_int16_t *)w->v = (u_int16_t)w->len;
    return n;
}

// Truncate the vector 
size_t VVW_Truncate(VarintVectorWriter *w) {
    
    *(u_int16_t *)w->v = (u_int16_t)w->len;
    w->v = realloc(w->v, w->len);
    w->cap = w->len;
    return w->len;
}