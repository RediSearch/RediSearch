#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <sys/param.h>
#include "varint.h"


static int msb = (int)(~0ULL << 25);

// int decodeVarint(u_char **bufp)
// {
// 	u_char *buf = *bufp;
// 	u_char c = *buf++;
// 	int val = c & 127;
    
// 	while (c & 128) {
// 		++val;
        
// 		if (!val || val & msb)
// 			return 0; /* overflow */
// 		c = *buf++;
// 		val = (val << 7) + (c & 127);
// 	}
// 	*bufp = buf;
// 	return val;
// }

int ReadVarint(Buffer *b) {
    u_char c;
    if (BufferReadByte(b, (char*)&c) == 0) {
        return 0;
    }
	int val = c & 127;
    
	while (c & 128) {
		++val;
        
		if (!val || val & msb)
			return 0; /* overflow */
		if (BufferReadByte(b, (char*)&c) == 0) { //EOF
            return 0;
        }
		val = (val << 7) + (c & 127);
	}
	return val;
}

int WriteVarint(int value, BufferWriter *w) {
    unsigned char varint[16];
	unsigned pos = sizeof(varint) - 1;
	varint[pos] = value & 127;
	while (value >>= 7)
		varint[--pos] = 128 | (--value & 127);
	
    return w->Write(w->buf, varint + pos, 16 - pos);
}



size_t varintSize(int value) {
    size_t outputSize = 1;
    while (value > 127) {
        value >>= 7;
        outputSize++;
    }
    
    return outputSize;
}




// int encodeVarint(int value, unsigned char *buf)
// {
// 	unsigned char varint[16];
// 	unsigned pos = sizeof(varint) - 1;
// 	varint[pos] = value & 127;
// 	while (value >>= 7)
// 		varint[--pos] = 128 | (--value & 127);
// 	if (buf)
// 		memcpy(buf, varint + pos, sizeof(varint) - pos);
// 	return sizeof(varint) - pos;
// }


VarintVectorIterator VarIntVector_iter(VarintVector *v) {
     VarintVectorIterator ret;
     ret.buf = v;
     ret.index = 0;
     ret.lastValue = 0;
     
     return ret;
}


int VV_HasNext(VarintVectorIterator *vi) {
    return !BufferAtEnd(vi->buf);
}


int VV_Next(VarintVectorIterator *vi) {
  if (VV_HasNext(vi)) {
      int i = ReadVarint(vi->buf);
      vi->lastValue = i;
      vi->index++;
      return i;
  }  
  
  return -1;
}

size_t VV_Size(VarintVector *vv) {
    if (vv->type == BUFFER_WRITE) {
        return BufferLen(vv);
    }
    // for readonly buffers the size is the capacity
    return vv->cap;
}


void VVW_Free(VarintVectorWriter *w) {
    w->bw.Release(w->v);
    free(w);
}



VarintVectorWriter *NewVarintVectorWriter(size_t cap) {
    VarintVectorWriter *w = malloc(sizeof(VarintVectorWriter));
    w->bw = NewBufferWriter(cap);
    w->v = w->bw.buf;
    w->lastValue = 0;
    w->nmemb = 0;
    
    return w;
}

/**
Write an integer to the vector. 
@param w a vector writer
@param i the integer we want to write
@retur 0 if we're out of capacity, the varint's actual size otherwise
*/
size_t VVW_Write(VarintVectorWriter *w, int i) {
    
    size_t n = WriteVarint( i - w->lastValue, &w->bw);
    if (n != 0) {
        w->nmemb += 1;
        w->lastValue = i;
    }
    return n;
}

// Truncate the vector 
size_t VVW_Truncate(VarintVectorWriter *w) {
    
    w->bw.Truncate(w->bw.buf, 0);
    w->v = w->bw.buf;
    return w->v->cap;
}

/** 
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list 
*/
int VV_MinDistance(VarintVector **vs, int num) {
    
    int minDist = 0;
    int dist = 0;
    
    VarintVectorIterator iters[num];
    int vals[num];
    int i;
    for (i = 0; i < num; i++) {
        iters[i] = VarIntVector_iter(vs[i]);
        vals[i] = VV_Next(&iters[i]);
        if (i >= 1) {
            dist += abs(vals[i] - vals[i-1]);
        }
    }
    
    minDist = dist;
    
    while (1) {
       // find the smallest iterator and advance it
       int minIdx = -1;
       
       for (i = 0; i < num; i++) {
           if (VV_HasNext(&iters[i]) &&
               (minIdx == -1 || vals[i] < vals[minIdx])) {
               minIdx = i;
           }
       }
       // all lists are at their end
       if (minIdx == -1) break;
       
       dist -= minIdx > 0 ? abs(vals[minIdx] - vals[minIdx-1]) : 0;
       dist -= minIdx < num -1 ? abs(vals[minIdx+1] - vals[minIdx]) : 0;
       
       vals[minIdx] = VV_Next(&iters[minIdx]);
       
       dist += minIdx > 0 ? abs(vals[minIdx] - vals[minIdx-1]) : 0;
       dist += minIdx < num -1 ? abs(vals[minIdx+1] - vals[minIdx]) : 0;
       
       
       minDist = MIN(dist,minDist);
    }
    
    return minDist;
    
    
}