#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>
#include "buffer.h"

int decodeVarint(u_char **bufp);
int encodeVarint(int value, unsigned char *buf);
size_t varintSize(int value);

int ReadVarint(Buffer *b);
int WriteVarint(int value, BufferWriter *w);

typedef Buffer VarintVector;

typedef struct {
    Buffer *buf;
    u_char index;
    int lastValue;
} VarintVectorIterator;


typedef struct {
    VarintVector *v;
    BufferWriter bw;
    // how many members we've put in
    size_t nmemb;
    int lastValue;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorIterator VarIntVector_iter(VarintVector *v);
int VV_HasNext(VarintVectorIterator *vi);
int VV_Next(VarintVectorIterator *vi);

/** 
Find the minimal distance between members of the vectos.
e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
@param vs a list of vector pointers
@param num the size of the list 
*/
int VV_MinDistance(VarintVector **vs, int num);


VarintVectorWriter *NewVarintVectorWriter(size_t cap);
size_t VVW_Write(VarintVectorWriter *w, int i);
size_t VVW_Truncate(VarintVectorWriter *w);
size_t VV_Size(VarintVector *vv);
void VVW_Free(VarintVectorWriter *w);



#endif