#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>
#include <sys/types.h>

int decodeVarint(u_char **bufp);
int encodeVarint(int value, unsigned char *buf);
size_t varintSize(int value);

typedef u_char VarintVector;

typedef struct {
    VarintVector *v;
    u_char *pos;
    u_char index;
    int lastValue;
} VarintVectorIterator;


typedef struct {
    VarintVector *v;
    u_char *pos;
    // actual data length 
    size_t len;
    // how many members we've put in
    size_t nmemb;
    
    // allocated capacity
    size_t cap;
    
    int lastValue;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorIterator VarIntVector_iter(VarintVector *v);
int VV_HasNext(VarintVectorIterator *vi);
int VV_Next(VarintVectorIterator *vi);

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
size_t VVW_Write(VarintVectorWriter *w, int i);
size_t VVW_Truncate(VarintVectorWriter *w);
u_char VV_Size(VarintVector *vv);
void VVW_Free(VarintVectorWriter *w);



#define bitsizeof(x)  (8 * sizeof(x))
#ifdef __GNUC__
#define TYPEOF(x) (__typeof__(x))
#else
#define TYPEOF(x)
#endif
#define MSB(x, bits) ((x) & TYPEOF(x)(~0ULL << (bitsizeof(x) - (bits))))



#endif