#ifndef __VARINT_H__
#define __VARINT_H__

#include <stdlib.h>

int decodeVarint(u_char **bufp);
int encodeVarint(int value, unsigned char *buf);


typedef u_char VarintVector;



typedef struct {
    VarintVector *v;
    u_char *pos;
    u_char index;
} VarintVectorIterator;


typedef struct {
    VarintVector *v;
    u_char *pos;
    size_t len;
    size_t cap;
} VarintVectorWriter;

#define MAX_VARINT_LEN 5

VarintVectorWriter *NewVarintVectorWriter(size_t cap);
int VVW_Write(VarintVectorWriter *w, int i);
void VVW_Finalize(VarintVectorWriter *w);
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