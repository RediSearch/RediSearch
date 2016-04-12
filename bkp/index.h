#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define MSB(x, bits) ((x) & TYPEOF(x)(~0ULL << (bitsizeof(x) - (bits))))

#define foo
 

#pragma pack(1)


typedef struct {
    u_int8_t len;
    char *data;
} VarintVector;

typedef struct {
    VarintVector *v;
    char *pos;
    int index;
} VarintVectorIterator;


typedef struct {
    u_int32_t docId;
    u_int16_t len; 
    u_int16_t freq;
    u_char flags;
    
} IndexHit;



#pragma pack() 
#endif