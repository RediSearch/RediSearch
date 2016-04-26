#ifndef __FORWARD_INDEX_H__
#define __FORWARD_INDEX_H__
#include "types.h"
#include "util/khash.h"
#include "varint.h"
#include "tokenize.h"

typedef struct {
    const char *term;
    t_docId docId;
    u_int16_t freq;
    u_char flags;
    VarintVectorWriter *vw; 
} ForwardIndexEntry;





KHASH_MAP_INIT_STR(32, ForwardIndexEntry*)

// the quantizationn factor used to encode normalized (0..1) frquencies in the index
#define FREQ_QUANTIZE_FACTOR 127

typedef struct {
    khash_t(32) *hits;
    u_int totalFreq;
    u_int maxFreq;
    t_docId docId;
    float docScore;
    int uniqueTokens;
} ForwardIndex;


typedef struct {
    ForwardIndex *idx;
    khiter_t k;
} ForwardIndexIterator;


int forwardIndexTokenFunc(void *ctx, Token t);

void ForwardIndexFree(ForwardIndex *idx);
ForwardIndex *NewForwardIndex(t_docId docId, float docScore);
ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i);
ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter);
void ForwardIndex_NormalizeFreq(ForwardIndex *, ForwardIndexEntry*);


#endif