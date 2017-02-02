#ifndef __SKIP_INDEX_H__
#define __SKIP_INDEX_H__
#include "buffer.h"
#include "types.h"

// The size of a step in a skip index.
#define SKIPINDEX_STEP 100

// SkipEntry represents a single entry in a skip index
typedef struct {
  t_docId docId;
  t_offset offset;
} SkipEntry;

/*
A SkipIndex is a an array of {streamOffset,docId} pairs that allows
skipping quickly over inverted indexes during intersecions.

SkipIndexes are saved on separate redis keys for each word, and loaded only
during intersect queries.
*/
typedef struct {
  u_int len;
  SkipEntry *entries;
} SkipIndex;

/*
Find the closest skip entry for a given docId in a skip index.
If the an entry is not found we return null.

Otherwise we return the skipEntry that comes before the document, so we can
skip to it and scan one at a time from it
*/
SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int *offset);
int si_isPos(SkipIndex *idx, u_int i, t_docId docId);

/* Create a skip index from a buffer */
SkipIndex *NewSkipIndex(Buffer *b);

/* Free a skip index an all its resources */
void SkipIndex_Free(SkipIndex *si);

#endif