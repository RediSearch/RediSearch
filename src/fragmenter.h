/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef FRAGMENTER_H
#define FRAGMENTER_H

#include <stdlib.h>
#include <stdint.h>
#include <sys/uio.h>
#include "util/array.h"
#include "stemmer.h"
#include "redisearch.h"
#include "stopwords.h"
#include "delimiters.h"
#include "byte_offsets.h"

/**
 *
 ## Implementation
The summarization/highlight subsystem is implemented using an environment-agnostic
highlighter/fragmenter, and a higher level which is integrated with RediSearch and
the query keyword parser.

The summarization process begins by tokenizing the requested field, and splitting
the document into *fragments*.

When a matching token (or its stemmed variant) is found, a distance counter begins.
This counts the number of tokens following the matched token. If another matching
token occurs before the maximum token distance has been exceeded, the counter is
reset to 0 and the fragment is extended.

Each time a token is found in a fragment, the fragment's score increases. The
score increase is dependent on the base token score (this is provided as
input to the fragmenter), and whether this term is being repeated, or if it is
a new occurrence (within the same fragment). New terms get higher scores; which
helps eliminate forms like "I said to Abraham: Abraham, why...".

The input score for each term is calculated based on the term's overall frequency
in the DB (lower frequency means higher score), but this is consider out of bounds
for the fragmenter.

Once all fragments are scored, they are then *contextualized*. The fragment's
context is determined to be X amount of tokens surrounding the given matched
tokens. Words in between the tokens are considered as well, ensuring that every
fragment is more or less the same size.
 */

typedef struct {
  uint32_t tokPos;
  uint32_t bytePos;
  uint32_t termId;
  uint32_t len;
  float score;
} FragmentTerm;

typedef struct {
  RSByteOffsetIterator *byteIter;
  RSOffsetIterator *offsetIter;
  RSQueryTerm *curMatchRec;
  uint32_t curTokPos;
  uint32_t curByteOffset;
  FragmentTerm tmpTerm;
} FragmentTermIterator;

int FragmentTermIterator_Next(FragmentTermIterator *iter, FragmentTerm **termInfo);
void FragmentTermIterator_InitOffsets(FragmentTermIterator *iter, RSByteOffsetIterator *bytesIter,
                                      RSOffsetIterator *offIter);

typedef struct {
  // Position in current fragment (bytes)
  uint32_t offset;

  // Length of the token. This might be a stem, so not necessarily similar to termId
  uint16_t len;

  // Index into FragmentList::terms
  uint16_t termId;
} TermLoc;

typedef struct Fragment {
  const char *buf;
  uint32_t len;

  // (token-wise) position of the last matched token
  uint32_t lastMatchPos;

  // How many tokens are in this fragment
  uint32_t totalTokens;

  // How many _matched_ tokens are in this fragment
  uint32_t numMatches;

  // Inverted ranking (from 0..n) in the score array. A lower number means a higher score
  uint32_t scoreRank;

  // Position within the array of fragments
  uint32_t fragPos;

  // Score calculated from the number of matches
  float score;
  Array termLocs;  // TermLoc
} Fragment;

typedef struct {
  // Array of fragments
  Array frags;

  // Array of indexes (in frags), sorted by score
  const Fragment **sortedFrags;

  // Scratch space, used internally
  const Fragment **scratchFrags;

  // Number of fragments
  uint32_t numFrags;

  // Number of tokens since last match. Used in determining 'context ratio'
  uint32_t numToksSinceLastMatch;

  const char *doc;
  uint32_t docLen;

  // Maximum allowable distance between relevant terms to be called a 'fragment'
  uint16_t maxDistance;

  // Average word size. Used when determining context.
  uint8_t estAvgWordSize;
} FragmentList;

static inline void FragmentList_Init(FragmentList *fragList, uint16_t maxDistance,
                                     uint8_t estWordSize) {
  fragList->doc = NULL;
  fragList->docLen = 0;
  fragList->numFrags = 0;
  fragList->maxDistance = maxDistance;
  fragList->estAvgWordSize = estWordSize;
  fragList->sortedFrags = NULL;
  fragList->scratchFrags = NULL;
  Array_Init(&fragList->frags);
}

static inline size_t FragmentList_GetNumFrags(const FragmentList *fragList) {
  return ARRAY_GETSIZE_AS(&fragList->frags, Fragment);
}

static const Fragment *FragmentList_GetFragments(const FragmentList *fragList) {
  return ARRAY_GETARRAY_AS(&fragList->frags, const Fragment *);
}

#define FRAGMENT_TERM(buf_, len_, score_) \
  { .tok = buf_, .len = len_, .score = score_ }
/**
 * A single term to use for searching. Used when fragmenting a buffer
 */
typedef struct {
  const char *tok;
  size_t len;
  float score;
} FragmentSearchTerm;

#define DOCLEN_NULTERM ((size_t)-1)

/**
 * Split a document into a list of fragments.
 * - doc is the document to split
 * - numTerms is the number of terms to search for. The terms themselves are
 *   not searched, but each Fragment needs to have this memory made available.
 *
 * Returns a list of fragments.
 */
void FragmentList_FragmentizeBuffer(FragmentList *fragList, const char *doc, Stemmer *stemmer,
                                    StopWordList *stopwords, const FragmentSearchTerm *terms,
                                    size_t numTerms);

#define FRAGMENTIZE_TOKLEN_EXACT 0x01
void FragmentList_FragmentizeIter(FragmentList *fragList, const char *doc, size_t docLen,
                                  FragmentTermIterator *iter, int options,
                                  const DelimiterList *dl);

typedef struct {
  const char *openTag;
  const char *closeTag;
} HighlightTags;

void FragmentList_Free(FragmentList *frags);

/** Highlight matches the entire document, returning a series of IOVs */
void FragmentList_HighlightWholeDocV(const FragmentList *fragList, const HighlightTags *tags,
                                     Array *iovs);

/** Highlight matches the entire document, returning it as a freeable NUL-terminated buffer */
char *FragmentList_HighlightWholeDocS(const FragmentList *fragList, const HighlightTags *tags);

/**
 * Return fragments by their score. The highest ranked fragment is returned fist
 */
#define HIGHLIGHT_ORDER_SCORE 0x01

/**
 * Return fragments by their order in the document. The fragment with the lowest
 * position is returned first.
 */
#define HIGHLIGHT_ORDER_POS 0x02

/**
 * First select the highest scoring elements and then sort them by position
 */
#define HIGHLIGHT_ORDER_SCOREPOS 0x03
/**
 * Highlight fragments for each document.
 *
 * - contextSize is the size of the surrounding context, in estimated words,
 * for each returned fragment. The function will use this as a hint.
 *
 * - iovBufList is an array of buffers. Each element corresponds to a fragment,
 * and the fragments are always returned in order.
 *
 * - niovs If niovs is less than the number of fragments, then only the first
 * <niov> fragments are returned.
 *
 * - order is one of the HIGHLIGHT_ORDER_ constants. See their documentation
 * for more details
 *
 */
void FragmentList_HighlightFragments(FragmentList *fragList, const HighlightTags *tags,
                                     size_t contextSize, Array *iovBufList, size_t niovs,
                                     int order, const DelimiterList *dl);

void FragmentList_Dump(const FragmentList *fragList);

#endif