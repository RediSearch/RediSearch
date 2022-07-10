#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <sys/uio.h>
#include "util/array.h"
#include "stemmer.h"
#include "tokenize.h"
#include "redisearch.h"
#include "stopwords.h"
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

struct FragmentTerm {
  uint32_t tokPos;
  uint32_t bytePos;
  uint32_t termId;
  uint32_t len;
  float score;
};

struct FragmentTermIterator {
  RSByteOffsetIterator *byteIter;
  RSOffsetIterator *offsetIter;
  RSQueryTerm *curMatchRec;
  uint32_t curTokPos;
  uint32_t curByteOffset;
  FragmentTerm tmpTerm;

  int Next(FragmentTerm **termInfo);
  void InitOffsets(RSByteOffsetIterator *bytesIter, RSOffsetIterator *offIter);
};

struct TermLoc {
  // Position in current fragment (bytes)
  uint32_t offset;

  // Length of the token. This might be a stem, so not necessarily similar to termId
  uint16_t len;

  // Index into FragmentList::terms
  uint16_t termId;
};

struct Fragment {
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
  Array<TermLoc> termLocs;  // TermLoc

  size_t GetNumTerms() const;

  int HasTerm(uint32_t termId) const;

  void WriteIovs(const char *openTag, size_t openLen, const char *closeTag,
                 size_t closeLen, Array<iovec *> iovs, const char **preamble) const;
};

struct HighlightTags {
  const char *openTag;
  const char *closeTag;

  HighlightTags(HighlightSettings settings) : openTag(settings.openTag), closeTag(settings.closeTag) {}
};

#define FRAGMENT_TERM(buf_, len_, score_) \
  { .tok = buf_, .len = len_, .score = score_ }
/**
 * A single term to use for searching. Used when fragmenting a buffer
 */
struct FragmentSearchTerm {
  const char *tok;
  size_t len;
  float score;
};

struct FragmentList {
  // Array of fragments
  Array<Fragment *> frags;

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

  FragmentList(uint16_t maxDistance, uint8_t estWordSize) {
    doc = NULL;
    docLen = 0;
    numFrags = 0;
    maxDistance = maxDistance;
    estAvgWordSize = estWordSize;
    sortedFrags = NULL;
    scratchFrags = NULL;
  }
  ~FragmentList();

  size_t GetNumFrags() const {
    return frags.ARRAY_GETSIZE_AS();
  }

  Fragment *GetFragments() const {
    return *frags.ARRAY_GETARRAY_AS();
  }

  void extractToken(const Token *tokInfo, const FragmentSearchTerm *terms, size_t numTerms);

  Fragment *LastFragment();
  Fragment *AddFragment();
  Fragment *AddMatchingTerm(uint32_t termId, uint32_t tokPos, const char *tokBuf, size_t tokLen,
                            float baseScore);
  void FragmentizeBuffer(const char *doc_, Stemmer *stemmer, StopWordList *stopwords,
                         const FragmentSearchTerm *terms, size_t numTerms);
  void FragmentizeIter(const char *doc_, size_t docLen, FragmentTermIterator *iter, int options);

  void HighlightWholeDocV(const HighlightTags *tags, Array<iovec *> iovs) const;
  char *HighlightWholeDocS(const HighlightTags *tags) const;

  void HighlightFragments(const HighlightTags *tags, size_t contextSize, Array<iovec *> *iovBufList,
                          size_t niovs, int order);

  void FindContext(const Fragment *frag, const char *limitBefore, const char *limitAfter,
                   size_t contextSize, struct iovec *before, struct iovec *after) const;

  void Sort();
  void Dump() const;
};

#define DOCLEN_NULTERM ((size_t)-1)

#define FRAGMENTIZE_TOKLEN_EXACT 0x01

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
