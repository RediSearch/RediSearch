#include "fragmenter.h"
#include "toksep.h"
#include "tokenize.h"
#include <ctype.h>
#include <float.h>
#include <sys/uio.h>
#include <assert.h>

#define myMin(a, b) (a) < (b) ? (a) : (b)
#define myMax(a, b) (a) > (b) ? (a) : (b)

#ifdef __APPLE__
#define myQsort_r(arr, nelem, elemSize, compfn, arg) qsort_r(arr, nelem, elemSize, arg, compfn)
#define MY_QSORTR_ARGS(ctx, a, b) void *ctx, const void *a, const void *b
#else
#define myQsort_r(arr, nelem, elemSize, compfn, arg) qsort_r(arr, nelem, elemSize, compfn, arg)
#define MY_QSORTR_ARGS(ctx, a, b) const void *a, const void *b, void *ctx
#endif

// Estimated characters per token
#define EST_CHARS_PER_TOK 6

static Fragment *FragmentList_LastFragment(FragmentList *fragList) {
  if (!fragList->frags.len) {
    return NULL;
  }
  return (Fragment *)(fragList->frags.data + (fragList->frags.len - sizeof(Fragment)));
}

static Fragment *FragmentList_AddFragment(FragmentList *fragList) {
  Fragment *frag = Array_Add(&fragList->frags, sizeof(Fragment));
  memset(frag, 0, sizeof(*frag));
  frag->fragPos = fragList->numFrags++;
  return frag;
}

static size_t Fragment_GetNumTerms(const Fragment *frag) {
  return ARRAY_GETSIZE_AS(&frag->termLocs, TermLoc);
}

static int Fragment_HasTerm(const Fragment *frag, uint32_t termId) {
  TermLoc *locs = ARRAY_GETARRAY_AS(&frag->termLocs, TermLoc *);

  int firstOcurrence = 1;
  // If this is the first time the term appears in the fragment, increment the
  // fragment's score by the term's score. Otherwise, increment it by half
  // the fragment's score. This allows for better 'blended' results.
  for (size_t ii = 0; ii < Fragment_GetNumTerms(frag); ii++) {
    if (locs[ii].termId == termId) {
      return 1;
    }
  }
  return 0;
}

static Fragment *FragmentList_AddMatchingTerm(FragmentList *fragList, uint32_t termId,
                                              uint32_t tokPos, const char *tokBuf, size_t tokLen) {

  const FragmentTerm *term = &fragList->terms[termId];
  Fragment *curFrag = FragmentList_LastFragment(fragList);
  if (curFrag && tokPos - curFrag->lastMatchPos > fragList->maxDistance) {
    // There is too much distance between tokens for it to still be relevant.
    curFrag = NULL;
  }

  if (curFrag == NULL) {
    curFrag = FragmentList_AddFragment(fragList);
    fragList->numToksSinceLastMatch = 0;
    curFrag->buf = tokBuf;
  }

  if (!Fragment_HasTerm(curFrag, termId)) {
    curFrag->score += term->score;
  }

  curFrag->len = (tokBuf - curFrag->buf) + tokLen;
  curFrag->lastMatchPos = tokPos;
  curFrag->numMatches++;
  curFrag->totalTokens += fragList->numToksSinceLastMatch + 1;
  fragList->numToksSinceLastMatch = 0;

  TermLoc *newLoc = Array_Add(&curFrag->termLocs, sizeof(TermLoc));
  newLoc->termId = termId;
  newLoc->offset = tokBuf - curFrag->buf;
  newLoc->len = tokLen;

  return curFrag;
}

static int tokenizerCallback(void *ctx, const Token *tokInfo) {
  FragmentList *fragList = ctx;
  const FragmentTerm *term = NULL;
  uint32_t termId;
  int isStem = 0;

  // See if this token matches any of our terms.
  for (termId = 0; termId < fragList->numTerms; ++termId) {
    const FragmentTerm *cur = fragList->terms + termId;
    if (tokInfo->tokLen == cur->len && strncmp(tokInfo->tok, cur->tok, cur->len) == 0) {
    } else if (tokInfo->stem && tokInfo->stemLen == cur->len &&
               strncmp(tokInfo->stem, cur->tok, cur->len) == 0) {
      isStem = 1;
    } else {
      continue;
    }
    term = cur;
    break;
  }

  // Don't care about this token
  if (!term) {
    fragList->numToksSinceLastMatch++;
    return 0;
  }

  Fragment *curFrag =
      FragmentList_AddMatchingTerm(fragList, termId, tokInfo->pos, tokInfo->raw, tokInfo->rawLen);

  if (curFrag->score >= fragList->maxPossibleScore * 0.66) {
    // printf("Returning early because we found a good fragment!\n");
    return 1;
  }

  return 0;
}

void FragmentList_Fragmentize(FragmentList *fragList, const char *doc, Stemmer *stemmer,
                              StopWordList *stopwords) {
  fragList->doc = doc;
  fragList->docLen = strlen(doc);

  tokenize(doc, fragList, tokenizerCallback, stemmer, 0, stopwords, TOKENIZE_NOMODIFY);
  const Fragment *frags = FragmentList_GetFragments(fragList);

  float totalScore = 0;
  if (fragList->numFrags) {
    return;
  }
}

static void addToIov(const char *s, size_t n, Array *b) {
  if (n == 0 || s == NULL) {
    return;
  }
  struct iovec *iov = Array_Add(b, sizeof(*iov));
  assert(iov);
  iov->iov_base = (void *)s;
  iov->iov_len = n;
}

/**
 * Writes a complete fragment as a series of IOVs.
 * - fragment is the fragment to write
 * - tags is the tags to use
 * - contextLen is any amount of context used to surround the fragment with
 * - iovs is the target buffer in which the iovs should be written
 *
 * - preamble is any prior text which may need to be written alongside the fragment.
 *    In output, it contains the first byte after the fragment+context. This may be
 *    used as the 'preamble' value for a subsequent call to this function, if the next
 *    fragment being written is after the current one.
 */
static void Fragment_WriteIovs(const Fragment *curFrag, const char *openTag, size_t openLen,
                               const char *closeTag, size_t closeLen, Array *iovs,
                               const char **preamble) {

  const TermLoc *locs = ARRAY_GETARRAY_AS(&curFrag->termLocs, const TermLoc *);
  size_t nlocs = ARRAY_GETSIZE_AS(&curFrag->termLocs, TermLoc);
  const char *preamble_s = NULL;

  if (!preamble) {
    preamble = &preamble_s;
  }
  if (!*preamble) {
    *preamble = curFrag->buf;
  }

  for (size_t ii = 0; ii < nlocs; ++ii) {
    const TermLoc *curLoc = locs + ii;

    size_t preambleLen = (curFrag->buf + curLoc->offset) - *preamble;

    // Add any prior text
    if (preambleLen) {
      addToIov(*preamble, preambleLen, iovs);
    }

    if (openLen) {
      addToIov(openTag, openLen, iovs);
    }

    // Add the token itself
    addToIov(curFrag->buf + curLoc->offset, curLoc->len, iovs);

    // Add close tag
    if (closeLen) {
      addToIov(closeTag, closeLen, iovs);
    }

    *preamble = curFrag->buf + curLoc->offset + curLoc->len;
  }
}

void FragmentList_HighlightWholeDocV(const FragmentList *fragList, const HighlightTags *tags,
                                     Array *iovs) {
  const Fragment *frags = FragmentList_GetFragments(fragList);
  const char *preamble = NULL;
  size_t openLen = strlen(tags->openTag);
  size_t closeLen = strlen(tags->closeTag);

  for (size_t ii = 0; ii < fragList->numFrags; ++ii) {
    const Fragment *curFrag = frags + ii;
    Fragment_WriteIovs(curFrag, tags->openTag, openLen, tags->closeTag, closeLen, iovs, &preamble);
  }

  // Write the last preamble
  size_t preambleLen = strlen(preamble);
  if (preambleLen) {
    addToIov(preamble, preambleLen, iovs);
  }
}

char *FragmentList_HighlightWholeDocS(const FragmentList *fragList, const HighlightTags *tags) {
  Array iovsArr;
  Array_Init(&iovsArr);
  FragmentList_HighlightWholeDocV(fragList, tags, &iovsArr);

  // Calculate the length
  struct iovec *iovs = ARRAY_GETARRAY_AS(&iovsArr, struct iovec *);
  size_t niovs = ARRAY_GETSIZE_AS(&iovsArr, struct iovec);
  size_t docLen = 0;
  for (size_t ii = 0; ii < niovs; ++ii) {
    docLen += iovs[ii].iov_len;
  }

  char *docBuf = malloc(docLen + 1);
  docBuf[docLen] = '\0';

  assert(docBuf);
  size_t offset = 0;
  for (size_t ii = 0; ii < niovs; ++ii) {
    memcpy(docBuf + offset, iovs[ii].iov_base, iovs[ii].iov_len);
    offset += iovs[ii].iov_len;
  }

  Array_Free(&iovsArr);
  return docBuf;
}

static int fragSortCmp(MY_QSORTR_ARGS(ctx, pa, pb)) {
  const uint32_t a = *(uint32_t *)pa, b = *(uint32_t *)pb;
  const Fragment *frags = FragmentList_GetFragments(ctx);
  return frags[a].score == frags[b].score ? 0 : frags[a].score > frags[b].score ? -1 : 1;
}

static void FragmentList_Sort(FragmentList *fragList) {
  if (fragList->sortedFrags) {
    return;
  }

  const Fragment *origFrags = FragmentList_GetFragments(fragList);
  fragList->sortedFrags = malloc(sizeof(*fragList->sortedFrags) * fragList->numFrags);

  for (size_t ii = 0; ii < fragList->numFrags; ++ii) {
    fragList->sortedFrags[ii] = ii;
  }
  myQsort_r(fragList->sortedFrags, fragList->numFrags, sizeof fragList->sortedFrags[0], fragSortCmp,
            fragList);
  for (size_t ii = 0; ii < fragList->numFrags; ++ii) {
    ((Fragment *)origFrags)[fragList->sortedFrags[ii]].scoreRank = ii;
  }
}

static int sortByOrder(MY_QSORTR_ARGS(ctx, pa, pb)) {
  uint32_t a = *(uint32_t *)pa, b = *(uint32_t *)pb;
  const Fragment *frags = FragmentList_GetFragments(ctx);
  return (int)frags[a].fragPos - (int)frags[b].fragPos;
}

/**
 * Add context before and after the fragment.
 * - frag is the fragment to contextualize
 * - limitBefore, limitAfter are boundaries, such that the following will be
 *   true:
 *   - limitBefore <= before <= frag->buf
 *   - limitAfter > after >= frag->buf + frag->len
 *   If limitBefore is not specified, it defaults to the beginning of the fragList's doc
 *   If limitAfter is not specified, then the limit ends after the first NUL terminator.
 */
static void FragmentList_FindContext(const FragmentList *fragList, const Fragment *frag,
                                     const char *limitBefore, const char *limitAfter,
                                     size_t contextSize, struct iovec *before,
                                     struct iovec *after) {

  if (limitBefore == NULL) {
    limitBefore = fragList->doc;
  }
  if (limitAfter == NULL) {
    limitAfter = fragList->doc + fragList->docLen;
  }

  // Subtract the number of context (i.e. non-match) words
  // already inside the
  // snippet.
  if (contextSize <= frag->totalTokens - frag->numMatches) {
    before->iov_base = after->iov_base = NULL;
    before->iov_len = after->iov_len = 0;
    return;
  }

  contextSize -= (frag->totalTokens - frag->numMatches);

  // i.e. how much context before and after
  contextSize /= 2;

  // At some point we need to make a cutoff in terms of *bytes*
  contextSize *= fragList->estAvgWordSize;

  // TODO: If this context flows directly into a neighboring context, signal
  // some way to *merge* them.

  limitBefore = myMax(frag->buf - contextSize, limitBefore);
  limitAfter = myMin(frag->buf + frag->len + contextSize, limitAfter);

  before->iov_base = (void *)frag->buf;
  before->iov_len = 0;

  // Find the context immediately prior to our fragment, this means to advance
  // the cursor as much as possible until a separator is reached, and then
  // seek past that separator (if there are separators)
  for (; limitBefore < frag->buf && !istoksep(*limitBefore); limitBefore++) {
    // Found a separator.
  }
  for (; limitBefore < frag->buf && istoksep(*limitBefore); limitBefore++) {
    // Strip away future separators
  }
  before->iov_base = (void *)limitBefore;
  before->iov_len = frag->buf - limitBefore;

  // Do the same for the 'after' context.
  for (; limitAfter > frag->buf + frag->len && !istoksep(*limitAfter); limitAfter--) {
    // Found a separator
  }

  for (; limitAfter > frag->buf + frag->len && istoksep(*limitAfter); limitAfter--) {
    // Seek to the end of the last non-separator word
  }

  after->iov_base = (void *)frag->buf + frag->len;
  after->iov_len = limitAfter - (frag->buf + frag->len) + 1;
}

void FragmentList_HighlightFragments(FragmentList *fragList, const HighlightTags *tags,
                                     size_t contextSize, Array *iovArrList, size_t niovs,
                                     int order) {

  const Fragment *frags = FragmentList_GetFragments(fragList);
  niovs = myMin(niovs, fragList->numFrags);

  if (!fragList->scratchFrags) {
    fragList->scratchFrags = malloc(sizeof(fragList->scratchFrags) * fragList->numFrags);
  }
  uint32_t *indexes = fragList->scratchFrags;

  if (order == HIGHLIGHT_ORDER_POS) {
    for (size_t ii = 0; ii < niovs; ++ii) {
      indexes[ii] = ii;
    }
  } else if (order & HIGHLIGHT_ORDER_SCORE) {
    FragmentList_Sort(fragList);
    for (size_t ii = 0; ii < niovs; ++ii) {
      indexes[ii] = frags[fragList->sortedFrags[ii]].fragPos;
    }
    if (order & HIGHLIGHT_ORDER_POS) {
      myQsort_r(indexes, niovs, sizeof indexes[0], sortByOrder, fragList);
    }
  }

  size_t openLen = strlen(tags->openTag);
  size_t closeLen = strlen(tags->closeTag);

  for (size_t ii = 0; ii < niovs; ++ii) {
    Array *curArr = iovArrList + ii;
    Array_Init(curArr);

    const char *beforeLimit = NULL, *afterLimit = NULL;
    const Fragment *curFrag = frags + indexes[ii];

    if (order & HIGHLIGHT_ORDER_POS) {
      if (ii > 0) {
        beforeLimit = frags[indexes[ii - 1]].buf + frags[indexes[ii - 1]].len;
      }
      if (ii + 1 < niovs) {
        afterLimit = frags[indexes[ii + 1]].buf;
      }
    }

    struct iovec before, after;
    FragmentList_FindContext(fragList, curFrag, beforeLimit, afterLimit, contextSize, &before,
                             &after);
    addToIov(before.iov_base, before.iov_len, curArr);
    Fragment_WriteIovs(curFrag, tags->openTag, openLen, tags->closeTag, closeLen, curArr, NULL);
    addToIov(after.iov_base, after.iov_len, curArr);
  }
}

void FragmentList_Free(FragmentList *fragList) {
  Fragment *frags = (Fragment *)FragmentList_GetFragments(fragList);
  for (size_t ii = 0; ii < fragList->numFrags; ii++) {
    Array_Free(&frags[ii].termLocs);
  }
  Array_Free(&fragList->frags);
  free(fragList->sortedFrags);
  free(fragList->scratchFrags);
}

void FragmentList_Dump(const FragmentList *fragList) {
  printf("NumFrags: %u\n", fragList->numFrags);
  printf("Max Possible Score: %f\n", fragList->maxPossibleScore);
  for (size_t ii = 0; ii < fragList->numFrags; ++ii) {
    const Fragment *frag = ARRAY_GETITEM_AS(&fragList->frags, ii, Fragment *);
    printf("Frag[%lu]: Buf=%p, (pos=%lu), Len=%u\n", ii, frag->buf, frag->buf - fragList->doc,
           frag->len);
    printf("  Score: %f, Rank=%u. Total Tokens=%u\n", frag->score, frag->scoreRank,
           frag->totalTokens);
    printf("  BEGIN:\n");
    printf("     %.*s\n", (int)frag->len, frag->buf);
    printf("  END\n");
    printf("\n");
  }
}