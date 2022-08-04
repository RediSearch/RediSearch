
#include "fragmenter.h"
#include "toksep.h"
#include "tokenize.h"
#include "util/minmax.h"
#include <ctype.h>
#include <float.h>
#include <sys/uio.h>
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Estimated characters per token
#define EST_CHARS_PER_TOK 6

//---------------------------------------------------------------------------------------------

Fragment *FragmentList::LastFragment() {
  if (frags.empty()) {
    return NULL;
  }
  return frags.back();
}

//---------------------------------------------------------------------------------------------

Fragment *FragmentList::AddFragment() {
  Fragment *frag;
  memset(frag, 0, sizeof(*frag));
  frag->fragPos = numFrags++;
  frags.push_back(frag);
  return frag;
}

//---------------------------------------------------------------------------------------------

size_t Fragment::GetNumTerms() const {
  return termLocs.size();
}

//---------------------------------------------------------------------------------------------

bool Fragment::HasTerm(uint32_t termId) const {
  // If this is the first time the term appears in the fragment, increment the
  // fragment's score by the term's score. Otherwise, increment it by half
  // the fragment's score. This allows for better 'blended' results.
  for (size_t i = 0; i < GetNumTerms(); i++) {
    if (termLocs[i].termId == termId) {
      return true;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

Fragment *FragmentList::AddMatchingTerm(uint32_t termId, uint32_t tokPos, const char *tokBuf,
                                        size_t tokLen, float baseScore) {
  Fragment *curFrag = LastFragment();
  if (curFrag && tokPos - curFrag->lastMatchPos > maxDistance) {
    // There is too much distance between tokens for it to still be relevant.
    curFrag = NULL;
  }

  if (curFrag == NULL) {
    curFrag = AddFragment();
    numToksSinceLastMatch = 0;
    curFrag->buf = tokBuf;
  }

  if (!curFrag->HasTerm(termId)) {
    curFrag->score += baseScore;
  }

  curFrag->len = (tokBuf - curFrag->buf) + tokLen;
  curFrag->lastMatchPos = tokPos;
  curFrag->numMatches++;
  curFrag->totalTokens += numToksSinceLastMatch + 1;
  numToksSinceLastMatch = 0;

  curFrag->termLocs.push_back(TermLoc(tokBuf - curFrag->buf, tokLen, termId));

  return curFrag;
}

//---------------------------------------------------------------------------------------------

void FragmentList::extractToken(const Token *tokInfo, const FragmentSearchTerm *terms, size_t numTerms) {
  const FragmentSearchTerm *term;
  uint32_t termId;

  // See if this token matches any of our terms.
  for (termId = 0; termId < numTerms; ++termId) {
    const FragmentSearchTerm *cur = terms + termId;
    if (tokInfo->tokLen == cur->len && strncmp(tokInfo->tok, cur->tok, cur->len) == 0) {
    } else if (tokInfo->stem && tokInfo->stemLen == cur->len &&
               strncmp(tokInfo->stem, cur->tok, cur->len) == 0) {
    } else {
      continue;
    }
    term = cur;
    break;
  }

  // Don't care about this token
  if (!term) {
    numToksSinceLastMatch++;
    return;
  }

  AddMatchingTerm(termId, tokInfo->pos, tokInfo->raw, tokInfo->rawLen, term->score);
}

//---------------------------------------------------------------------------------------------

// Split a document into a list of fragments.
// - doc is the document to split
// - numTerms is the number of terms to search for. The terms themselves are
//   not searched, but each Fragment needs to have this memory made available.
//
// Returns a list of fragments.

void FragmentList::FragmentizeBuffer(const char *doc_, Stemmer *stemmer, StopWordList *stopwords,
                                     const FragmentSearchTerm *terms, size_t numTerms) {
  doc = doc_;
  docLen = strlen(doc_);
  SimpleTokenizer tokenizer(stemmer, stopwords, TOKENIZE_NOMODIFY);
  tokenizer.Start((char *)doc, docLen, 0);
  Token tokInfo;
  while (tokenizer.Next(&tokInfo)) {
    extractToken(&tokInfo, terms, numTerms);
  }
}

//---------------------------------------------------------------------------------------------

static void addToIov(const char *s, size_t n, Vector<iovec> &b) {
  if (n == 0 || s == NULL) {
    return;
  }
  b.emplace_back(iovec{s, n});
}

//---------------------------------------------------------------------------------------------

// Writes a complete fragment as a series of IOVs.
// - fragment is the fragment to write
// - tags is the tags to use
// - contextLen is any amount of context used to surround the fragment with
// - iovs is the target buffer in which the iovs should be written
//
// - preamble is any prior text which may need to be written alongside the fragment.
//    In output, it contains the first byte after the fragment+context. This may be
//    used as the 'preamble' value for a subsequent call to this function, if the next
//    fragment being written is after the current one.

void Fragment::WriteIovs(const char *openTag, size_t openLen, const char *closeTag,
                         size_t closeLen, Vector<iovec> &iovs, const char **preamble) const {
  size_t nlocs = termLocs.size();
  const char *preamble_s = NULL;

  if (!preamble) {
    preamble = &preamble_s;
  }
  if (!*preamble) {
    *preamble = buf;
  }

  for (size_t i = 0; i < nlocs; ++i) {
    const TermLoc curLoc = termLocs[i];
    size_t preambleLen = (buf + curLoc.offset) - *preamble;

    // Add any prior text
    if (preambleLen) {
      addToIov(*preamble, preambleLen, iovs);
    }

    if (openLen) {
      addToIov(openTag, openLen, iovs);
    }

    // Add the token itself
    addToIov(buf + curLoc.offset, curLoc.len, iovs);

    // Add close tag
    if (closeLen) {
      addToIov(closeTag, closeLen, iovs);
    }

    *preamble = buf + curLoc.offset + curLoc.len;
  }
}

//---------------------------------------------------------------------------------------------

// Highlight matches the entire document, returning a series of IOVs
Vector<iovec> FragmentList::HighlightWholeDocV(const HighlightTags &tags) const {
  Vector<iovec> iovs;

  if (!numFrags) {
    // Whole doc, but no matches found
    addToIov(doc, docLen, iovs);
    return iovs;
  }

  const char *preamble = doc;
  size_t openLen = strlen(tags.openTag);
  size_t closeLen = strlen(tags.closeTag);

  for (size_t i = 0; i < numFrags; ++i) {
    const Fragment *curFrag = frags[i];
    curFrag->WriteIovs(tags.openTag, openLen, tags.closeTag, closeLen, iovs, &preamble);
  }

  // Write the last preamble
  size_t preambleLen = (doc + docLen) - preamble;
  // size_t preambleLen = strlen(preamble);
  if (preambleLen) {
    addToIov(preamble, preambleLen, iovs);
  }

  return iovs;
}

//---------------------------------------------------------------------------------------------

// Highlight matches the entire document, returning it as a freeable NUL-terminated buffer

char *FragmentList::HighlightWholeDocS(const HighlightTags &tags) const {
  Vector<iovec> iovs = HighlightWholeDocV(tags);

  // Calculate the length
  size_t docLen = 0;
  for (auto &iov: iovs) {
    docLen += iov.iov_len;
  }

  char *docBuf = rm_malloc(docLen + 1);
  RS_LOG_ASSERT(docBuf, "failed malloc of docBuf");

  size_t offset = 0;
  for (auto &iov: iovs) {
    memcpy(docBuf + offset, iov.iov_base, iov.iov_len);
    offset += iov.iov_len;
  }

  docBuf[docLen] = '\0';
  return docBuf;
}

//---------------------------------------------------------------------------------------------

static int fragSortCmp(const void *pa, const void *pb) {
  const Fragment *a = *(const Fragment **)pa, *b = *(const Fragment **)pb;
  if (a->score == b->score) {
    return a - b;
  }
  return a->score > b->score ? -1 : 1;
}

//---------------------------------------------------------------------------------------------

void FragmentList::Sort() {
  if (sortedFrags) {
    return;
  }

  sortedFrags = rm_malloc(sizeof(*sortedFrags) * numFrags);

  for (size_t i = 0; i < numFrags; ++i) {
    sortedFrags[i] = frags[i];
  }

  qsort(sortedFrags, numFrags, sizeof(sortedFrags[0]), fragSortCmp);
  for (size_t i = 0; i < numFrags; ++i) {
    ((Fragment *)sortedFrags[i])->scoreRank = i;
  }
}

//---------------------------------------------------------------------------------------------

static int sortByOrder(const void *pa, const void *pb) {
  const Fragment *a = *(const Fragment **)pa, *b = *(const Fragment **)pb;
  return (int)a->fragPos - (int)b->fragPos;
}

//---------------------------------------------------------------------------------------------

// Add context before and after the fragment.
// - frag is the fragment to contextualize
// - limitBefore, limitAfter are boundaries, such that the following will be
//   true:
//   - limitBefore <= before <= frag->buf
//   - limitAfter > after >= frag->buf + frag->len
//   If limitBefore is not specified, it defaults to the beginning of the fragList's doc
//   If limitAfter is not specified, then the limit ends after the first NUL terminator.

void FragmentList::FindContext(const Fragment *frag, const char *limitBefore, const char *limitAfter,
                               size_t contextSize, struct iovec *before, struct iovec *after) const {
  if (limitBefore == NULL) {
    limitBefore = doc;
  }
  if (limitAfter == NULL) {
    limitAfter = doc + docLen - 1;
  }

  // Subtract the number of context (i.e. non-match) words
  // already inside the
  // snippet.
  if (contextSize <= frag->totalTokens - frag->numMatches) {
    before->iov_base = after->iov_base = NULL;
    before->iov_len = after->iov_len = 0;
    return;
  }

  contextSize -= frag->totalTokens - frag->numMatches;

  // i.e. how much context before and after
  contextSize /= 2;

  // At some point we need to make a cutoff in terms of *bytes*
  contextSize *= estAvgWordSize;

  // TODO: If this context flows directly into a neighboring context, signal
  // some way to *merge* them.

  limitBefore = Max(frag->buf - contextSize, limitBefore);
  limitAfter = Min(frag->buf + frag->len + contextSize, limitAfter);

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

//---------------------------------------------------------------------------------------------

// Attempts to fragmentize a single field from its offset entries. This takes
// the field name, gets the matching field ID, retrieves the offset iterator
// for the field ID, and fragments the text based on the offsets. The fragmenter
// itself is in fragmenter.{c,h}
//
// Returns true if the fragmentation succeeded, false otherwise.

bool FragmentList::fragmentizeOffsets(IndexSpec *spec, const char *fieldName, const char *fieldText,
    size_t fieldLen, const IndexResult *indexResult, const RSByteOffsets *byteOffsets, int options) {
  const FieldSpec *fs = spec->GetField(fieldName, strlen(fieldName));
  if (!fs || !fs->IsFieldType(INDEXFLD_T_FULLTEXT)) {
    return false;
  }

  std::unique_ptr<RSOffsetIterator> offsIter = indexResult->IterateOffsets();
  RSByteOffsetIterator bytesIter(*byteOffsets, fs->ftId);
  if (!bytesIter.valid) {
    return false;
  }

  FragmentTermIterator fragIter(bytesIter, *offsIter);
  FragmentizeIter(fieldText, fieldLen, fragIter, options);
  if (numFrags == 0) {
    return false;
  }

  return true;
}

//---------------------------------------------------------------------------------------------

// Highlight fragments for each document.
//
// - contextSize is the size of the surrounding context, in estimated words,
// for each returned fragment. The function will use this as a hint.
//
// - iovBufList is an array of buffers. Each element corresponds to a fragment,
// and the fragments are always returned in order.
//
// - niovs If niovs is less than the number of fragments, then only the first
// <niov> fragments are returned.
//
// - order is one of the HIGHLIGHT_ORDER_ constants. See their documentation
// for more details

void FragmentList::HighlightFragments(const HighlightTags &tags, size_t contextSize, IOVecArrays &iovArrays,
                                      int order) {
  size_t niovs = Min(iovArrays.size(), numFrags);

  if (!scratchFrags) {
    scratchFrags = rm_malloc(sizeof(*scratchFrags) * numFrags);
  }
  const Fragment **indexes = scratchFrags;

  if (order == HIGHLIGHT_ORDER_POS) {
    for (size_t i = 0; i < niovs; ++i) {
      indexes[i] = frags[i];
    }
  } else if (order & HIGHLIGHT_ORDER_SCORE) {
    Sort();
    for (size_t i = 0; i < niovs; ++i) {
      indexes[i] = sortedFrags[i];
    }
    if (order & HIGHLIGHT_ORDER_POS) {
      qsort(indexes, niovs, sizeof indexes[0], sortByOrder);
    }
  }

  size_t openLen = tags.openTag ? strlen(tags.openTag) : 0;
  size_t closeLen = tags.closeTag ? strlen(tags.closeTag) : 0;

  int i = 0;
  for (auto &iovs: iovArrays) {
    const char *beforeLimit = NULL, *afterLimit = NULL;
    const Fragment *curFrag = indexes[i];

    if (order & HIGHLIGHT_ORDER_POS) {
      if (i > 0) {
        beforeLimit = indexes[i - 1]->buf + indexes[i - 1]->len;
      }
      if (i + 1 < niovs) {
        afterLimit = indexes[i + 1]->buf;
      }
    }

    struct iovec before, after;
    FindContext(curFrag, beforeLimit, afterLimit, contextSize, &before, &after);
    addToIov(before.iov_base, before.iov_len, iovs);
    curFrag->WriteIovs(tags.openTag, openLen, tags.closeTag, closeLen, iovs, NULL);
    addToIov(after.iov_base, after.iov_len, iovs);

    ++i;
  }
}

//---------------------------------------------------------------------------------------------

FragmentList::~FragmentList() {
  rm_free(sortedFrags);
  rm_free(scratchFrags);
}

//---------------------------------------------------------------------------------------------

// Tokenization:
// If we have term offsets and document terms, we can skip the tokenization process.
//
// 1) Gather all matching terms for the documents, and get their offsets (in position)
// 2) Sort all terms, by position
// 3) Start reading the byte offset list, until we reach the first term of the match
//    list, then, consume the matches until the maximum distance has been reached,
//    noting the terms for each.

void FragmentList::FragmentizeIter(const char *doc_, size_t docLen,
                                   FragmentTermIterator &iter, int options) {
  docLen = docLen;
  doc = doc_;
  size_t lastTokPos = -1;
  size_t lastByteEnd = 0;

  FragmentTerm *curTerm;
  while (iter.Next(&curTerm)) {
    if (curTerm == NULL) {
      numToksSinceLastMatch++;
      continue;
    }

    if (curTerm->tokPos == lastTokPos) {
      continue;
    }

    if (curTerm->bytePos < lastByteEnd) {
      // If our length estimations are off, don't use already-swallowed matches
      continue;
    }

    // Get the length of the current token. This is used to highlight the term
    // (if requested), and just terminates at the first non-separator character
    size_t len;
    if (options & FRAGMENTIZE_TOKLEN_EXACT) {
      len = curTerm->len;
    } else {
      len = 0;
      for (size_t i = curTerm->bytePos; i < docLen && !istoksep(doc_[i]); ++i, ++len) { //@@Is something happening here??
      }
    }

    AddMatchingTerm(curTerm->termId, curTerm->tokPos, doc_ + curTerm->bytePos, len, curTerm->score);
    lastTokPos = curTerm->tokPos;
    lastByteEnd = curTerm->bytePos + len;
  }
}

//---------------------------------------------------------------------------------------------

FragmentTermIterator::FragmentTermIterator(RSByteOffsetIterator &byteOffsets, RSOffsetIterator &offIter) :
    byteIter(byteOffsets), offsetIter(offIter) {
  curByteOffset = byteIter.Next();

  // Advance the offset iterator to the first offset we care about (i.e. that
  // correlates with the first byte offset)
  do {
    curTokPos = offsetIter.Next(&curMatchRec);
  } while (byteIter.curPos > curTokPos);
}

//---------------------------------------------------------------------------------------------

int FragmentTermIterator::Next(FragmentTerm **termInfo) {
  if (curMatchRec == NULL || curByteOffset == RSBYTEOFFSET_EOF || curTokPos == RS_OFFSETVECTOR_EOF) {
    return 0;
  }

  if (byteIter.curPos < curTokPos) {
    curByteOffset = byteIter.Next();
    // No matching term at this position.
    // printf("IterPos=%lu. LastMatchPos=%u\n", byteIter->curPos, curTokPos);
    *termInfo = NULL;
    return 1;
  }

  // printf("ByteOffset=%lu. LastMatchPos=%u\n", curByteOffset, curTokPos);

  RSQueryTerm *term = curMatchRec;

  // printf("Term Pointer: %p\n", term);
  aTerm.score = term->idf;
  aTerm.termId = term->id;
  aTerm.len = term->len;
  aTerm.tokPos = curTokPos;
  aTerm.bytePos = curByteOffset;
  *termInfo = &aTerm;

  uint32_t nextPos = offsetIter.Next(&curMatchRec);
  if (nextPos != curTokPos) {
    curByteOffset = byteIter.Next();
  }
  curTokPos = nextPos;
  return 1;
}

//---------------------------------------------------------------------------------------------

void FragmentList::Dump() const {
  printf("NumFrags: %u\n", numFrags);
  for (size_t i = 0; i < numFrags; ++i) {
    const Fragment *frag = frags[i];
    printf("Frag[%lu]: Buf=%p, (pos=%lu), Len=%u\n", i, frag->buf, frag->buf - doc,
           frag->len);
    printf("  Score: %f, Rank=%u. Total Tokens=%u\n", frag->score, frag->scoreRank,
           frag->totalTokens);
    printf("  BEGIN:\n");
    printf("     %.*s\n", (int)frag->len, frag->buf);
    printf("  END\n");
    printf("\n");
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
