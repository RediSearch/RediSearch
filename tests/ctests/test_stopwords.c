/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "test_util.h"
#include <rmalloc.h>
#include <stopwords.h>
#include <rmutil/args.h>

void RMUTil_InitAlloc();

int testStopwordList() {

  char *terms[] = {strdup("foo"), strdup("bar"), strdup("שלום"), strdup("Hello"), strdup("WORLD")};
  const char *test_terms[] = {"foo", "bar", "שלום", "hello", "world"};

  StopWordList *sl = NewStopWordListCStr((const char **)terms, sizeof(terms) / sizeof(char *));
  ASSERT(sl != NULL);

  for (int i = 0; i < sizeof(test_terms) / sizeof(const char *); i++) {
    ASSERT(StopWordList_Contains(sl, test_terms[i], strlen(test_terms[i])));
  }

  ASSERT(!StopWordList_Contains(sl, "asdfasdf", strlen("asdfasdf")));
  ASSERT(!StopWordList_Contains(sl, NULL, 0));
  ASSERT(!StopWordList_Contains(NULL, NULL, 0));

  StopWordList_Free(sl);
  for (int i = 0; i < sizeof(terms) / sizeof(const char *); i++) {
    free(terms[i]);
  }
  return 0;
}

int testDefaultStopwords() {

  StopWordList *sl = DefaultStopWordList();
  for (int i = 0; DEFAULT_STOPWORDS[i] != NULL; i++) {
    ASSERT(StopWordList_Contains(sl, DEFAULT_STOPWORDS[i], strlen(DEFAULT_STOPWORDS[i])));
  }
  const char *test_terms[] = {"foo", "bar", "שלום", "hello", "world", "x", "i", "t"};
  for (int i = 0; i < sizeof(test_terms) / sizeof(const char *); i++) {
    // printf("checking %s\n", test_terms[i]);
    ASSERT(!StopWordList_Contains(sl, test_terms[i], strlen(test_terms[i])));
  }

  StopWordList_Free(sl);
  return 0;
}

int testStopwordListAC() {

  // Same inputs as testStopwordList, but consumed via an ArgsCursor.
  const char *terms[] = {"foo", "bar", "שלום", "Hello", "WORLD"};
  const char *test_terms[] = {"foo", "bar", "שלום", "hello", "world"};
  const size_t nterms = sizeof(terms) / sizeof(const char *);

  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, terms, nterms);

  StopWordList *sl = NewStopWordListAC(&ac);
  ASSERT(sl != NULL);
  // The cursor should have been fully consumed.
  ASSERT_EQUAL(0, AC_NumRemaining(&ac));

  for (int i = 0; i < sizeof(test_terms) / sizeof(const char *); i++) {
    ASSERT(StopWordList_Contains(sl, test_terms[i], strlen(test_terms[i])));
  }

  ASSERT(!StopWordList_Contains(sl, "asdfasdf", strlen("asdfasdf")));
  StopWordList_Free(sl);
  return 0;
}

int testStopwordListACEmpty() {

  // An empty cursor should produce a non-NULL (cached, empty) list.
  ArgsCursor ac;
  ArgsCursor_InitCString(&ac, NULL, 0);

  StopWordList *sl = NewStopWordListAC(&ac);
  ASSERT(sl != NULL);
  ASSERT(!StopWordList_Contains(sl, "foo", 3));
  StopWordList_Free(sl);
  return 0;
}

int testStopwordListEmbeddedNul() {

  // Case folding stops at an embedded NUL and shortens the length, so the key
  // actually stored is only the prefix before it. Reaching that state needs a
  // cursor type that carries an explicit length: AC_TYPE_CHAR derives the
  // length with strlen, while AC_TYPE_SDS (here) and AC_TYPE_RSTRING (the
  // FT.CREATE path in production) both pass the length through untouched.
  sds terms[] = {sdsnewlen("foo\0bar", 7)};

  ArgsCursor ac;
  ArgsCursor_InitSDS(&ac, terms, 1);

  StopWordList *sl = NewStopWordListAC(&ac);
  ASSERT(sl != NULL);

  // Add and lookup fold through the same code, so both truncate identically:
  // the full term matches, and so does the bare prefix.
  ASSERT(StopWordList_Contains(sl, "foo\0bar", 7));
  ASSERT(StopWordList_Contains(sl, "foo", 3));
  // Truncation is lossy — anything sharing the prefix collides.
  ASSERT(StopWordList_Contains(sl, "foo\0baz", 7));
  ASSERT(!StopWordList_Contains(sl, "f", 1));
  ASSERT(!StopWordList_Contains(sl, "bar", 3));

  StopWordList_Free(sl);
  sdsfree(terms[0]);
  return 0;
}

int testStopwordListInvalidUtf8() {

  // The folding decoder never validates its input: a lead byte consumes its
  // continuation bytes whatever they hold, mapping invalid sequences onto some
  // arbitrary key instead of rejecting them. What that key is does not matter
  // here; what matters is that add and lookup mangle a term the same way, so a
  // stopword given as invalid UTF-8 still suppresses the identical bytes at
  // query time.
  // Each sequence ends in a non-hex-digit character so the preceding \x escape
  // cannot swallow it.
  const char *terms[] = {"\xC3\x28z", "\xE0\x80q"};
  const size_t nterms = sizeof(terms) / sizeof(const char *);

  StopWordList *sl = NewStopWordListCStr(terms, nterms);
  ASSERT(sl != NULL);

  for (size_t i = 0; i < nterms; i++) {
    ASSERT(StopWordList_Contains(sl, terms[i], strlen(terms[i])));
  }

  ASSERT(!StopWordList_Contains(sl, "\xC3\x28y", 3));

  StopWordList_Free(sl);
  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testStopwordList);
  TESTFUNC(testStopwordListAC);
  TESTFUNC(testStopwordListACEmpty);
  TESTFUNC(testStopwordListEmbeddedNul);
  TESTFUNC(testStopwordListInvalidUtf8);
  TESTFUNC(testDefaultStopwords);
  StopWordList_FreeGlobals();
});
