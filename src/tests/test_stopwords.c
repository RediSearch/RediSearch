#include "test_util.h"
#include <rmalloc.h>
#include <stopwords.h>

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

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testStopwordList);
  TESTFUNC(testDefaultStopwords);
});