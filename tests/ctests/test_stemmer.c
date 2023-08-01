/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "src/stemmer.h"
#include "src/tokenize.h"
#include "rmutil/alloc.h"
#include "test_util.h"

#include <string.h>

int testStemmer() {

  Stemmer *s = NewStemmer(SnowballStemmer, RS_LANG_ENGLISH);
  ASSERT(s != NULL)

  size_t sl;
  const char *stem = s->Stem(s->ctx, "arbitrary", (size_t)strlen("arbitrary"), &sl);
  ASSERT(stem != NULL)
  ASSERT(!strcasecmp(stem, "+arbitrari"));
  ASSERT(sl == strlen(stem));
  printf("stem: %s\n", stem);

  // free((void*)stem);
  s->Free(s);
  return 0;
}

typedef struct {
  int num;
  const char **expectedTokens;
  const char **expectedStems;
} tokenContext;

int testTokenize() {

  char *txt = strdup("Hello? world... worlds going ? -WAZZ@UP? שלום");

  const char *expectedToks[] = {"hello", "world", "worlds", "going", "wazz", "up", "שלום"};
  const char *expectedStems[] = {NULL /*hello*/,
                                 NULL /*world/*/,
                                 "+world" /*worlds*/,
                                 "+go" /*going*/,
                                 NULL /*wazz*/,
                                 NULL /*up*/,
                                 NULL /*שלום*/};
  tokenContext ctx = {0};
  ctx.expectedTokens = expectedToks;
  ctx.expectedStems = expectedStems;

  Stemmer *s = NewStemmer(SnowballStemmer, RS_LANG_ENGLISH);
  ASSERT(s != NULL)

  RSTokenizer *tk = NewSimpleTokenizer(s, DefaultStopWordList(), 0, NULL);
  Token t;

  tokenContext *tx = &ctx;
  tk->Start(tk, txt, strlen(txt), 0);
  while (tk->Next(tk, &t)) {
    printf("round %d\n", ctx.num);
    int ret = strncmp(t.tok, tx->expectedTokens[tx->num], t.tokLen);
    ASSERT(ret == 0);
    ASSERT(t.pos > 0);

    if (t.stem) {
      printf("Stem: %.*s, num=%d, orig=%.*s\n", (int)t.stemLen, t.stem, tx->num, (int)t.tokLen,
             t.tok);
      ASSERT(tx->expectedStems[tx->num]);
      ASSERT(strlen(tx->expectedStems[tx->num]) == t.stemLen);
      ASSERT(strncmp(tx->expectedStems[tx->num], t.stem, t.stemLen) == 0);
    } else {
      ASSERT(tx->expectedStems[tx->num] == NULL);
    }
    tx->num++;
  }

  ASSERT_EQUAL(ctx.num, 7);

  free(txt);
  tk->Free(tk);
  s->Free(s);

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testStemmer);
  TESTFUNC(testTokenize);
  StopWordList_FreeGlobals();
});
