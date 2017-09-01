#include "../stemmer.h"
#include "../tokenize.h"
#include "test_util.h"
#include "../rmutil/alloc.h"
#include <string.h>

int testStemmer() {

  Stemmer *s = NewStemmer(SnowballStemmer, "en");
  ASSERT(s != NULL)

  size_t sl;
  const char *stem = s->Stem(s->ctx, "arbitrary", (size_t)strlen("arbitrary"), &sl);
  ASSERT(stem != NULL)
  ASSERT(!strcasecmp(stem, "arbitrari"));
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

int tokenFunc(void *ctx, const Token *t) {
  // printf("%s %d\n", t.s, t.type);

  tokenContext *tx = ctx;
  int ret = strncmp(t->tok, tx->expectedTokens[tx->num], t->tokLen);
  assert(ret == 0);
  assert(t->pos > 0);

  if (t->stem) {
    printf("Stem: %.*s, num=%lu, orig=%.*s\n", (int)t->stemLen, t->stem, tx->num, (int)t->tokLen,
           t->tok);
    assert(tx->expectedStems[tx->num]);
    assert(strlen(tx->expectedStems[tx->num]) == t->stemLen);
    assert(strncmp(tx->expectedStems[tx->num], t->stem, t->stemLen) == 0);
  } else {
    assert(tx->expectedStems[tx->num] == NULL);
  }
  tx->num++;
  return 0;
}

int testTokenize() {

  char *txt = strdup("Hello? world... worlds going ? -WAZZ@UP? שלום");

  const char *expectedToks[] = {"hello", "world", "worlds", "going", "wazz", "up", "שלום"};
  const char *expectedStems[] = {NULL /*hello*/,
                                 NULL /*world/*/,
                                 "world" /*worlds*/,
                                 "go" /*going*/,
                                 NULL /*wazz*/,
                                 NULL /*up*/,
                                 NULL /*שלום*/};
  tokenContext ctx = {0};
  ctx.expectedTokens = expectedToks;
  ctx.expectedStems = expectedStems;

  Stemmer *s = NewStemmer(SnowballStemmer, "en");
  ASSERT(s != NULL)

  tokenize(txt, &ctx, tokenFunc, s, 0, DefaultStopWordList(), 0);
  ASSERT(ctx.num == 7);

  free(txt);

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testStemmer);
  TESTFUNC(testTokenize);
});