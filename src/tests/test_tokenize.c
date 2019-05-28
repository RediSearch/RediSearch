#include "test_util.h"
#include "../tokenize.h"
#include "../stemmer.h"
#include "../rmutil/alloc.h"

int testTokenize() {
  Stemmer *st = NewStemmer(SnowballStemmer, "english");

  RSTokenizer *tk = GetSimpleTokenizer(st, DefaultStopWordList());
  char *txt = strdup("hello worlds    - - -,,, . . . -=- hello\\-world to be שלום עולם");
  const char *expected[] = {"hello", "worlds", "hello-world", "שלום", "עולם"};
  const char *stems[] = {NULL, "+world", NULL, NULL, NULL, NULL};
  tk->Start(tk, txt, strlen(txt), TOKENIZE_DEFAULT_OPTIONS);
  Token tok;
  int i = 0;
  while (tk->Next(tk, &tok)) {
    ;

    ASSERT(tok.tokLen == strlen(expected[i]));
    ASSERT(!strncmp(tok.tok, expected[i], tok.tokLen));
    if (!stems[i]) {
      ASSERT(tok.stem == NULL);
    } else {
      ASSERT(!strncmp(tok.stem, stems[i], tok.stemLen));
    }
    i++;
  }
  free(txt);

  RETURN_TEST_SUCCESS;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testTokenize);
})