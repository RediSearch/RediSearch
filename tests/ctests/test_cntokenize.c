/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

// TODO: We might not need all these includes
#include "test_util.h"
#include "time_sample.h"

#include "src/buffer.h"
#include "src/index.h"
#include "src/inverted_index.h"
#include "src/index_result.h"
#include "src/query_parser/tokenizer.h"
#include "src/spec.h"
#include "src/tokenize.h"
#include "src/varint.h"

#include "rmutil/alloc.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <time.h>
#include <float.h>

static char *getFile(const char *name) {
  FILE *fp = fopen(name, "rb");
  if (fp == NULL) {
    perror(name);
    abort();
  }

  if (fseek(fp, 0, SEEK_END) != 0) {
    perror(name);
    abort();
  }
  size_t nbuf = ftell(fp);
  if (fseek(fp, 0, SEEK_SET) != 0) {
    perror(name);
    abort();
  }

  if (nbuf == 0) {
    fprintf(stderr, "File is empty!\n");
    abort();
  }

  char *buf = malloc(nbuf + 1);
  buf[nbuf] = '\0';

  size_t nr, offset = 0;
  do {
    nr = fread(buf + offset, 1, nbuf - offset, fp);
    offset += nr;
  } while (nr > 0);

  if (strlen(buf) == 0) {
    perror(name);
    abort();
  }
  fclose(fp);

  return buf;
}

static int testCnTokenize(void) {
  char *cnTxt = getFile("cn_sample.txt");
  RSTokenizer *cnTok = NewChineseTokenizer(NULL, NULL, 0, NULL);
  ASSERT(cnTok != NULL);
  cnTok->Start(cnTok, cnTxt, strlen(cnTxt), 0);
  Token t;
  uint32_t pos;
  while ((pos = cnTok->Next(cnTok, &t)) != 0) {
    // printf("Token: %.*s. Raw: %.*s. Pos=%u\n", (int)t.tokLen, t.tok, (int)t.rawLen, t.raw,
    // t.pos);
    ASSERT(pos == t.pos);
  }
  cnTok->Free(cnTok);
  free(cnTxt);
  return 0;
}

TEST_MAIN({
  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();
  TESTFUNC(testCnTokenize);
});
