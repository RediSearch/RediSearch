/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "test_util.h"
#include "fragmenter.h"
#include "rmutil/alloc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOREM_IPSUM_FILE "./lorem_ipsum.txt"
#define GENESIS_FILE "./genesis.txt"

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

#define SIMPLE_TERM(s) FRAGMENT_TERM(s, strlen(s), 1)
#define SCORED_TERM(s, score) FRAGMENT_TERM(s, strlen(s), score)

int testFragmentize() {
  char *lorem = getFile(GENESIS_FILE);
  const FragmentSearchTerm terms[] = {SCORED_TERM("adam", 1.5), SCORED_TERM("eve", 2),
                                      SIMPLE_TERM("good"),      SIMPLE_TERM("woman"),
                                      SCORED_TERM("man", 0.7),  SIMPLE_TERM("earth"),
                                      SCORED_TERM("evil", 1.3)};
  size_t nterms = sizeof(terms) / sizeof(terms[0]);
  FragmentList fragList;
  FragmentList_Init(&fragList, 8, 6);

  // Fragmentize
  FragmentList_FragmentizeBuffer(&fragList, lorem, NULL, DefaultStopWordList(), terms, nterms);
  size_t nfrags = FragmentList_GetNumFrags(&fragList);
  const Fragment *allFrags = FragmentList_GetFragments(&fragList);
  ASSERT(allFrags != NULL);
  ASSERT(nfrags != 0);

  HighlightTags tags = {.openTag = "<i>", .closeTag = "</i>"};
  char *hlRes = FragmentList_HighlightWholeDocS(&fragList, &tags);
  ASSERT(strlen(hlRes) > strlen(lorem));
  free(hlRes);

  static const size_t numFrags = 3;
  Array contexts[numFrags];
  memset(&contexts, 0, sizeof contexts[0] * numFrags);
  for (size_t ii = 0; ii < numFrags; ++ii) {
    Array_Init(&contexts[ii]);
  }

  FragmentList_HighlightFragments(&fragList, &tags, 15, contexts, numFrags,
                                  HIGHLIGHT_ORDER_SCOREPOS,
                                  DefaultDelimiterList());

  // for (size_t ii = 0; ii < numFrags; ++ii) {
  //   struct iovec *iovs = (struct iovec *)Buffer_GetData(&contexts[ii]);
  //   size_t niovs = BUFFER_GETSIZE_AS(&contexts[ii], struct iovec);
  //   printf("Frag[%lu]: NIOV=%lu\n", ii, niovs);
  //   for (size_t jj = 0; jj < niovs; ++jj) {
  //     const struct iovec *iov = iovs + jj;
  //     printf("[%lu][%lu]: %.*s\n", ii, jj, (int)iov->iov_len, iov->iov_base);
  //   }
  // }

  printf("Consolidated snippet ====\n");
  for (size_t ii = 0; ii < numFrags; ++ii) {
    struct iovec *iovs = (void *)contexts[ii].data;
    size_t niovs = ARRAY_GETSIZE_AS(&contexts[ii], struct iovec);
    for (size_t jj = 0; jj < niovs; ++jj) {
      const struct iovec *iov = iovs + jj;
      printf("%.*s", (int)iov->iov_len, (char *)iov->iov_base);
    }
    printf(" ... ");
  }

  free(lorem);
  FragmentList_Free(&fragList);
  for (size_t ii = 0; ii < numFrags; ++ii) {
    Array_Free(contexts + ii);
  }
  return 0;
}

TEST_MAIN({
  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();
  TESTFUNC(testFragmentize);
  StopWordList_FreeGlobals();
});