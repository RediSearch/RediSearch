/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "phonetic_manager.h"
#include "phonetics/double_metaphone.h"
#include <string.h>
#include <stdlib.h>
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

#define MAX_STACK_ALLOC_TERM_SIZE 128

static void PhoneticManager_AddPrefix(char** phoneticTerm) {
  if (!phoneticTerm || !(*phoneticTerm)) {
    return;
  }
  size_t len = strlen(*phoneticTerm) + 1;
  *phoneticTerm = rm_realloc(*phoneticTerm, sizeof(char*) * (len + 1));
  memmove((*phoneticTerm) + 1, *phoneticTerm, len);
  *phoneticTerm[0] = PHONETIC_PREFIX;
}

void PhoneticManager_ExpandPhonetics(PhoneticManagerCtx* ctx, const char* term, size_t len,
                                     char** primary, char** secondary) {
  char *bufTmp;
  char stackStr[MAX_STACK_ALLOC_TERM_SIZE];

  // do not use heap allocation for short strings
  if (len < MAX_STACK_ALLOC_TERM_SIZE) {
    memcpy(stackStr, term, len);
    stackStr[len] = '\0';
    bufTmp = stackStr;
  } else {
    bufTmp = rm_strndup(term, len);
  }

  // currently ctx is irrelevant we support only one universal algorithm for all 4 languages
  // this phonetic manager was built for future thinking and easily add more algorithms
  DoubleMetaphone(bufTmp, primary, secondary);
  PhoneticManager_AddPrefix(primary);
  PhoneticManager_AddPrefix(secondary);

  // free memory if allocated
  if (len >= MAX_STACK_ALLOC_TERM_SIZE) {
    rm_free(bufTmp);
  }
}
