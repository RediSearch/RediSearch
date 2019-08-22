#include "phonetic_manager.h"
#include "dep/phonetics/double_metaphone.h"
#include <string.h>
#include <stdlib.h>
#include "rmalloc.h"

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
  // currently ctx is irrelevant we support only one universal algorithm for all 4 languages
  // this phonetic manager was built for future thinking and easily add more algorithms
  char bufTmp[len + 1];
  bufTmp[len] = 0;
  memcpy(bufTmp, term, len);
  DoubleMetaphone(bufTmp, primary, secondary);
  PhoneticManager_AddPrefix(primary);
  PhoneticManager_AddPrefix(secondary);
}
