#include "phonetic_manager.h"
#include "dep/phonetics/double_metaphone_capi.h"
#include <string.h>
#include "malloc.h"

static void PhoneticManager_AddPrefix(char** phoneticTerm){
  if(!phoneticTerm || !(*phoneticTerm)){
    return;
  }
  size_t len = strlen(*phoneticTerm) + 1;
  *phoneticTerm = realloc(*phoneticTerm, sizeof(char*) * (len + 1));
  memcpy((*phoneticTerm) + 1, *phoneticTerm, len);\
  *phoneticTerm[0] = PHONETIC_PREFIX;
}

void PhoneticManager_ExpandPhonerics(PhoneticManagerCtx* ctx, const char* term, size_t len,
                                     char** primary, char** secondary){
  // currently ctx is irrelevant we support only one universal algorithm for all 4 languages
  // this phonetic manager was built for future thinking and easily add more algorithms
  DoubleMetaphone_c(term, len, primary, secondary);
  PhoneticManager_AddPrefix(primary);
  PhoneticManager_AddPrefix(secondary);
}
