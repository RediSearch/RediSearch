
#include "phonetic_manager.h"
#include "phonetics/double_metaphone.h"

#include "rmalloc.h"

#include <string.h>
#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

void PhoneticManager::AddPrefix(char **phoneticTerm) {
  if (!phoneticTerm || !*phoneticTerm) {
    return;
  }
  size_t len = strlen(*phoneticTerm) + 1;
  *phoneticTerm = rm_realloc(*phoneticTerm, sizeof(char*) * (len + 1));
  memmove((*phoneticTerm) + 1, *phoneticTerm, len);
  *phoneticTerm[0] = PHONETIC_PREFIX;
}

//---------------------------------------------------------------------------------------------

void PhoneticManager::ExpandPhonetics(std::string_view term, char **primary, char **secondary) {
  // currently we support only one universal algorithm for all 4 languages
  DoubleMetaphone(term.data(), primary, secondary);
  PhoneticManager::AddPrefix(secondary);
  PhoneticManager::AddPrefix(primary);
}

///////////////////////////////////////////////////////////////////////////////////////////////
