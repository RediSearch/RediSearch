
#ifndef SRC_PHONETIC_MANAGER_H_
#define SRC_PHONETIC_MANAGER_H_

#include <stddef.h>

#define PHONETIC_PREFIX '<'

struct PhoneticManagerCtx {
  char* algorithm;
  //RSLanguage language; // not currently used

  static void ExpandPhonetics(const char* term, size_t len, char** primary, char** secondary);
};

#endif /* SRC_PHONETIC_MANAGER_H_ */
