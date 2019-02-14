
#ifndef SRC_PHONETIC_MANAGER_H_
#define SRC_PHONETIC_MANAGER_H_

#include <stddef.h>

#define PHONETIC_PREFIX '<'

typedef struct {
  char* algorithm;
  char* language;
} PhoneticManagerCtx;

void PhoneticManager_ExpandPhonetics(PhoneticManagerCtx* ctx, const char* term, size_t len,
                                     char** primary, char** secondary);

#endif /* SRC_PHONETIC_MANAGER_H_ */
