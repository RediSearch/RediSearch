/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#ifndef SRC_PHONETIC_MANAGER_H_
#define SRC_PHONETIC_MANAGER_H_

#include <stddef.h>

#define PHONETIC_PREFIX '<'

typedef struct {
  char* algorithm;
  //RSLanguage language; // not currently used
} PhoneticManagerCtx;

void PhoneticManager_ExpandPhonetics(PhoneticManagerCtx* ctx, const char* term, size_t len,
                                     char** primary, char** secondary);

#endif /* SRC_PHONETIC_MANAGER_H_ */
