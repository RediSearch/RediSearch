/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "toksep.h"

char *toksep(char **s, size_t *tokLen) {
  return toksep_inline(s, tokLen);
}

int istoksep(int c) {
  return istoksep_inline(c);
}
