/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "arr.h"

void array_debug(void *pp) {
  const array_hdr_t *hdr = array_hdr(pp);
  printf("Array: %p, hdr@%p", pp, hdr);
  printf("Len: %u. Cap: %u. ElemSize: %u\n", hdr->len, hdr->cap, hdr->elem_sz);
}
