/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "arr.h"

void array_debug(void *pp) {
  const array_hdr_t *hdr = array_hdr(pp);
  printf("Array: %p, hdr@%p", pp, hdr);
  printf("Len: %u. Cap: %u. ElemSize: %u\n", hdr->len, hdr->cap, hdr->elem_sz);
}
