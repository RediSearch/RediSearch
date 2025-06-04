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

void array_free(array_t arr) {
  if (arr != NULL) {
    // like free(), shouldn't explode if NULL
    array_free_fn(array_hdr(arr));
  }
}

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
array_t array_new_sz(uint32_t elem_sz, uint32_t cap, uint32_t len) {
  array_hdr_t *hdr = (array_hdr_t *)array_alloc_fn(sizeof(array_hdr_t) + (uint64_t)cap * elem_sz);
  hdr->cap = cap;
  hdr->elem_sz = elem_sz;
  hdr->len = len;
  return (array_t)(hdr->buf);
}
