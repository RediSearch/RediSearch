/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "arr.h"

void array_free(array_t arr) {
  if (arr != NULL) {
    // like free(), shouldn't explode if NULL
    array_free_fn(array_hdr(arr));
  }
}

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
array_t array_new_sz(uint16_t elem_sz, uint16_t cap, uint32_t len) {
  array_hdr_t *hdr = (array_hdr_t *)array_alloc_fn(sizeof(array_hdr_t) + (uint64_t) (len + cap) * elem_sz);
  hdr->cap = cap;
  hdr->elem_sz = elem_sz;
  hdr->len = len;
  return (array_t)(hdr->buf);
}

/* Function declared as a symbol to allow invocation from Rust */
array_t array_ensure_append_n_func(array_t arr, array_t src, uint16_t n, uint16_t elem_sz) {
  if (!arr) {
    arr = array_new_sz(elem_sz, n, 0);
  }
  
  array_hdr_t *hdr = array_hdr(arr);
  size_t old_len = hdr->len;
  
  arr = array_grow(arr, n);
  hdr = array_hdr(arr);
  
  if (src) {
    memcpy((char *)arr + old_len * hdr->elem_sz, src, n * hdr->elem_sz);
  }
  
  return arr;
}

/* Function declared as a symbol to allow invocation from Rust */
array_t array_clear_func(array_t arr, size_t elem_sz) {
  return array_new_sz(elem_sz, 0, 0);
}