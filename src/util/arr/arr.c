/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "arr.h"

// define a function to be used from Rust
uint32_t array_len_func(array_t arr) {
  return array_len(arr);
}

void array_free(array_t arr) {
  if (arr != NULL) {
    // like free(), shouldn't explode if NULL
    array_free_fn(array_hdr(arr));
  }
}

/* Initialize a new array with a given element size and capacity. Should not be used directly - use
 * array_new instead */
array_t array_new_sz(uint16_t elem_sz, uint16_t remain_cap, uint32_t len) {
  array_hdr_t *hdr = (array_hdr_t *)array_alloc_fn(sizeof(array_hdr_t) + (uint64_t) (len + remain_cap) * elem_sz);
  hdr->remain_cap = remain_cap;
  hdr->len = len;
  hdr->elem_sz = elem_sz;
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
array_t array_clear_func(array_t arr, uint16_t elem_sz) {
  if (!arr) {
    arr = array_new_sz(elem_sz, 1, 0);
  } else {
    array_hdr(arr)->remain_cap += array_hdr(arr)->len;
    array_hdr(arr)->len = 0;
  }
  return arr;
}
