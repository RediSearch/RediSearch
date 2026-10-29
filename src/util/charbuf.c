/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "util/charbuf.h"

#include <string.h>

#include "rmalloc.h"
#include "util/dict.h"

uint64_t CharBuf_HashFunction(const void *key) {
  const CharBuf *cb = key;
  return RS_dictGenHashFunction(cb->buf, cb->len);
}

int CharBuf_KeyCompare(void *privdata, const void *key1, const void *key2) {
  const CharBuf *cb1 = key1;
  const CharBuf *cb2 = key2;
  if (cb1->len != cb2->len) {
    return 0;
  }
  return (memcmp(cb1->buf, cb2->buf, cb1->len) == 0);
}

void *CharBuf_KeyDup(void *privdata, const void *key) {
  const CharBuf *cb = key;
  CharBuf *newcb = rm_malloc(sizeof(*newcb));
  newcb->len = cb->len;
  newcb->buf = rm_malloc(cb->len);
  memcpy(newcb->buf, cb->buf, cb->len);
  return newcb;
}

void CharBuf_KeyDestructor(void *privdata, void *key) {
  CharBuf *cb = key;
  rm_free(cb->buf);
  rm_free(cb);
}
