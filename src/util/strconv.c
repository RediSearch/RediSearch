/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "strconv.h"

char *unicode_tolower_fn(char *encoded, size_t *inout_len) {
  return unicode_tolower(encoded, inout_len);
}

const char *nu_utf8_read_fn(const char *utf8, uint32_t *unicode) {
  return nu_utf8_read(utf8, unicode);
}

char *nu_utf8_write_fn(uint32_t unicode, char *utf8) {
  return nu_utf8_write(unicode, utf8);
}
