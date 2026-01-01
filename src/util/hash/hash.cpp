/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "hash.h"
#include <boost/uuid/detail/sha1.hpp>

void Sha1_Compute(const char *value, size_t len, Sha1* output) {
  boost::uuids::detail::sha1 sha1;
  sha1.process_bytes(value, len);
  sha1.get_digest(output->hash);
}

void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer) {
  for (int i = 0; i < 5; i++) {
    sprintf(buffer + i * 8, "%08x", sha1->hash[i]);
  }
  buffer[40] = '\0';
}
