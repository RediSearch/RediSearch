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
  boost::uuids::detail::sha1::digest_type digest;
  sha1.get_digest(digest);

  // Convert from digest_type (unsigned int[5]) to unsigned char[20]
  for (int i = 0; i < 5; i++) {
    output->hash[i*4] = (digest[i] >> 24) & 0xFF;
    output->hash[i*4+1] = (digest[i] >> 16) & 0xFF;
    output->hash[i*4+2] = (digest[i] >> 8) & 0xFF;
    output->hash[i*4+3] = digest[i] & 0xFF;
  }
}

void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer) {
  for (int i = 0; i < 5; i++) {
    uint32_t word = (sha1->hash[i*4] << 24) | (sha1->hash[i*4+1] << 16) |
                    (sha1->hash[i*4+2] << 8) | sha1->hash[i*4+3];
    snprintf(buffer + i * 8, 9, "%08x", word);
  }
  buffer[40] = '\0';
}
