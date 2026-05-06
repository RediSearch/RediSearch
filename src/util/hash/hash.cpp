/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "hash.h"
#include <boost/version.hpp>
#include <boost/uuid/detail/sha1.hpp>

void Sha1_Compute(const char *value, size_t len, Sha1* output) {
  boost::uuids::detail::sha1 sha1;
  sha1.process_bytes(value, len);
#if BOOST_VERSION >= 108600
  // Boost 1.86+: digest_type is unsigned char[20], stored as big-endian bytes.
  sha1.get_digest(output->hash);
#else
  // Boost < 1.86: digest_type is unsigned int[5] (host-endian words).
  // Convert each 32-bit word to big-endian bytes to match the layout
  // expected by Sha1_FormatIntoBuffer.
  boost::uuids::detail::sha1::digest_type digest;
  sha1.get_digest(digest);
  for (int i = 0; i < 5; i++) {
    output->hash[i*4]   = static_cast<unsigned char>((digest[i] >> 24) & 0xFF);
    output->hash[i*4+1] = static_cast<unsigned char>((digest[i] >> 16) & 0xFF);
    output->hash[i*4+2] = static_cast<unsigned char>((digest[i] >> 8)  & 0xFF);
    output->hash[i*4+3] = static_cast<unsigned char>( digest[i]        & 0xFF);
  }
#endif
}

void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer) {
  for (int i = 0; i < 5; i++) {
    uint32_t word = (sha1->hash[i*4] << 24) | (sha1->hash[i*4+1] << 16) |
                    (sha1->hash[i*4+2] << 8) | sha1->hash[i*4+3];
    sprintf(buffer + i * 8, "%08x", word);
  }
  buffer[40] = '\0';
}
