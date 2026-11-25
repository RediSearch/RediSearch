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
#include <stdint.h>
#include <stdio.h>
#include <cstring>

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

struct Sha1Context {
  boost::uuids::detail::sha1 state;
};

uint64_t Sha1_ComputeValue(void (*visit)(Sha1Context *, const void *), const void *value) {
  Sha1Context ctx;
  visit(&ctx, value);
  boost::uuids::detail::sha1::digest_type digest;
  ctx.state.get_digest(digest);
#if BOOST_VERSION >= 108600
  uint64_t result = 0;
  for (size_t i = 0; i < sizeof(result); ++i) {
    result = (result << 8) | digest[i];
  }
  return result;
#else
  return (uint64_t{digest[0]} << 32) | digest[1];
#endif
}

void Sha1_UpdateU64(Sha1Context *ctx, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    ctx->state.process_byte(static_cast<unsigned char>(value >> shift));
  }
}

void Sha1_UpdateDouble(Sha1Context *ctx, double value) {
  static_assert(sizeof(double) == sizeof(uint64_t));
  uint64_t bits = 0;
  // Numerically equal signed zeroes must not indicate differing schemas.
  if (value != 0) {
    std::memcpy(&bits, &value, sizeof(bits));
  }
  Sha1_UpdateU64(ctx, bits);
}

void Sha1_UpdateBuffer(Sha1Context *ctx, const char *value, size_t len) {
  Sha1_UpdateU64(ctx, len);
  if (len) {
    ctx->state.process_bytes(value, len);
  }
}

void Sha1_UpdateCString(Sha1Context *ctx, const char *value) {
  ctx->state.process_byte(value != nullptr);
  if (value) {
    do {
      ctx->state.process_byte(static_cast<unsigned char>(*value));
    } while (*value++);
  }
}
