#include "hash.h"
#include <cstdio>
#include <boost/uuid/detail/sha1.hpp>

void Sha1_Compute(const char *value, size_t len, Sha1* output) {
  boost::uuids::detail::sha1 sha1;
  sha1.process_bytes(value, len);
  sha1.get_digest(output->hash);
}

void Sha1_FormatIntoBuffer(const Sha1 *sha1, char *buffer) {
  for (int i = 0; i < 20; i++) {
    snprintf(buffer + i * 2, 3, "%02x", sha1->hash[i]);
  }
  buffer[40] = '\0';
}
