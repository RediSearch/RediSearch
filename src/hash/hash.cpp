#include "hash.h"
#include <boost/uuid/detail/sha1.hpp>

void Sha1_Compute(Sha1* output, const char *value, size_t len) {
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

char *Sha1_Format(const Sha1* sha1) {
  char* buffer = (char*)malloc(41);
  Sha1_FormatIntoBuffer(sha1, buffer);
  return buffer;
}