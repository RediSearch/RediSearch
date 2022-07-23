
#pragma once

#include <stddef.h>

#define PHONETIC_PREFIX '<'

struct PhoneticManager {
  char* algorithm;
  //RSLanguage language; // not currently used

  static void ExpandPhonetics(const char* term, size_t len, char** primary, char** secondary);
};
