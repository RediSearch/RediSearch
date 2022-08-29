
#pragma once

#include <stddef.h>
#include <string_view>

#define PHONETIC_PREFIX '<'

struct PhoneticManager {
  char *algorithm;
  //RSLanguage language; // not currently used

  static void ExpandPhonetics(std::string_view term, char** primary, char** secondary);
  static void AddPrefix(char** phoneticTerm);
};
