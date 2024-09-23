#include "hidden.h"
#include "rmalloc.h"
#include "obfuscation_api.h"

typedef struct {
  const char* user;
  uint64_t length;
  char* obfuscated;
} UserAndObfuscatedString;

typedef struct {
  uint64_t user;
  uint64_t obfuscated;
} UserAndObfuscatedUInt64;

typedef struct {
  const char *user;
  uint64_t length;
} UserString;

HiddenString* NewHiddenString(const char* str, uint64_t length) {
  UserAndObfuscatedString* value = rm_malloc(sizeof(*value));
  value->user = str;
  value->length = length;
  value->obfuscated = Obfuscate_Text(str);
  return (HiddenString*)value;
}

HiddenSize* NewHiddenSize(uint64_t num) {
  UserAndObfuscatedUInt64* value = rm_malloc(sizeof(*value));
  value->user = num;
  value->obfuscated = 0;
  return (HiddenSize*)value;
};

HiddenName* NewHiddenName(const char* name, uint64_t length) {
  UserString* value = rm_malloc(sizeof(*value));
  value->user = name;
  value->length = length;
  return (HiddenName*)value;
};

void FreeHiddenString(HiddenString* hs) {
  UserAndObfuscatedString* value = (UserAndObfuscatedString*)hs;
  rm_free(value);
};

void FreeHiddenSize(HiddenSize* hn) {
  UserAndObfuscatedUInt64* value = (UserAndObfuscatedUInt64*)hn;
  rm_free(value);
};

void FreeHiddenName(HiddenName* hn) {
  UserAndObfuscatedUInt64* value = (UserAndObfuscatedUInt64*)hn;
  rm_free(value);
};