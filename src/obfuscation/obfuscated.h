#ifndef OBFUSCATED_H
#define OBFUSCATED_H
#include <stdint.h>
#include "obfuscation_api.h"

typedef struct ObfuscatedSize ObfuscatedSize;
typedef struct ObfuscatedString ObfuscatedString;

// Hides the string and obfuscates it
ObfuscatedString *ObfuscateString(const char *str, uint64_t length, bool takeOwnership);
// Hides the size and obfuscates it
ObfuscatedSize *ObfuscateNumber(uint64_t num);

void ObfuscatedString_Free(ObfuscatedString *value, bool tookOwnership);
void ObfuscatedSize_Free(ObfuscatedSize *value);

ObfuscatedString *ObfuscatedString_Clone(const ObfuscatedString* value);
const char *ObfuscatedString_Get(const ObfuscatedString *value, bool obfuscate);

#endif // OBFUSCATED_H