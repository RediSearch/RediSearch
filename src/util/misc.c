/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "misc.h"
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include <unicode/uchar.h>
#include <unicode/ustring.h>
#include <unicode/utypes.h>

bool isAlphabetic(const char *str, size_t len) {
    UChar32 c;
    UErrorCode error = U_ZERO_ERROR;

    for (int32_t i = 0; i < len; ) {
        U8_NEXT(str, i, len, c);
        if (c < 0) {
            return false; // Invalid UTF-8 sequence
        }
        if (!u_isalpha(c)) {
            return false;
        }
    }
    return true;
}

void GenericAofRewrite_DisabledHandler(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  RedisModule_Log(RedisModule_GetContextFromIO(aof), "error",
                  "Requested AOF, but this is unsupported for this module");
  abort();
}

char *strtolower(char *str) {
  char *p = str;
  while (*p) {
    *p = tolower(*p);
    p++;
  }
  return str;
}

int GetRedisErrorCodeLength(const char* error) {
  const char* errorSpace = strchr(error, ' ');
  return errorSpace ? errorSpace - error : 0;
}