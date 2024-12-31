/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "misc.h"
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

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

bool contains_non_alphabetic_char(char* str, size_t len) {
    if (len == 0 || !str ) return false;
    const char* non_alphabetic_chars = "0123456789!@#$%^&*()_+-=[]{}\\|;:'\",.<>/?`~§± ";
    for (size_t i = 0; i < len; i++) {
        if (strchr(non_alphabetic_chars, str[i])) {
            return true;
        }
    }
    return false;
}