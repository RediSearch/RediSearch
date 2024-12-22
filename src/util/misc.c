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
// #include <unicode/uchar.h>
// #include <unicode/ustring.h>
// #include <unicode/utypes.h>

// bool isAlphabetic(const char *str, size_t len) {
//     UChar32 c;
//     UErrorCode error = U_ZERO_ERROR;

//     for (int32_t i = 0; i < len; ) {
//         U8_NEXT(str, i, len, c);
//         if (c < 0) {
//             return false; // Invalid UTF-8 sequence
//         }
//         if (!u_isalpha(c)) {
//             return false;
//         }
//     }
//     return true;
// }


bool isAlphabetic(const char *str, size_t len) {
    for (size_t i = 0; i < len;) {
        unsigned char c = (unsigned char)str[i];
        int bytes;
        uint32_t code_point;

        // UTF-8 Decoding with validation
        if (c <= 0x7F) {
            code_point = c;
            bytes = 1;
        } else if ((c & 0xE0) == 0xC0) {
            if (i + 1 >= len || (str[i + 1] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x1F) << 6) | (str[i + 1] & 0x3F);
            bytes = 2;
        } else if ((c & 0xF0) == 0xE0) {
            if (i + 2 >= len || (str[i + 1] & 0xC0) != 0x80 || (str[i + 2] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x0F) << 12) | ((str[i + 1] & 0x3F) << 6) | (str[i + 2] & 0x3F);
            bytes = 3;
        } else if ((c & 0xF8) == 0xF0) {
            if (i + 3 >= len || (str[i + 1] & 0xC0) != 0x80 || (str[i + 2] & 0xC0) != 0x80 || (str[i + 3] & 0xC0) != 0x80) return false;
            code_point = ((c & 0x07) << 18) | ((str[i + 1] & 0x3F) << 12) | ((str[i + 2] & 0x3F) << 6) | (str[i + 3] & 0x3F);
            bytes = 4;
        } else {
            return false;
        }

        // Check if code point is in any letter range
        bool isLetter = 
            // Latin scripts
            (code_point >= 0x0041 && code_point <= 0x005A) ||  // Basic Latin (A-Z)
            (code_point >= 0x0061 && code_point <= 0x007A) ||  // Basic Latin (a-z)
            (code_point >= 0x00C0 && code_point <= 0x00FF) ||  // Latin-1 Supplement
            (code_point >= 0x0100 && code_point <= 0x017F) ||  // Latin Extended-A
            (code_point >= 0x0180 && code_point <= 0x024F) ||  // Latin Extended-B
            (code_point >= 0x1E00 && code_point <= 0x1EFF) ||  // Latin Extended Additional
            // Other scripts
            (code_point >= 0x0370 && code_point <= 0x03FF) ||  // Greek and Coptic
            (code_point >= 0x0400 && code_point <= 0x04FF) ||  // Cyrillic
            (code_point >= 0x0500 && code_point <= 0x052F) ||  // Cyrillic Supplement
            (code_point >= 0x0531 && code_point <= 0x0556) ||  // Armenian
            (code_point >= 0x0561 && code_point <= 0x0587) ||  // Armenian
            // CJK scripts
            (code_point >= 0x3040 && code_point <= 0x309F) ||  // Hiragana
            (code_point >= 0x30A0 && code_point <= 0x30FF) ||  // Katakana
            (code_point >= 0x3400 && code_point <= 0x4DBF) ||  // CJK Extension A
            (code_point >= 0x4E00 && code_point <= 0x9FFF) ||  // CJK Unified Ideographs
            (code_point >= 0xF900 && code_point <= 0xFAFF) ||  // CJK Compatibility Ideographs
            (code_point >= 0x20000 && code_point <= 0x2A6DF) || // CJK Extension B
            (code_point >= 0x2A700 && code_point <= 0x2B73F) || // CJK Extension C
            (code_point >= 0x2B740 && code_point <= 0x2B81F) || // CJK Extension D
            (code_point >= 0x2B820 && code_point <= 0x2CEAF) || // CJK Extension E
            (code_point >= 0x2CEB0 && code_point <= 0x2EBEF);   // CJK Extension F

        if (!isLetter) {
            return false;
        }
        i += bytes;
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