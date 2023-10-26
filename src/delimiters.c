/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdint.h>
#define __REDISEARCH_DELIMITERS_C__
#include "delimiters.h"
#include "rdb.h"
#include "rmalloc.h"

#define DELIMITERMAP_SIZE 256
#define MAX_DELIMITERSTRING_SIZE 64

// \t SPACE ! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ ` { | } ~
static const char __defaultDelimiterString[33] = "\t !\"#$%&\'()*+,-./:;<=>?@[]^`{|}~\0";
static const char __defaultDelimiterMap[DELIMITERMAP_SIZE] = {
    [' '] = 1, ['\t'] = 1, [','] = 1,  ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1, ['{'] = 1,
    ['}'] = 1, ['['] = 1,  [']'] = 1,  [':'] = 1, [';'] = 1, ['~'] = 1, ['!'] = 1, ['@'] = 1,
    ['#'] = 1, ['$'] = 1,  ['%'] = 1,  ['^'] = 1, ['&'] = 1, ['*'] = 1, ['-'] = 1, ['='] = 1,
    ['+'] = 1, ['|'] = 1,  ['\''] = 1, ['`'] = 1, ['"'] = 1, ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

static DelimiterList *__default_delimiters = NULL;

// generate the delimiter string that represents the delimiter map
// the string is allocated on the heap and must be freed by the caller
static char *_GenerateDelimiterString(const char delimiterMap[DELIMITERMAP_SIZE],
  size_t ndelimiters
) {
  assert(ndelimiters <= DELIMITERMAP_SIZE);
  char *delimiterString = rm_malloc(sizeof(char) * (ndelimiters + 1));
  int j = 0;
  for(uint16_t i = 0; i < DELIMITERMAP_SIZE; i++) {
    if(delimiterMap[i]) {
      delimiterString[j++] = (char)(i);
    }
  }
  assert(j == ndelimiters);
  delimiterString[j]='\0';
  return delimiterString;
}

DelimiterList *DefaultDelimiterList() {
  return __default_delimiters;
}

const char *DefaultDelimiterString() {
  return __defaultDelimiterString;
}

DelimiterList *_UpdateDelimiterListCStr(const char* str, DelimiterList* dl,
                                        char operation) {
  if(str == NULL) {
    return dl;
  }

  if(dl == NULL) {
    dl = NewDelimiterListCStr(__defaultDelimiterString);
  }

  char new_value = 0;
  char expected_value = 0;
  int delta_change = 0;

  // set the values according to operation
  // + = SET value in map
  // - = CLEAR value in map
  if(operation == '+') {
    expected_value = 0;
    new_value = 1;
    delta_change = 1;
  } else if (operation == '-') {
    expected_value = 1;
    new_value = 0;
    delta_change = -1;
  } else {
    return dl;
  }

  // update delimiter map
  size_t ndelimiters = strlen(str);
  int nchanges = 0;
  for(size_t i = 0; i < ndelimiters; i++) {
    uint8_t pos = (uint8_t)str[i];
    if(pos == '\\') {
      pos = (uint8_t)str[++i];
      // unescape tab character
      if(pos == 't') {
        pos = '\t';
      }
    }
    if(dl->delimiterMap[pos] == expected_value) {
      nchanges += delta_change;
      dl->delimiterMap[pos] = new_value;
    }
  }

  // update delimiter string
  ndelimiters = strlen(dl->delimiterString) + nchanges;
  rm_free(dl->delimiterString);
  dl->delimiterString = _GenerateDelimiterString(dl->delimiterMap, ndelimiters);

  return dl;
}

DelimiterList *AddDelimiterListCStr(const char* str, DelimiterList* dl) {
  return _UpdateDelimiterListCStr(str, dl, '+');
}

DelimiterList *RemoveDelimiterListCStr(const char* str, DelimiterList* dl) {
  return _UpdateDelimiterListCStr(str, dl, '-');
}

DelimiterList *NewDelimiterListCStr(const char* str) {
  if(str == NULL) {
    return NULL;
  }

  DelimiterList *dl = rm_malloc(sizeof(DelimiterList));

  // truncate input string if it is necessary
  size_t len = strlen(str);
  if (len > MAX_DELIMITERSTRING_SIZE) {
    len = MAX_DELIMITERSTRING_SIZE;
  }

  // initialize the delimiter map
  size_t ndelimiters = 0;
  memset(dl->delimiterMap, 0, sizeof(dl->delimiterMap));
  for(size_t i = 0; i < len; i++) {
    uint8_t pos = (uint8_t)str[i];
    if(pos == '\\') {
      pos = (uint8_t)str[++i];
      // unescape tab character
      if(pos == 't') {
        pos = '\t';
      }
    }
    if(dl->delimiterMap[pos] == 0) {
      ndelimiters++;
      dl->delimiterMap[pos] = 1;
    }
  }

  // initialize the delimiter string
  dl->delimiterString = _GenerateDelimiterString(dl->delimiterMap, ndelimiters);

  dl->refcount = 1;
  return dl;
}

static void DelimiterList_FreeInternal(DelimiterList *dl) {
  if(dl) {
    if(dl->delimiterString) {
        rm_free(dl->delimiterString);
      }
      rm_free(dl);
  }
}

void DelimiterList_Unref(DelimiterList *dl) {
  if (dl == __default_delimiters) {
    return;
  }

  if (__sync_sub_and_fetch(&dl->refcount, 1)) {
    return;
  }
  DelimiterList_FreeInternal(dl);
}

void DelimiterList_FreeGlobals(void) {
}

DelimiterList *DelimiterList_RdbLoad(RedisModuleIO* rdb) {
  DelimiterList *dl = NULL;
  size_t len;
  char *s = LoadStringBuffer_IOError(rdb, &len, goto cleanup);
  dl = NewDelimiterListCStr(s);
  if(s) {
    rm_free(s);
  }
  return dl;

cleanup:
  if(dl) {
    DelimiterList_FreeInternal(dl);
  }
  return DefaultDelimiterList();
}

void DelimiterList_RdbSave(RedisModuleIO* rdb, DelimiterList *dl) {
  if (dl != NULL && dl->delimiterString != NULL) {
    RedisModule_SaveStringBuffer(rdb, dl->delimiterString, strlen(dl->delimiterString) + 1);
  }
}

void DelimiterList_Ref(DelimiterList *dl) {
    __sync_fetch_and_add(&dl->refcount, 1);
}

void ReplyWithDelimiterList(RedisModule_Reply* reply, DelimiterList *dl) {
  RedisModule_Reply_SimpleString(reply, "delimiters");

  if (dl == NULL) {
    RedisModule_Reply_StringBuffer(reply, DefaultDelimiterString(), 
                                    strlen(DefaultDelimiterString()));
  } else {
    RedisModule_Reply_StringBuffer(reply, dl->delimiterString,
                                    strlen(dl->delimiterString));
  }
}

// TODO: How to test this function?
#ifdef FTINFO_FOR_INFO_MODULES
void AddDelimiterListToInfo(RedisModuleInfoCtx* ctx, DelimiterList *dl) {
  if (dl == NULL) {
    RedisModule_InfoAddFieldCString(ctx, "delimiters", DefaultDelimiterString());
  } else {
    RedisModule_InfoAddFieldCString(ctx, "delimiters", dl->delimiterString);
  }
}
#endif

/**
 * Function reads string pointed to by `s` and indicates the length of the next
 * token in `tokLen`. `s` is set to NULL if this is the last token.
 */
char *toksep(char **s, size_t *tokLen, const DelimiterList *dl) {
  char c;
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if(dl != NULL) {
      c = dl->delimiterMap[*pos];
    } else {
      c = __defaultDelimiterMap[*pos];
    }
    if (c && ((char *)pos == orig || *(pos - 1) != '\\')) {
      *s = (char *)++pos;
      *tokLen = ((char *)pos - orig) - 1;
      if (!*pos) {
        *s = NULL;
      }
      return orig;
    }
  }

  // Didn't find a terminating token. Use a simpler length calculation
  *s = NULL;
  *tokLen = (char *)pos - orig;
  return orig;
}

int istoksep(int c, const DelimiterList *dl) {
  if(dl != NULL) {
    return dl->delimiterMap [(uint8_t)c] != 0;
  } else {
    return __defaultDelimiterMap[(uint8_t)c] != 0;
  }
}
