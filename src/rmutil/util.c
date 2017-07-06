#include <stdlib.h>
#include <errno.h>
#include <math.h>
#include <ctype.h>
#include <sys/time.h>
#include <stdarg.h>
#include <limits.h>
#include <string.h>
#include <strings.h>
#include <redismodule.h>
#include "util.h"

/**
Check if an argument exists in an argument list (argv,argc), starting at offset.
@return 0 if it doesn't exist, otherwise the offset it exists in
*/
int RMUtil_ArgExists(const char *arg, RedisModuleString **argv, int argc, int offset) {

  size_t larg = strlen(arg);
  for (; offset < argc; offset++) {
    size_t l;
    const char *carg = RedisModule_StringPtrLen(argv[offset], &l);
    if (l != larg) continue;
    if (carg != NULL && strncasecmp(carg, arg, larg) == 0) {
      return offset;
    }
  }
  return 0;
}

/**
Check if an argument exists in an argument list (argv,argc)
@return -1 if it doesn't exist, otherwise the offset it exists in
*/
int RMUtil_ArgIndex(const char *arg, RedisModuleString **argv, int argc) {

  size_t larg = strlen(arg);
  for (int offset = 0; offset < argc; offset++) {
    size_t l;
    const char *carg = RedisModule_StringPtrLen(argv[offset], &l);
    if (l != larg) continue;
    if (carg != NULL && strncasecmp(carg, arg, larg) == 0) {
      return offset;
    } \
  }
  return -1;
}

RMUtilInfo *RMUtil_GetRedisInfo(RedisModuleCtx *ctx) {

  RedisModuleCallReply *r = RedisModule_Call(ctx, "INFO", "c", "all");
  if (r == NULL || RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR) {
    return NULL;
  }

  int cap = 100;  // rough estimate of info lines
  RMUtilInfo *info = malloc(sizeof(RMUtilInfo));
  info->entries = calloc(cap, sizeof(RMUtilInfoEntry));

  int i = 0;
  char *text = (char *)RedisModule_StringPtrLen(RedisModule_CreateStringFromCallReply(r), NULL);
  char *line = text;
  while (line) {
    char *line = strsep(&text, "\r\n");
    if (line == NULL) break;

    if (!(*line >= 'a' && *line <= 'z')) {  // skip non entry lines
      continue;
    }

    char *key = strsep(&line, ":");
    info->entries[i].key = key;
    info->entries[i].val = line;
    i++;
    if (i >= cap) {
      cap *= 2;
      info->entries = realloc(info->entries, cap * sizeof(RMUtilInfoEntry));
    }
  }
  info->numEntries = i;

  return info;
}
void RMUtilRedisInfo_Free(RMUtilInfo *info) {

  free(info->entries);
  free(info);
}

int RMUtilInfo_GetInt(RMUtilInfo *info, const char *key, long long *val) {

  const char *p = NULL;
  if (!RMUtilInfo_GetString(info, key, &p)) {
    return 0;
  }

  *val = strtoll(p, NULL, 10);
  if ((errno == ERANGE && (*val == LONG_MAX || *val == LONG_MIN)) || (errno != 0 && *val == 0)) {
    *val = -1;
    return 0;
  }

  return 1;
}

int RMUtilInfo_GetString(RMUtilInfo *info, const char *key, const char **str) {
  int i;
  for (i = 0; i < info->numEntries; i++) {
    if (!strcmp(key, info->entries[i].key)) {
      *str = info->entries[i].val;
      return 1;
    }
  }
  return 0;
}

int RMUtilInfo_GetDouble(RMUtilInfo *info, const char *key, double *d) {
  const char *p = NULL;
  if (!RMUtilInfo_GetString(info, key, &p)) {
    printf("not found %s\n", key);
    return 0;
  }

  *d = strtod(p, NULL);
  if ((errno == ERANGE && (*d == HUGE_VAL || *d == -HUGE_VAL)) || (errno != 0 && *d == 0)) {
    return 0;
  }

  return 1;
}

/*
c -- pointer to a Null terminated C string pointer.
b -- pointer to a C buffer, followed by pointer to a size_t for its length
s -- pointer to a RedisModuleString
l -- pointer to Long long integer.
d -- pointer to a Double
* -- do not parse this argument at all
*/
int RMUtil_ParseArgs(RedisModuleString **argv, int argc, int offset, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int rc = rmutil_vparseArgs(argv, argc, offset, fmt, ap);
  va_end(ap);
  return rc;
}

// Internal function that parses arguments based on the format described above
int rmutil_vparseArgs(RedisModuleString **argv, int argc, int offset, const char *fmt, va_list ap) {

  int i = offset;
  char *c = (char *)fmt;
  while (*c && i < argc) {

    // read c string
    if (*c == 'c') {
      char **p = va_arg(ap, char **);
      *p = (char *)RedisModule_StringPtrLen(argv[i], NULL);
    } else if (*c == 'b') {
      char **p = va_arg(ap, char **);
      size_t *len = va_arg(ap, size_t *);
      *p = (char *)RedisModule_StringPtrLen(argv[i], len);
    } else if (*c == 's') {  // read redis string

      RedisModuleString **s = va_arg(ap, void *);
      *s = argv[i];

    } else if (*c == 'l') {  // read long
      long long *l = va_arg(ap, long long *);

      if (RedisModule_StringToLongLong(argv[i], l) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (*c == 'd') {  // read double
      double *d = va_arg(ap, double *);
      if (RedisModule_StringToDouble(argv[i], d) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
      }
    } else if (*c == '*') {  // skip current arg
      // do nothing
    } else {
      return REDISMODULE_ERR;  // WAT?
    }
    c++;
    i++;
  }
  // if the format is longer than argc, retun an error
  if (*c != 0) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

int RMUtil_ParseArgsAfter(const char *token, RedisModuleString **argv, int argc, const char *fmt,
                          ...) {

  int pos = RMUtil_ArgIndex(token, argv, argc);
  if (pos < 0) {
    return REDISMODULE_ERR;
  }

  va_list ap;
  va_start(ap, fmt);
  int rc = rmutil_vparseArgs(argv, argc, pos + 1, fmt, ap);
  va_end(ap);
  return rc;
}

RedisModuleCallReply *RedisModule_CallReplyArrayElementByPath(RedisModuleCallReply *rep,
                                                              const char *path) {
  if (rep == NULL) return NULL;

  RedisModuleCallReply *ele = rep;
  const char *s = path;
  char *e;
  long idx;
  do {
    errno = 0;
    idx = strtol(s, &e, 10);

    if ((errno == ERANGE && (idx == LONG_MAX || idx == LONG_MIN)) || (errno != 0 && idx == 0) ||
        (REDISMODULE_REPLY_ARRAY != RedisModule_CallReplyType(ele)) || (s == e)) {
      ele = NULL;
      break;
    }
    s = e;
    ele = RedisModule_CallReplyArrayElement(ele, idx - 1);

  } while ((ele != NULL) && (*e != '\0'));

  return ele;
}

int RedisModule_TryGetValue(RedisModuleKey *key, const RedisModuleType *type, void **out) {
  if (key == NULL) {
    return RMUTIL_VALUE_MISSING;
  }
  int keytype = RedisModule_KeyType(key);
  if (keytype == REDISMODULE_KEYTYPE_EMPTY) {
    return RMUTIL_VALUE_EMPTY;
  } else if (keytype == REDISMODULE_KEYTYPE_MODULE && RedisModule_ModuleTypeGetType(key) == type) {
    *out = RedisModule_ModuleTypeGetValue(key);
    return RMUTIL_VALUE_OK;
  } else {
    return RMUTIL_VALUE_MISMATCH;
  }
}

int RedisModule_Strncasecmp(const RedisModuleString *rs1, const char *s2, size_t n) {
  size_t n2;
  const char *s1 = RedisModule_StringPtrLen(rs1, &n2);
  if (n != n2) {
    return -1;
  }
  return strncasecmp(s1, s2, n);
}

int RedisModule_Strcasecmp(const RedisModuleString *s1, const char *s2) {
  return RedisModule_Strncasecmp(s1, s2, strlen(s2));
}