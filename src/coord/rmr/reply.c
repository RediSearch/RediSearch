/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define __RMR_REPLY_C__
#include "reply.h"

#include "redismodule.h"
#include "hiredis/hiredis.h"
#include "fast_float/fast_float_strtod.h"

#include <string.h>
#include <errno.h>
#include <limits.h>


int MRReply_StringEquals(MRReply *r, const char *s, int caseSensitive) {
  if (!r || MRReply_Type(r) != MR_REPLY_STRING && MRReply_Type(r) != MR_REPLY_STATUS) return 0;
  size_t len;
  const char *rs = MRReply_String(r, &len);
  size_t slen = strlen(s);
  if (len != slen) return 0;
  if (caseSensitive) {
    return !strncmp(s, rs, slen);
  } else {
    return !strncasecmp(s, rs, slen);
  }
}

int _parseInt(const char *str, size_t len, long long *i) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = (char *)str + len;
  long long int val = strtoll(str, &endptr, 10);

  if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0)) {
    perror("strtol");
    return 0;
  }

  if (endptr == str) {
    return 0;
  }

  *i = val;
  return 1;
}

int _parseFloat(const char *str, size_t len, double *d) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = (char *)str + len;
  double val = fast_float_strtod(str, &endptr);

  /* Check for various possible errors */
  if (errno != 0 || (endptr == str && val == 0)) {
    return 0;
  }
  *d = val;
  return 1;
}

int MRReply_ToInteger(MRReply *reply, long long *i) {
  if (reply == NULL) return 0;

  switch (MRReply_Type(reply)) {
    case MR_REPLY_INTEGER:
      *i = MRReply_Integer(reply);
      return 1;

    case MR_REPLY_DOUBLE:
      *i = (int) MRReply_Double(reply);
      return 1;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS: {
      size_t n;
      const char *s = MRReply_String(reply, &n);
      return _parseInt(s, n, i);
    }

    default:
      return 0;
  }
}

int MRReply_ToDouble(MRReply *reply, double *d) {
  if (reply == NULL) return 0;

  switch (MRReply_Type(reply)) {
    case MR_REPLY_INTEGER:
      *d = (double)MRReply_Integer(reply);
      return 1;

    case MR_REPLY_DOUBLE:
      *d = MRReply_Double(reply);
      return 1;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
    case MR_REPLY_ERROR: {
      size_t n;
      const char *s = MRReply_String(reply, &n);
      return _parseFloat(s, n, d);
    }

    default:
      return 0;
  }
}

int MR_ReplyWithMRReply(RedisModule_Reply *reply, MRReply *rep) {
  if (rep == NULL) {
    return RedisModule_Reply_Null(reply);
  }

  switch (MRReply_Type(rep)) {
    case MR_REPLY_STRING: {
      size_t len;
      const char *str = MRReply_String(rep, &len);
      return RedisModule_Reply_StringBuffer(reply, str, len);
    }

    case MR_REPLY_STATUS:
      return RedisModule_Reply_SimpleString(reply, MRReply_String(rep, NULL));

    case MR_REPLY_MAP: {
      RedisModule_Reply_Map(reply);
      size_t len = MRReply_Length(rep);
      for (size_t i = 0; i < len; i++) {
        MR_ReplyWithMRReply(reply, MRReply_ArrayElement(rep, i));
      }
      RedisModule_Reply_MapEnd(reply);
      return REDISMODULE_OK;
    }

    case MR_REPLY_SET: {
      RedisModule_Reply_Set(reply);
      size_t len = MRReply_Length(rep);
      for (size_t i = 0; i < len; i++) {
        MR_ReplyWithMRReply(reply, MRReply_ArrayElement(rep, i));
      }
      RedisModule_Reply_SetEnd(reply);
      return REDISMODULE_OK;
    }

    case MR_REPLY_ARRAY: {
      RedisModule_Reply_Array(reply);
      size_t len = MRReply_Length(rep);
      for (size_t i = 0; i < len; i++) {
        MR_ReplyWithMRReply(reply, MRReply_ArrayElement(rep, i));
      }
      RedisModule_Reply_ArrayEnd(reply);
      return REDISMODULE_OK;
    }

    case MR_REPLY_INTEGER:
    case MR_REPLY_BOOL:
      return RedisModule_Reply_LongLong(reply, MRReply_Integer(rep));

    case MR_REPLY_ERROR:
      return RedisModule_Reply_Error(reply, MRReply_String(rep, NULL));

    case MR_REPLY_DOUBLE:
      return RedisModule_Reply_Double(reply, MRReply_Double(rep));

    case MR_REPLY_ATTR:
    case MR_REPLY_PUSH:
    case MR_REPLY_BIGNUM:
      return REDISMODULE_ERR;

    case MR_REPLY_NIL:
    default:
      return RedisModule_Reply_Null(reply);
  }
  return REDISMODULE_ERR;
}

int RedisModule_ReplyKV_MRReply(RedisModule_Reply *reply, const char *key, MRReply *rep) {
  RedisModule_Reply_SimpleString(reply, key);
  MR_ReplyWithMRReply(reply, rep);
  return REDISMODULE_OK;
}


inline void MRReply_Free(MRReply *reply) {
  freeReplyObject(reply);
}


inline int MRReply_Type(const MRReply *reply) {
  return reply->type;
}


inline long long MRReply_Integer(const MRReply *reply) {
  return reply->integer;
}


inline double MRReply_Double(const MRReply *reply) {
  return reply->dval;
}


inline size_t MRReply_Length(const MRReply *reply) {
  return reply ? reply->elements : 0;
}


inline const char *MRReply_String(const MRReply *reply, size_t *len) {
  if (len) {
    *len = reply->len;
  }
  return reply->str;
}

inline MRReply *MRReply_ArrayElement(const MRReply *reply, size_t idx) {
  RS_ASSERT(reply->elements > idx);
  return reply->element[idx];
}

inline MRReply *MRReply_TakeArrayElement(const MRReply *reply, size_t idx) {
  RS_ASSERT(reply->elements > idx);
  MRReply *ret = reply->element[idx];
  reply->element[idx] = NULL; // Take ownership
  return ret;
}

static inline int MRReply_FindMapElement(const MRReply *reply, const char *key) {
  if (reply->type != MR_REPLY_MAP) return -1;
  for (int i = 0; i < reply->elements - 1; i += 2) {
    if (MRReply_StringEquals(reply->element[i], key, false)) {
      return i + 1; // Return the index of the value
    }
  }
  return -1; // Not found
}

inline MRReply *MRReply_MapElement(const MRReply *reply, const char *key) {
  int idx = MRReply_FindMapElement(reply, key);
  return idx >= 0 ? reply->element[idx] : NULL;
}

inline MRReply *MRReply_TakeMapElement(const MRReply *reply, const char *key) {
  int idx = MRReply_FindMapElement(reply, key);
  if (idx < 0) return NULL; // Not found
  return MRReply_TakeArrayElement(reply, idx); // Take ownership of the value
}


void MRReply_ArrayToMap(MRReply *reply) {
  if (reply->type != MR_REPLY_ARRAY) return;
  reply->type = MR_REPLY_MAP;
}

void printMRReplyRecursive(MRReply *reply, int depth) {
  if (!reply) {
      RedisModule_Log(NULL, "warning", "%*sNULL", depth * 2, "");
      return;
  }

  const char *indent = "";
  for (int i = 0; i < depth; i++) {
      indent = "  ";
  }

  switch (MRReply_Type(reply)) {
      case MR_REPLY_STRING: {
          size_t len;
          const char *str = MRReply_String(reply, &len);
          RedisModule_Log(NULL, "warning", "%*sSTRING: %.*s", depth * 2, "", (int)len, str);
          break;
      }
      case MR_REPLY_STATUS: {
          const char *str = MRReply_String(reply, NULL);
          RedisModule_Log(NULL, "warning", "%*sSTATUS: %s", depth * 2, "", str);
          break;
      }
      case MR_REPLY_INTEGER:
          RedisModule_Log(NULL, "warning", "%*sINTEGER: %lld", depth * 2, "", MRReply_Integer(reply));
          break;
      case MR_REPLY_DOUBLE:
          RedisModule_Log(NULL, "warning", "%*sDOUBLE: %f", depth * 2, "", MRReply_Double(reply));
          break;
      case MR_REPLY_BOOL:
          RedisModule_Log(NULL, "warning", "%*sBOOL: %s", depth * 2, "", MRReply_Integer(reply) ? "true" : "false");
          break;
      case MR_REPLY_ERROR: {
          const char *str = MRReply_String(reply, NULL);
          RedisModule_Log(NULL, "warning", "%*sERROR: %s", depth * 2, "", str);
          break;
      }
      case MR_REPLY_NIL:
          RedisModule_Log(NULL, "warning", "%*sNIL", depth * 2, "");
          break;
      case MR_REPLY_ARRAY: {
          size_t len = MRReply_Length(reply);
          RedisModule_Log(NULL, "warning", "%*sARRAY[%zu]:", depth * 2, "", len);
          for (size_t i = 0; i < len; i++) {
              RedisModule_Log(NULL, "warning", "%*s[%zu]:", depth * 2, "", i);
              printMRReplyRecursive(MRReply_ArrayElement(reply, i), depth + 1);
          }
          break;
      }
      case MR_REPLY_MAP: {
          size_t len = MRReply_Length(reply);
          RedisModule_Log(NULL, "warning", "%*sMAP[%zu]:", depth * 2, "", len);
          for (size_t i = 0; i < len; i += 2) {
              size_t key_len;
              const char *key = MRReply_String(MRReply_ArrayElement(reply, i), &key_len);
              RedisModule_Log(NULL, "warning", "%*s\"%.*s\":", depth * 2, "", (int)key_len, key);
              printMRReplyRecursive(MRReply_ArrayElement(reply, i + 1), depth + 1);
          }
          break;
      }
      case MR_REPLY_SET: {
          size_t len = MRReply_Length(reply);
          RedisModule_Log(NULL, "warning", "%*sSET[%zu]:", depth * 2, "", len);
          for (size_t i = 0; i < len; i++) {
              RedisModule_Log(NULL, "warning", "%*s[%zu]:", depth * 2, "", i);
              printMRReplyRecursive(MRReply_ArrayElement(reply, i), depth + 1);
          }
          break;
      }
      default:
          RedisModule_Log(NULL, "warning", "%*sUNKNOWN_TYPE: %d", depth * 2, "", MRReply_Type(reply));
          break;
  }
}
