/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define __RMR_REPLY_C__
#include "reply.h"

#include "redismodule.h"
#include "hiredis/hiredis.h"

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

void MRReply_Print(FILE *fp, MRReply *r) {
  if (!r) {
    fprintf(fp, "NULL");
    return;
  }

  size_t len;
  switch (MRReply_Type(r)) {
    case MR_REPLY_INTEGER:
      fprintf(fp, "INT(%lld)", MRReply_Integer(r));
      break;

    case MR_REPLY_DOUBLE:
      fprintf(fp, "DOUBLE(%f)", MRReply_Double(r));
      break;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
      fprintf(fp, "STR(%s)", MRReply_String(r, NULL));
      break;

    case MR_REPLY_ERROR:
      fprintf(fp, "ERR(%s)", MRReply_String(r, NULL));
      break;

    case MR_REPLY_NIL:
      fprintf(fp, "(nil)");
      break;

    case MR_REPLY_ARRAY:
      len = MRReply_Length(r);
      fprintf(fp, "ARR(%zd):[ ", len);
      for (size_t i = 0; i < len; i++) {
        MRReply_Print(fp, MRReply_ArrayElement(r, i));
        fprintf(fp, ", ");
      }
      fprintf(fp, "]");
      break;

    case MR_REPLY_MAP:
      len = MRReply_Length(r);
      fprintf(fp, "MAP(%zd):{ ", len);
      for (size_t i = 0; i < len; i++) {
        MRReply_Print(fp, MRReply_ArrayElement(r, i++));
        fprintf(fp, ": ");
        if (i < len) {
          MRReply_Print(fp, MRReply_ArrayElement(r, i));
          fprintf(fp, ", ");
        } else {
          fprintf(fp, "(none), ");
        }
      }
      fprintf(fp, "}");
      break;

    default:
      break;
  }
}

void MRReply_Print_1(FILE *fp, MRReply *r) {
  if (!r) {
    fprintf(fp, "NULL");
    return;
  }

  size_t len;
  switch (MRReply_Type(r)) {
    case MR_REPLY_INTEGER:
      fprintf(fp, "%lld", MRReply_Integer(r));
      break;

    case MR_REPLY_DOUBLE:
      fprintf(fp, "%f", MRReply_Double(r));
      break;

    case MR_REPLY_STRING:
    case MR_REPLY_STATUS:
      fprintf(fp, "'%s'", MRReply_String(r, NULL));
      break;

    case MR_REPLY_ERROR:
      fprintf(fp, "ERR(%s)", MRReply_String(r, NULL));
      break;

    case MR_REPLY_NIL:
      fprintf(fp, "(nil)");
      break;

    case MR_REPLY_ARRAY:
      len = MRReply_Length(r);
      fprintf(fp, "[ ");
      for (size_t i = 0; i < len; i++) {
        MRReply_Print_1(fp, MRReply_ArrayElement(r, i));
        fprintf(fp, ", ");
      }
      fprintf(fp, " ]");
      break;

    case MR_REPLY_MAP:
      len = MRReply_Length(r);
      fprintf(fp, "{ ");
      for (size_t i = 0; i < len; i++) {
        MRReply_Print_1(fp, MRReply_ArrayElement(r, i++));
        fprintf(fp, ": ");
        if (i < len) {
          MRReply_Print_1(fp, MRReply_ArrayElement(r, i));
          fprintf(fp, ", ");
        } else {
          fprintf(fp, "(none), ");
        }
      }
      fprintf(fp, "}");
      break;

    default:
      break;
  }
}

#if DEBUG

void print_mr_reply(MRReply *r) {
  MRReply_Print_1(stderr, r);
  puts("");
}

#endif // DEBUG

int _parseInt(const char *str, size_t len, long long *i) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = (char *)str + len;
  long long int val = strtoll(str, &endptr, 10);

  if ((errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) || (errno != 0 && val == 0)) {
    perror("strtol");
    return 0;
  }

  if (endptr == str) {
    //  fprintf(stderr, "No digits were found\n");
    return 0;
  }

  *i = val;
  return 1;
}

int _parseFloat(const char *str, size_t len, double *d) {
  errno = 0; /* To distinguish success/failure after call */
  char *endptr = (char *)str + len;
  double val = strtod(str, &endptr);

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
      char *str = MRReply_String(rep, &len);
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
  // TODO: check out of bounds
  return reply->element[idx];
}

inline MRReply *MRReply_MapElement(const MRReply *reply, const char *key) {
  if (reply->type != MR_REPLY_MAP) return NULL;
  for (int i = 0; i < reply->elements; i += 2) {
    if (MRReply_StringEquals(reply->element[i], key, false)) {
      ++i;
      return i < reply->elements ? reply->element[i] : NULL;
    }
  }
  return NULL;
}
