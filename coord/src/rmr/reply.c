#define __RMR_REPLY_C__
#include "reply.h"
#include "hiredis/hiredis.h"
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <redismodule.h>


int MRReply_StringEquals(MRReply *r, const char *s, int caseSensitive) {
  if (!r || MRReply_Type(r) != MR_REPLY_STRING) return 0;
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

  switch (MRReply_Type(r)) {
    case MR_REPLY_INTEGER:
      fprintf(fp, "INT(%lld)", MRReply_Integer(r));
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
      fprintf(fp, "ARR(%zd):[ ", MRReply_Length(r));
      for (size_t i = 0; i < MRReply_Length(r); i++) {
        MRReply_Print(fp, MRReply_ArrayElement(r, i));
        fprintf(fp, ", ");
      }
      fprintf(fp, "]");
      break;
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

int MR_ReplyWithMRReply(RedisModuleCtx *ctx, MRReply *rep) {
  if (rep == NULL) {
    return RedisModule_ReplyWithNull(ctx);
  }
  switch (MRReply_Type(rep)) {

    case MR_REPLY_STRING: {
      size_t len;
      char *str = MRReply_String(rep, &len);
      return RedisModule_ReplyWithStringBuffer(ctx, str, len);
    }

    case MR_REPLY_STATUS:
      return RedisModule_ReplyWithSimpleString(ctx, MRReply_String(rep, NULL));

    case MR_REPLY_ARRAY: {
      RedisModule_ReplyWithArray(ctx, MRReply_Length(rep));
      for (size_t i = 0; i < MRReply_Length(rep); i++) {
        MR_ReplyWithMRReply(ctx, MRReply_ArrayElement(rep, i));
      }
      return REDISMODULE_OK;
    }

    case MR_REPLY_INTEGER:
      return RedisModule_ReplyWithLongLong(ctx, MRReply_Integer(rep));

    case MR_REPLY_ERROR:
      return RedisModule_ReplyWithError(ctx, MRReply_String(rep, NULL));

    case MR_REPLY_NIL:
    default:
      return RedisModule_ReplyWithNull(ctx);
  }
  return REDISMODULE_ERR;
}
