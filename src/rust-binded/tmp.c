#include "tmp.h"

#include "value.h"
#include "libnu/libnu.h"

/* Load a sorting vector from RDB */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb) {

    int len = (int)RedisModule_LoadUnsigned(rdb);
    if (len > RS_SORTABLES_MAX || len <= 0) {
      return NULL;
    }
    RSSortingVector *vec = NewSortingVector(len);
    for (int i = 0; i < len; i++) {
      RSValueType t = RedisModule_LoadUnsigned(rdb);
  
      switch (t) {
        case RSValue_String: {
          size_t len;
          // strings include an extra character for null terminator. we set it to zero just in case
          char *s = RedisModule_LoadStringBuffer(rdb, &len);
          s[len - 1] = '\0';
          RSSortingVector_PutStr(vec, i, RS_StringValT(rm_strdup(s), len - 1, RSString_RMAlloc), true);
          RedisModule_Free(s);
          break;
        }
        case RS_SORTABLE_NUM:
          // load numeric value
          RSSortingVector_PutNum(vec, i, RedisModule_LoadDouble(rdb));
          break;
        // for nil we read nothing
        case RS_SORTABLE_NIL:
        default:
        //RSSortingVector_PutNull()
        //vec->values[i] = RS_NullVal();
          break;
      }
    }
    return vec;
  }

  /* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str) {

  size_t buflen = 2 * strlen(str) + 1;
  char *lower_buffer = rm_calloc(buflen, 1);
  char *lower = lower_buffer;
  char *end = lower + buflen;

  const char *p = str;
  size_t off = 0;
  while (*p != 0 && lower < end) {
    uint32_t in = 0;
    p = nu_utf8_read(p, &in);
    const char *lo = nu_tofold(in);

    if (lo != 0) {
      uint32_t u = 0;
      do {
        lo = nu_casemap_read(lo, &u);
        if (u == 0) break;
        lower = nu_utf8_write(u, lower);
      } while (u != 0 && lower < end);
    } else {
      lower = nu_utf8_write(in, lower);
    }
  }

  return lower_buffer;
}