#include "tmp.h"

#include "value.h"

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