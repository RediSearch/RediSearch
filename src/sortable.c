#include "sortable.h"
#include "buffer.h"
#include "rmalloc.h"

#include "rmutil/rm_assert.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"

#include "libnu/libnu.h"
#include "util/strconv.h"

#include <stdlib.h>
#include <stdio.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Create a sorting vector of a given length for a document

RSSortingVector::RSSortingVector(int len_) {
  if (len_ > RS_SORTABLES_MAX) {
    return;
  }
  len = len_;
  // set all values to NIL
  for (int i = 0; i < len_; i++) {
    values[i] = RS_NullVal()->IncrRef();
  }
}

//---------------------------------------------------------------------------------------------

// Internal compare function between members of the sorting vectors, sorted by sk

static int RSSortingVector::Cmp(RSSortingVector *self, RSSortingVector *other, RSSortingKey *sk,
                                QueryError *qerr) {
  RSValue *v1 = self->values[sk->index];
  RSValue *v2 = other->values[sk->index];
  int rc = RSValue::Cmp(v1, v2, qerr);
  return sk->ascending ? rc : -rc;
}

//---------------------------------------------------------------------------------------------

// Normalize sorting string for storage. This folds everything to unicode equivalent strings.
// The allocated return string needs to be freed later.

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

//---------------------------------------------------------------------------------------------

// Put a value in the sorting vector

void RSSortingVector::Put(int idx, const void *p, int type) {
  if (idx > RS_SORTABLES_MAX) {
    return;
  }
  if (values[idx]) {
    values[idx]->Decref();
    values[idx] = NULL;
  }
  switch (type) {
    case RS_SORTABLE_NUM:
      values[idx] = RS_NumVal(*(double *)p);

      break;
    case RS_SORTABLE_STR: {
      char *ns = normalizeStr((const char *)p);
      values[idx] = RS_StringValT(ns, strlen(ns), RSString_RMAlloc);
      break;
    }
    case RS_SORTABLE_NIL:
    default:
      values[idx] = RS_NullVal();
      break;
  }
}

//---------------------------------------------------------------------------------------------

// Free a sorting vector

RSSortingVector::~RSSortingVector() {
  for (size_t i = 0; i < len; i++) {
    values[i]->Decref();
  }
}

//---------------------------------------------------------------------------------------------

// Save a sorting vector to rdb. This is called from the doc table

void RSSortingVector::RdbSave(RedisModuleIO *rdb) {
  RedisModule_SaveUnsigned(rdb, len);
  for (int i = 0; i < len; i++) {
    RSValue *val = values[i];
    if (!val) {
      RedisModule_SaveUnsigned(rdb, RSValue_Null);
      continue;
    }
    RedisModule_SaveUnsigned(rdb, val->t);
    switch (val->t) {
      case RSValue_String:
        // save string - one extra byte for null terminator
        RedisModule_SaveStringBuffer(rdb, val->strval.str, val->strval.len + 1);
        break;

      case RSValue_Number:
        // save numeric value
        RedisModule_SaveDouble(rdb, val->numval);
        break;
      // for nil we write nothing
      default:
        break;
    }
  }
}

//---------------------------------------------------------------------------------------------

// Load a sorting vector from RDB

RSSortingVector::RSSortingVector(RedisModuleIO *rdb, int encver) {
  len = (int)RedisModule_LoadUnsigned(rdb);
  if (len > RS_SORTABLES_MAX || len <= 0) {
    return;
  }
  for (int i = 0; i < len; i++) {
    RSValueType t = RedisModule_LoadUnsigned(rdb);

    switch (t) {
      case RSValue_String: {
        size_t len;
        // strings include an extra character for null terminator. we set it to zero just in case
        char *s = RedisModule_LoadStringBuffer(rdb, &len);
        s[len - 1] = '\0';
        values[i] = RS_StringValT(rm_strdup(s), len - 1, RSString_RMAlloc);
        RedisModule_Free(s);
        break;
      }
      case RS_SORTABLE_NUM:
        // load numeric value
        values[i] = RS_NumVal(RedisModule_LoadDouble(rdb));
        break;
      // for nil we read nothing
      case RS_SORTABLE_NIL:
      default:
        values[i] = RS_NullVal();
        break;
    }
  }
}

//---------------------------------------------------------------------------------------------

// Returns the value for a given index. Does not increment the refcount

RSValue *RSSortingVector::Get(size_t index) {
  if (len <= index) {
    return NULL;
  }
  return values[index];
}

//---------------------------------------------------------------------------------------------

size_t RSSortingVector::memsize() const {
  size_t sum = len * sizeof(RSValue *);
  for (int i = 0; i < len; i++) {
    if (!values[i]) continue;
    sum += sizeof(RSValue);

    RSValue *val = values[i]->Dereference();
    if (val && val->IsString()) {
      size_t sz;
      val->StringPtrLen(&sz);
      sum += sz;
    }
  }
  return sum;
}

//---------------------------------------------------------------------------------------------

// Adds a field and returns the ID of the newly-inserted field

int RSSortingTable::Add(std::string_view name, RSValueType t) {
  if (len == RS_SORTABLES_MAX) return -1;

  fields[len].name = name;
  fields[len].type = t;
  return len++;
}

//---------------------------------------------------------------------------------------------

// Get the field index by name from the sorting table. Returns -1 if the field was not found

int RSSortingTable::GetFieldIdx(std::string_view field) {
  for (int i = 0; i < len; i++) {
    if (!str_casecmp(fields[i].name, field)) {
      return i;
    }
  }
  return -1;
}

///////////////////////////////////////////////////////////////////////////////////////////////
