#include "rlookup.h"
#include <assert.h>
#include <util/arr.h>

static RLookupKey *createNewKey(RLookup *lookup, const char *name, int flags, uint16_t idx) {
  RLookupKey *ret = calloc(1, sizeof(*ret));

  ret->flags = (flags & (~RLOOKUP_TRANSIENT_FLAGS));
  ret->idx = idx;
  ret->refcnt = 1;

  if (flags & RLOOKUP_F_NAMEALLOC) {
    ret->name = strdup(name);
  } else {
    ret->name = name;
  }

  if (!lookup->head) {
    lookup->head = lookup->tail = ret;
  } else {
    lookup->tail->next = ret;
    lookup->tail = ret;
  }
  return ret;
}

static RLookupKey *genKeyFromSpec(RLookup *lookup, const char *name, int flags) {
  const IndexSpecCache *cc = lookup->spcache;
  if (!cc) {
    return NULL;
  }

  const FieldSpec *fs = NULL;
  for (size_t ii = 0; ii < cc->nfields; ++ii) {
    if (!strcmp(cc->fields[ii].name, name)) {
      fs = cc->fields + ii;
      break;
    }
  }

  if (!fs) {
    // Field does not exist in the schema at all
    return NULL;
  }

  uint16_t idx;

  if (FieldSpec_IsSortable(fs)) {
    flags |= RLOOKUP_F_SVSRC;
    idx = fs->sortIdx;
  } else {
    flags |= RLOOKUP_F_DOCSRC;
    idx = lookup->rowlen++;
  }

  return createNewKey(lookup, name, flags, idx);
}

RLookupKey *RLookup_GetKey(RLookup *lookup, const char *name, int flags) {
  RLookupKey *ret = NULL;
  int isNew = 0;

  for (RLookupKey *kk = lookup->head; kk; kk = kk->next) {
    if (!strcmp(kk->name, name)) {
      ret = kk;
      break;
    }
  }

  if (!ret) {
    ret = genKeyFromSpec(lookup, name, flags);
  }

  if (!ret) {
    if (!(flags & RLOOKUP_F_OCREAT)) {
      return NULL;
    } else {
      ret = createNewKey(lookup, name, flags, lookup->rowlen++);
    }
  }

  if (!(flags & RLOOKUP_F_NOINCREF)) {
    ret->refcnt++;
  }
  return ret;
}

void RLookup_WriteKey(const RLookupKey *key, RLookupRow *row, RSValue *v) {
  assert(!(key->flags & RLOOKUP_F_SVSRC));

  // Find the pointer to write to ...
  RSValue **vptr = array_ensure_at(&row->dyn, key->idx, RSValue *);
  if (*vptr) {
    RSValue_Decref(*vptr);
    row->ndyn--;
  }
  *vptr = v;
  RSValue_IncrRef(v);
  row->ndyn++;
}

void RLookup_WriteKeyByName(RLookup *lookup, const char *name, RLookupRow *dst, RSValue *v) {
  // Get the key first
  RLookupKey *k =
      RLookup_GetKey(lookup, name, RLOOKUP_F_NAMEALLOC | RLOOKUP_F_NOINCREF | RLOOKUP_F_OCREAT);
  assert(k);
  RLookup_WriteKey(k, dst, v);
}

void RLookupRow_Wipe(RLookupRow *r) {
  for (size_t ii = 0; ii < array_len(r->dyn) && r->ndyn; ++ii) {
    RSValue **vpp = r->dyn + ii;
    if (*vpp) {
      RSValue_Decref(*vpp);
      *vpp = NULL;
      r->ndyn--;
    }
  }
}

void RLookupRow_Cleanup(RLookupRow *r) {
  RLookupRow_Wipe(r);
  array_free(r->dyn);
}