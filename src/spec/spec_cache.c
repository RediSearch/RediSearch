/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec_cache.h"
#include "spec.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "obfuscation/hidden.h"

static void IndexSpecCache_Free(IndexSpecCache *c) {
  for (size_t ii = 0; ii < c->nfields; ++ii) {
    if (c->fields[ii].fieldName != c->fields[ii].fieldPath) {
      HiddenString_Free(c->fields[ii].fieldName, true);
    }
    HiddenString_Free(c->fields[ii].fieldPath, true);
  }
  rm_free(c->fields);
  rm_free(c);
}

// The value of the refcount can get to 0 only if the index spec itself does not point to it anymore,
// and at this point the refcount only gets decremented so there is no wory of some thread increasing the
// refcount while we are freeing the cache.
void IndexSpecCache_Decref(IndexSpecCache *c) {
  if (c && !__atomic_sub_fetch(&c->refcount, 1, __ATOMIC_RELAXED)) {
    IndexSpecCache_Free(c);
  }
}

// Assuming the spec is properly locked before calling this function.
IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec) {
  IndexSpecCache *ret = rm_calloc(1, sizeof(*ret));
  ret->nfields = spec->numFields;
  ret->fields = rm_malloc(sizeof(*ret->fields) * ret->nfields);
  ret->refcount = 1;
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    const FieldSpec* fs = spec->fields + ii;
    FieldSpec* field = ret->fields + ii;
    *field = *fs;
    field->fieldName = HiddenString_Duplicate(fs->fieldName);
    // if name & path are pointing to the same string, copy only pointer
    if (fs->fieldName != fs->fieldPath) {
      field->fieldPath = HiddenString_Duplicate(fs->fieldPath);
    } else {
      // use the same pointer for both name and path
      field->fieldPath = field->fieldName;
    }
  }
  return ret;
}

IndexSpecCache *IndexSpec_GetSpecCache(const IndexSpec *spec) {
  RS_LOG_ASSERT(spec->spcache, "Index spec cache is NULL");
  __atomic_fetch_add(&spec->spcache->refcount, 1, __ATOMIC_RELAXED);
  return spec->spcache;
}
