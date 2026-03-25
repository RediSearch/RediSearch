/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "spec.h"

#include "rmutil/util.h"
#include "obfuscation/obfuscation_api.h"
#include "util/hash/hash.h"
#include "info/info_redis/threads/current_thread.h"

///////////////////////////////////////////////////////////////////////////////////////////////

const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *) = NULL;

RedisModuleType *IndexSpecType;

Version redisVersion;
Version rlecVersion;
bool isCrdt;
bool isTrimming = false;
bool isFlex = false;

// Default values make no limits.
size_t memoryLimit = -1;
size_t used_memory = 0;

//---------------------------------------------------------------------------------------------

/*
 * Initialize the spec's fields that are related to the cursors.
 */

void Cursors_initSpec(IndexSpec *spec) {
  spec->activeCursors = 0;
}

/*
 * Get a field spec by field name. Case sensitive!
 * Return the field spec if found, NULL if not.
 * Assuming the spec is properly locked before calling this function.
 */
const FieldSpec *IndexSpec_GetFieldWithLength(const IndexSpec *spec, const char *name, size_t len) {
  for (size_t i = 0; i < spec->numFields; i++) {
    const FieldSpec *fs = spec->fields + i;
    if (!HiddenString_CompareC(fs->fieldName, name, len)) {
      return fs;
    }
  }
  return NULL;
}

const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const HiddenString *name) {
  for (size_t i = 0; i < spec->numFields; i++) {
    const FieldSpec *fs = spec->fields + i;
    if (!HiddenString_Compare(fs->fieldName, name)) {
      return fs;
    }
  }
  return NULL;
}

// Assuming the spec is properly locked before calling this function.
t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(spec, name, len);
  if (!fs || !FIELD_IS(fs, INDEXFLD_T_FULLTEXT) || !FieldSpec_IsIndexable(fs)) return 0;

  return FIELD_BIT(fs);
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CheckPhoneticEnabled(const IndexSpec *sp, t_fieldMask fm) {
  if (!(sp->flags & Index_HasPhonetic)) {
    return 0;
  }

  if (fm == 0 || fm == (t_fieldMask)-1) {
    // No fields -- implicit phonetic match!
    return 1;
  }

  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = sp->fields + ii;
      if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && (FieldSpec_IsPhonetics(fs))) {
        return 1;
      }
    }
  }
  return 0;
}

// Assuming the spec is properly locked before calling this function.
int IndexSpec_CheckAllowSlopAndInorder(const IndexSpec *spec, t_fieldMask fm, QueryError *status) {
  for (size_t ii = 0; ii < spec->numFields; ++ii) {
    if (fm & ((t_fieldMask)1 << ii)) {
      const FieldSpec *fs = spec->fields + ii;
      if (FIELD_IS(fs, INDEXFLD_T_FULLTEXT) && (FieldSpec_IsUndefinedOrder(fs))) {
        QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_BAD_ORDER_OPTION,
                               "slop/inorder are not supported for field with undefined ordering", " `%s`", HiddenString_GetUnsafe(fs->fieldName, NULL));
        return 0;
      }
    }
  }
  return 1;
}

// Assuming the spec is properly locked before calling this function.
const FieldSpec *IndexSpec_GetFieldBySortingIndex(const IndexSpec *sp, uint16_t idx) {
  for (size_t ii = 0; ii < sp->numFields; ++ii) {
    if (sp->fields[ii].options & FieldSpec_Sortable && sp->fields[ii].sortIdx == idx) {
      return sp->fields + ii;
    }
  }
  return NULL;
}

// Assuming the spec is properly locked before calling this function.
const char *IndexSpec_GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && FIELD_IS(&sp->fields[i], INDEXFLD_T_FULLTEXT) &&
      FieldSpec_IsIndexable(&sp->fields[i])) {
      return HiddenString_GetUnsafe(sp->fields[i].fieldName, NULL);
    }
  }
  return NULL;
}

// Get the field spec by the field mask.
const FieldSpec *IndexSpec_GetFieldByBit(const IndexSpec *sp, t_fieldMask id) {
  for (int i = 0; i < sp->numFields; i++) {
    if (FIELD_BIT(&sp->fields[i]) == id && FIELD_IS(&sp->fields[i], INDEXFLD_T_FULLTEXT) &&
        FieldSpec_IsIndexable(&sp->fields[i])) {
      return &sp->fields[i];
    }
  }
  return NULL;
}

// Get the field specs that match a field mask.
arrayof(FieldSpec *) IndexSpec_GetFieldsByMask(const IndexSpec *sp, t_fieldMask mask) {
  arrayof(FieldSpec *) res = array_new(FieldSpec *, 2);
  for (int i = 0; i < sp->numFields; i++) {
    if (mask & FIELD_BIT(sp->fields + i) && FIELD_IS(sp->fields + i, INDEXFLD_T_FULLTEXT)) {
      array_append(res, sp->fields + i);
    }
  }
  return res;
}

arrayof(FieldSpec *) getFieldsByType(IndexSpec *spec, FieldType type) {
#define FIELDS_ARRAY_CAP 2
  arrayof(FieldSpec *) fields = array_new(FieldSpec *, FIELDS_ARRAY_CAP);
  for (int i = 0; i < spec->numFields; ++i) {
    if (FIELD_IS(spec->fields + i, type)) {
      array_append(fields, &(spec->fields[i]));
    }
  }
  return fields;
}

//---------------------------------------------------------------------------------------------

/* Check if Redis is currently loading from RDB. Our thread starts before RDB loading is finished */
int isRdbLoading(RedisModuleCtx *ctx) {
  long long isLoading = 0;
  RMUtilInfo *info = RMUtil_GetRedisInfo(ctx);
  if (!info) {
    return 0;
  }

  if (!RMUtilInfo_GetInt(info, "loading", &isLoading)) {
    isLoading = 0;
  }

  RMUtilRedisInfo_Free(info);
  return isLoading == 1;
}

//---------------------------------------------------------------------------------------------

const char *IndexSpec_FormatName(const IndexSpec *sp, bool obfuscate) {
    return obfuscate ? sp->obfuscatedName : HiddenString_GetUnsafe(sp->specName, NULL);
}

char *IndexSpec_FormatObfuscatedName(const HiddenString *specName) {
  Sha1 sha1;
  size_t len;
  const char* value = HiddenString_GetUnsafe(specName, &len);
  Sha1_Compute(value, len, &sha1);
  char buffer[MAX_OBFUSCATED_INDEX_NAME];
  Obfuscate_Index(&sha1, buffer);
  return rm_strdup(buffer);
}

bool IndexSpec_IsCoherent(IndexSpec *spec, sds* prefixes, size_t n_prefixes) {
  if (!spec || !spec->rule) {
    return false;
  }
  arrayof(HiddenUnicodeString*) spec_prefixes = spec->rule->prefixes;
  if (n_prefixes != array_len(spec_prefixes)) {
    return false;
  }

  // Validate that the prefixes in the arguments are the same as the ones in the
  // index (also in the same order)
  for (size_t i = 0; i < n_prefixes; i++) {
    sds arg = prefixes[i];
    if (HiddenUnicodeString_CompareC(spec_prefixes[i], arg) != 0) {
      // Unmatching prefixes
      return false;
    }
  }

  return true;
}

StrongRef IndexSpec_GetStrongRefUnsafe(const IndexSpec *spec) {
  return spec->own_ref;
}

StrongRef IndexSpecRef_Promote(WeakRef ref) {
  StrongRef strong = WeakRef_Promote(ref);
  IndexSpec *spec = StrongRef_Get(strong);
  if (spec) {
    CurrentThread_SetIndexSpec(strong);
  }
  return strong;
}

void IndexSpecRef_Release(StrongRef ref) {
  CurrentThread_ClearIndexSpec();
  StrongRef_Release(ref);
}

int CompareVersions(Version v1, Version v2) {
  if (v1.majorVersion < v2.majorVersion) {
    return -1;
  } else if (v1.majorVersion > v2.majorVersion) {
    return 1;
  }

  if (v1.minorVersion < v2.minorVersion) {
    return -1;
  } else if (v1.minorVersion > v2.minorVersion) {
    return 1;
  }

  if (v1.patchVersion < v2.patchVersion) {
    return -1;
  } else if (v1.patchVersion > v2.patchVersion) {
    return 1;
  }

  return 0;
}
