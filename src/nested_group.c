/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "nested_group.h"

#include <string.h>

#include "json.h"
#include "rdb.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "search_disk.h"
#include "spec.h"
#include "util/arr.h"

const NestedGroup *IndexSpec_NestedGroupOf(const IndexSpec *spec, const FieldSpec *member) {
  return FieldSpec_IsNestedMember(member) ? &spec->nestedGroups[member->nestedGroup] : NULL;
}

int IndexSpec_FindNestedGroup(const IndexSpec *spec, const char *name, size_t len) {
  for (uint32_t i = 0; i < array_len(spec->nestedGroups); ++i) {
    if (!HiddenString_CompareC(spec->nestedGroups[i].name, name, len)) {
      return i;
    }
  }
  return -1;
}

static void NestedGroup_Cleanup(NestedGroup *group) {
  if (group->compiledPath) {
    japi->pathFree(group->compiledPath);
  }
  HiddenString_Free(group->path, true);
  HiddenString_Free(group->name, true);
}

void IndexSpec_FreeNestedGroups(IndexSpec *sp, uint32_t first) {
  for (uint32_t i = first; i < array_len(sp->nestedGroups); ++i) {
    NestedGroup_Cleanup(&sp->nestedGroups[i]);
  }
  if (first) {
    array_set_len(sp->nestedGroups, first);
  } else if (sp->nestedGroups) {
    array_free(sp->nestedGroups);
    sp->nestedGroups = NULL;
  }
}

int IndexSpec_ParseNestedGroup(ArgsCursor *ac, IndexSpec *sp, const char *name, size_t namelen,
                               const char *path, int isNew, size_t *numMembers,
                               QueryError *status) {
  if (!isSpecJson(sp) || SearchDisk_IsEnabledForValidation()) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        SPEC_NESTED_STR " is supported only for in-memory JSON indexes");
    return 0;
  }
  if (!isNew) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        SPEC_NESTED_STR " cannot be added to an existing index");
    return 0;
  }
  if (IndexSpec_FindNestedGroup(sp, name, namelen) >= 0 ||
      IndexSpec_GetFieldWithLength(sp, name, namelen)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
                                  "Duplicate " SPEC_NESTED_STR " group or field in schema",
                                  " - %s", name);
    return 0;
  }
  RS_LOG_ASSERT(array_len(sp->nestedGroups) < SPEC_MAX_FIELDS,
                "Every group has a member, and a schema has SPEC_MAX_FIELDS fields at most");
  NestedGroup group = {.name = NewHiddenString(name, namelen, true),
                       .path = NewHiddenString(path, strlen(path), true)};
  // Compiled from the group's own copy of the text: see NestedGroup.compiledPath.
  group.compiledPath = japi->pathParseNoErrorMsg(HiddenString_GetUnsafe(group.path, NULL));
  array_ensure_append_1(sp->nestedGroups, group);
  if (!group.compiledPath) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL, "Invalid JSONPath",
                                  " '%s' of " SPEC_NESTED_STR " group '%s'", path, name);
    return 0;
  }
  if (japi->pathIsSingle(group.compiledPath)) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        "The path of a " SPEC_NESTED_STR " group must select the elements of an"
                        " array, as $.items[*] does");
    return 0;
  }
  if (AC_GetSize(ac, numMembers, AC_F_GE1) != AC_OK) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS,
                        SPEC_NESTED_STR " requires the number of fields in the group");
    return 0;
  }
  return 1;
}

typedef enum {
  NestedValuesPath_Ok,
  // The member's path cannot follow the group's: it has to start at the element.
  NestedValuesPath_NotAtElement,
  NestedValuesPath_DoesNotCompile,
} NestedValuesPathStatus;

/* Set FieldSpec.nestedValuesPathText of `fs`, a member of a NESTED group, and
 * FieldSpec.nestedValuesPath of a NUMERIC member. Sets nothing on failure. */
static NestedValuesPathStatus setNestedValuesPath(const IndexSpec *sp, FieldSpec *fs) {
  const char *path = HiddenString_GetUnsafe(fs->fieldPath, NULL);
  if (path[0] != '$') {
    return NestedValuesPath_NotAtElement;
  }
  const NestedGroup *group = IndexSpec_NestedGroupOf(sp, fs);
  rm_asprintf(&fs->nestedValuesPathText, "%s%s", HiddenString_GetUnsafe(group->path, NULL),
              path + 1);
  // A query loads every member by this path, so it has to compile for every member.
  JSONPath compiled = japi->pathParseNoErrorMsg(fs->nestedValuesPathText);
  if (!compiled) {
    rm_free(fs->nestedValuesPathText);
    fs->nestedValuesPathText = NULL;
    return NestedValuesPath_DoesNotCompile;
  }
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    fs->nestedValuesPath = compiled;
  } else {
    japi->pathFree(compiled);
  }
  return NestedValuesPath_Ok;
}

// Whether `fs` is what a member of a NESTED group must be, apart from its paths: a TAG that is
// not SORTABLE, or a SORTABLE NUMERIC. NOINDEX, INDEXEMPTY and INDEXMISSING are refused:
// a member is always loaded, and only from the elements that have a value.
static bool isNestedMemberDefinition(const FieldSpec *fs) {
  if (fs->types == INDEXFLD_T_NUMERIC) {
    if (!FieldSpec_IsSortable(fs)) {
      return false;
    }
  } else if (fs->types != INDEXFLD_T_TAG || FieldSpec_IsSortable(fs)) {
    return false;
  }
  return FieldSpec_IsIndexable(fs) && !FieldSpec_IndexesEmpty(fs) && !FieldSpec_IndexesMissing(fs);
}

int IndexSpec_AddNestedMember(const IndexSpec *sp, FieldSpec *fs, uint16_t group,
                              QueryError *status) {
  const char *name = HiddenString_GetUnsafe(fs->fieldName, NULL);
  if (FIELD_IS(fs, INDEXFLD_T_NUMERIC)) {
    // A numeric condition on one element is tested on the values stored with the document,
    // which is where a SORTABLE field keeps them. So a NUMERIC member is SORTABLE whether or
    // not its definition says so.
    FieldSpec_SetSortable(fs);
  }
  if (!isNestedMemberDefinition(fs)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                  "A " SPEC_NESTED_STR " group supports only TAG and NUMERIC"
                                  " fields, SORTABLE only on NUMERIC, and neither "
                                  SPEC_NOINDEX_STR ", " SPEC_INDEXEMPTY_STR " nor "
                                  SPEC_INDEXMISSING_STR,
                                  " - %s", name);
    return 0;
  }
  if (!IndexSpec_ValidateJsonFieldPath(sp, fs, status)) {
    return 0;
  }
  // One element gives one entry: a tag index entry, or a slot of the stored numbers.
  if (!japi->pathIsSingle(fs->compiledPath)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                  "The path of a field of a " SPEC_NESTED_STR " group must select"
                                  " at most one value in an element",
                                  " - %s", name);
    return 0;
  }
  fs->options |= FieldSpec_NestedMember;
  fs->nestedGroup = group;
  switch (setNestedValuesPath(sp, fs)) {
    case NestedValuesPath_Ok:
      return 1;
    case NestedValuesPath_NotAtElement:
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS,
                                    "The path of a field of a " SPEC_NESTED_STR " group must"
                                    " start at an element, as $.name does",
                                    " - %s", name);
      return 0;
    case NestedValuesPath_DoesNotCompile:
      QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_INVAL,
                                    "Invalid JSONPath after the path of the " SPEC_NESTED_STR
                                    " group",
                                    " - %s", name);
      return 0;
  }
  return 0;
}

int IndexSpec_RdbLoadNestedGroups(RedisModuleIO *rdb, IndexSpec *sp) {
  uint64_t numGroups = LoadUnsigned_IOError(rdb, goto fail);
  if (numGroups == 0 || numGroups > UINT16_MAX) {
    goto fail;
  }
  for (uint64_t i = 0; i < numGroups; ++i) {
    char *name = NULL, *path = NULL;
    size_t nameLen = 0, pathLen = 0;
    LoadStringBufferAlloc_IOErrors(rdb, name, &nameLen, true, goto fail);
    LoadStringBufferAlloc_IOErrors(rdb, path, &pathLen, true, {
      rm_free(name);
      goto fail;
    });
    // A string of a corrupt payload can lack its terminator or hold one early.
    if (name[nameLen] != '\0' || path[pathLen] != '\0' || memchr(name, '\0', nameLen) ||
        memchr(path, '\0', pathLen)) {
      rm_free(name);
      rm_free(path);
      goto fail;
    }
    NestedGroup group = {.name = NewHiddenString(name, nameLen, true),
                         .path = NewHiddenString(path, pathLen, true)};
    rm_free(name);
    rm_free(path);
    array_ensure_append_1(sp->nestedGroups, group);
  }
  return REDISMODULE_OK;

fail:
  return REDISMODULE_ERR;
}

bool IndexSpec_PrepareLoadedNestedGroups(IndexSpec *sp) {
  const uint32_t numGroups = array_len(sp->nestedGroups);
  if (numGroups && (!isSpecJson(sp) || SearchDisk_IsEnabledForValidation())) {
    return false;
  }
  for (uint32_t i = 0; i < numGroups; ++i) {
    NestedGroup *group = &sp->nestedGroups[i];
    size_t len;
    const char *name = HiddenString_GetUnsafe(group->name, &len);
    if (IndexSpec_FindNestedGroup(sp, name, len) != i ||
        IndexSpec_GetFieldWithLength(sp, name, len)) {
      return false;
    }
    group->compiledPath = japi->pathParseNoErrorMsg(HiddenString_GetUnsafe(group->path, NULL));
    if (!group->compiledPath || japi->pathIsSingle(group->compiledPath)) {
      return false;
    }
  }

  uint32_t numGroupsWithMembers = 0;
  bool *hasMembers = rm_calloc(numGroups + 1, sizeof(*hasMembers));
  bool valid = true;
  for (size_t i = 0; valid && i < sp->numFields; ++i) {
    FieldSpec *fs = &sp->fields[i];
    if (!FieldSpec_IsNestedMember(fs)) {
      continue;
    }
    valid = fs->nestedGroup < numGroups && isNestedMemberDefinition(fs) &&
            (fs->types != INDEXFLD_T_NUMERIC || fs->sortIdx >= 0) && fs->compiledPath &&
            japi->pathIsSingle(fs->compiledPath) &&
            setNestedValuesPath(sp, fs) == NestedValuesPath_Ok;
    if (valid && !hasMembers[fs->nestedGroup]) {
      hasMembers[fs->nestedGroup] = true;
      ++numGroupsWithMembers;
    }
  }
  rm_free(hasMembers);
  return valid && numGroupsWithMembers == numGroups;
}
