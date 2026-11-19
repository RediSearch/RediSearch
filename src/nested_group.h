/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef NESTED_GROUP_H__
#define NESTED_GROUP_H__

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "redismodule.h"
#include "field_spec.h"
#include "obfuscation/hidden.h"
#include "query_error_ffi.h"
#include "rejson_api.h"
#include "rmutil/args.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;

// A NESTED group: a JSON array whose elements hold the values of the group's members. A query
// names the group to require conditions on its members to hold on one element.
typedef struct NestedGroup {
  HiddenString *name;
  // Selects the elements of the array.
  HiddenString *path;
  // The compiled form of `path`, which borrows its text.
  JSONPath compiledPath;
} NestedGroup;

/* The NESTED group that `member` belongs to. NULL when `member` is not a member of a group. */
const NestedGroup *IndexSpec_NestedGroupOf(const struct IndexSpec *spec, const FieldSpec *member);

/* The position in `spec->nestedGroups` of the group called `name`, or -1 when there is none. */
int IndexSpec_FindNestedGroup(const struct IndexSpec *spec, const char *name, size_t len);

/* Free the groups of `sp` from position `first` on. */
void IndexSpec_FreeNestedGroups(struct IndexSpec *sp, uint32_t first);

/* Parse a NESTED group called `name` at `path`, after the NESTED keyword, and add it to `sp`.
 * The next `*numMembers` field definitions belong to it. Returns 1 on successful parse, 0
 * otherwise */
int IndexSpec_ParseNestedGroup(ArgsCursor *ac, struct IndexSpec *sp, const char *name,
                               size_t namelen, const char *path, int isNew, size_t *numMembers,
                               QueryError *status);

/* Make `fs`, just parsed, a member of the NESTED group at `group`. Returns 1 on success, 0
 * otherwise */
int IndexSpec_AddNestedMember(const struct IndexSpec *sp, FieldSpec *fs, uint16_t group,
                              QueryError *status);

/* Load the NESTED groups of `sp`, which IndexSpec_RdbSave puts after the fields. */
int IndexSpec_RdbLoadNestedGroups(RedisModuleIO *rdb, struct IndexSpec *sp);

/* Make the NESTED groups of a loaded schema usable: compile their paths and those of their
 * members. Returns false for groups that FT.CREATE would not have built, which everything that
 * reads a group relies on. */
bool IndexSpec_PrepareLoadedNestedGroups(struct IndexSpec *sp);

#ifdef __cplusplus
}
#endif

#endif  // NESTED_GROUP_H__
