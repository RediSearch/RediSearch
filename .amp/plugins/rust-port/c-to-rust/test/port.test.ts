/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { headerFunctions } from '../port.ts';

test('the declarations of a header are its functions, not its macros, types or comments', () => {
  const header = `/* NewGeoFilter(...) frees nothing */
/*
Fragments score(occurrence) by how often a term appears,
as in DB(lookup) of a whole document
*/
#define X(c, unused) GEO_##c,
typedef int (*Cb)(void);
  X(KM, "km")
GeoFilter *NewGeoFilter(double lon, double lat,
                        double radius);
// isWithinRadius(gf) in a comment
int isWithinRadius(const GeoFilter *gf, double d, double *distance);
static inline int helper(void) { return 0; }
extern const char *GEO_UNITS;
extern void GeoFilter_Free(GeoFilter *gf);
extern arrayof(int*) asm_sanitizer_allocs;
extern khash_t(cursors) *cursorsTable;
arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, rune *str);
arrayof(char **)
    TagIndex_GetSuffixMatches(const TagIndex *idx, const char *str);
RS_ENUM_BITWISE_HELPER(FieldSpecOptions)
KHASH_MAP_INIT_INT64(cursors, Cursor *);
static_assert(sizeof(GeoFilter) == 48, "layout");`;
  assert.deepEqual(headerFunctions(header), [
    'NewGeoFilter',
    'isWithinRadius',
    'GeoFilter_Free',
    'GetList_SuffixTrieMap',
    'TagIndex_GetSuffixMatches',
  ]);
});
