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
import { checkPortPlan, describePortPlan, headerFunctions, type PortPlan } from '../port.ts';
import type { Plan } from '../../core/plan.ts';
import type { Vcs } from '../../core/ports.ts';
import { plan, profile } from '../../core/test/fakes.ts';

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

test('every declared function needs a replacement or an explicit retirement reason', async () => {
  const retired = { c: 'Geo_Free', retired: 'Only tests use it after the swap.' };
  const p: Plan<PortPlan> = { ...plan, module: ['geo.c', 'geo.h'], symbols: [retired] };
  const vcs = {
    readFileAt: async () => 'void Geo_Free(void);',
    listFiles: async () => [],
  } as unknown as Vcs;
  const errors = async (symbols: unknown) =>
    (
      await checkPortPlan({ ...p, symbols } as Plan<PortPlan>, profile.stages, {
        root: '.',
        vcs,
        base: 'base',
        workspace: 'missing',
      })
    ).filter((e) => e.startsWith('symbols'));
  assert.deepEqual(await errors([retired]), []);
  assert.match(describePortPlan(p, 'b'), /`Geo_Free`: retired — Only tests use it/);
  for (const symbol of [
    { c: 'Geo_Free' },
    { ...retired, retired: ' ' },
    { ...retired, rust: 'geo::free' },
  ])
    assert.deepEqual(await errors([symbol]), [
      'symbols[0] needs c and exactly one of rust or a retired reason',
    ]);
  assert.deepEqual(await errors([{ ...retired, c: 'Unknown' }]), [
    'symbols leaves out Geo_Free',
    'symbols names undeclared Unknown',
  ]);
});
