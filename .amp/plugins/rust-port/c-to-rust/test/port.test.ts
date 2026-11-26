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
import {
  checkPortPlan,
  cTests,
  describePortPlan,
  headerFunctions,
  type PortPlan,
} from '../port.ts';
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
  const p: Plan<PortPlan> = {
    ...plan,
    module: ['geo.c', 'geo.h'],
    symbols: [retired],
    replacedTests: [],
  };
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

test('a replaced C test is on the base branch, in the swap paths, and covered by a cited test', async () => {
  const replaced = { path: 'b/test_geo.cpp', name: 'GeoTest.units', by: plan.contract[0].tests[0] };
  const p: Plan<PortPlan> = {
    ...plan,
    module: ['geo.c', 'geo.h'],
    symbols: [{ c: 'Geo_Free', retired: 'Only tests use it.' }],
    replacedTests: [replaced],
  };
  const vcs = {
    readFileAt: async (_: string, path: string) =>
      path === 'geo.h' ? 'void Geo_Free(void);' : 'TEST_F(GeoTest, units) { radius(); }',
    listFiles: async () => [],
  } as unknown as Vcs;
  const errors = async (replacedTests: unknown) =>
    (
      await checkPortPlan({ ...p, replacedTests } as Plan<PortPlan>, profile.stages, {
        root: '.',
        vcs,
        base: 'base',
        workspace: 'missing',
      })
    ).filter((e) => e.startsWith('replacedTests'));
  assert.deepEqual(await errors([replaced]), []);
  assert.match(
    describePortPlan(p, 'b'),
    /`b\/test_geo\.cpp::GeoTest\.units`: by `a\/t\.txt::pins`/,
  );
  assert.match(describePortPlan({ ...p, replacedTests: [] }, 'b'), /replaces\n\nNone\.$/);
  assert.deepEqual(await errors(undefined), ['replacedTests is required, even if empty']);
  for (const r of [
    { ...replaced, name: ' ' },
    { ...replaced, by: { path: 'a/t.txt', name: 'uncited' } },
    { path: replaced.path, name: replaced.name },
  ])
    assert.deepEqual(await errors([r]), [
      'replacedTests[0] needs path, name and by, a test the contract or divergences cite',
    ]);
  assert.deepEqual(await errors([{ ...replaced, path: 'tests/test_geo.cpp' }]), [
    'replacedTests[0]: b may not change tests/test_geo.cpp; add it to its paths',
  ]);
  assert.deepEqual(await errors([{ ...replaced, name: 'GeoTest.radius' }]), [
    'replacedTests names b/test_geo.cpp::GeoTest.radius, which main lacks',
  ]);
  assert.deepEqual(await errors([{ ...replaced, name: 'OtherTest.units' }]), [
    'replacedTests names b/test_geo.cpp::OtherTest.units, which main lacks',
  ]);
});

test('a C test is its gtest declaration or TESTFUNC registration, not a mention', () => {
  const text = `// GeoTest.units and testOld are gone
TEST_F(GeoTest, units) {}
TYPED_TEST(Typed, sizes) {}
TEST_P( Param , each ) {}
TYPED_TEST_P(TypedP, grows) {}
TEST(Plain, works) {}
int testParse() { return 0; }
TEST_MAIN({
  TESTFUNC(testParse);
});
MU_RUN_TEST(test_chan);`;
  assert.deepEqual(cTests(text), [
    'GeoTest.units',
    'Typed.sizes',
    'Param.each',
    'TypedP.grows',
    'Plain.works',
    'testParse',
    'test_chan',
  ]);
});
