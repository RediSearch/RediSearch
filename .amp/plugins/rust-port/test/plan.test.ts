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
import { benchFilter, declarations, pr, STAGES, validate, type Plan } from '../src/plan.ts';
import { brief, REVIEWERS } from '../src/review.ts';

const LINT = 'make lint && cargo hakari generate --diff && cargo hakari manage-deps --dry-run';
const unitsTest = { path: 'tests/pytests/test_geo.py', name: 'testUnits', stage: 'rust' as const };
const checks = {
  rust: [
    'test',
    'cargo miri nextest run -p geo',
    'cargo mutants -p geo',
    LINT,
    'cargo bench -p geo_bencher',
  ],
  swap: ['test', 'cargo miri nextest run -p geo_ffi', LINT],
};
const kinds = {
  rust: ['test', 'miri', 'mutants', 'lint', 'bench'],
  swap: ['test', 'miri', 'lint'],
};

const plan: Plan = {
  repository: 'o/r',
  remote: 'origin',
  baseBranch: 'master',
  branchPrefix: 'p',
  module: ['src/geo_index.c', 'src/geo_index.h'],
  contract: [
    { behavior: 'An unknown unit fails with "Unknown distance unit".', tests: [unitsTest] },
  ],
  divergences: [
    {
      behavior: 'Parse failure output',
      c: 'leaves lon and lat written.',
      rust: 'leaves the filter untouched.',
      why: 'no caller reads it.',
      test: { path: 'src/redisearch_rs/geo/tests/filter.rs', name: 'failed_parse', stage: 'rust' },
    },
  ],
  symbols: [{ c: 'GeoFilter_Validate', rust: 'geo::RadiusFilter::new' }],
  structure: 'A geo crate shared by all stages.',
  interfaces: 'GeoFilter_Validate(const GeoFilter *, QueryError *) -> int',
  stages: Object.fromEntries(
    STAGES.map((s) => [
      s,
      {
        deliverable: `${s} deliverable`,
        paths: [`src/redisearch_rs/geo/`],
        title: 't',
        body: 'b',
        checks: checks[s].map((command, i) => ({ kind: kinds[s][i], command })),
      },
    ]),
  ) as Plan['stages'],
};

test('a plan needs tested contract items and divergences, and the stage checks', () => {
  assert.deepEqual(validate(plan), []);
  const broken = structuredClone(plan);
  broken.module = ['src/geo_index.h'];
  broken.contract = [{ behavior: 'b', tests: [] }];
  broken.divergences[0].test = { ...unitsTest, stage: 'later' as 'rust' };
  broken.symbols[0].rust = ' ';
  broken.stages.rust.checks = broken.stages.rust.checks.filter((c) => c.kind !== 'bench');
  broken.stages.swap.checks[2].command = 'make lint';
  broken.stages.swap.checks[1].command += ' || true';
  assert.deepEqual(validate(broken), [
    'module needs the C files being ported: sources and headers',
    'contract[0] needs the tests that pin it',
    'divergences[0] needs a test: { path, name, stage }, stage one of rust, swap',
    'symbols[0] needs c and rust',
    'rust needs a bench check running `cargo bench`',
    'swap miri check chains commands with something other than `&&`',
    'swap needs a lint check running `make lint` and running `cargo hakari generate --diff` and ' +
      'running `cargo hakari manage-deps --dry-run`',
  ]);
});

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
arrayof(char**) GetList_SuffixTrieMap(TrieMap *trie, rune *str);
arrayof(char **)
    TagIndex_GetSuffixMatches(const TagIndex *idx, const char *str);
RS_ENUM_BITWISE_HELPER(FieldSpecOptions)
KHASH_MAP_INIT_INT64(cursors, Cursor *);
static_assert(sizeof(GeoFilter) == 48, "layout");`;
  assert.deepEqual(declarations(header), [
    'NewGeoFilter',
    'isWithinRadius',
    'GeoFilter_Free',
    'GetList_SuffixTrieMap',
    'TagIndex_GetSuffixMatches',
  ]);
});

test('every PR lists the divergences, then folds away the plan and the run record', () => {
  const body = pr(plan, 'swap', '### Run').body;
  assert.match(body, /#### Behavior changes from C/);
  assert.match(body, /\*\*Parse failure output\.\*\* C: leaves lon and lat written\./);
  const none = pr({ ...plan, divergences: [] }, 'rust', '').body;
  assert.match(none, /None: the Rust behaves like the C everywhere\./);
  const folded = body.slice(body.indexOf('<details>'));
  assert.match(folded, /Tests: `tests\/pytests\/test_geo\.py::testUnits`/);
  assert.match(folded, /`swap` may change `src\/redisearch_rs\/geo\/` and must pass `test`/);
  assert.match(folded, /### Run\n<\/details>/);
  assert.doesNotMatch(folded, /Divergences from C/);
});

test('the brief renders the stage, its limits and the plan as prose, not JSON', () => {
  const text = brief(REVIEWERS[0], {
    plan,
    stage: 'rust',
    candidate: 'c1',
    base: 'b1',
    diff: '/runs/1/diff.patch',
    stat: ' src/lib.rs | 3 +++',
    lines: 120,
  });
  for (const expected of [
    '# rust / Correctness review, stage 1 of 2',
    'rust deliverable',
    'It may change only `src/redisearch_rs/geo/`, and adds 120 lines.',
    'Later stages: `swap`: swap deliverable',
    ' src/lib.rs | 3 +++',
    '`cargo miri nextest run -p geo`',
    'Tests: `tests/pytests/test_geo.py::testUnits`',
    '**Parse failure output.**',
    '- `GeoFilter_Validate`: `geo::RadiusFilter::new`',
    plan.interfaces,
  ])
    assert.ok(text.includes(expected), expected);
  assert.ok(!text.includes('"stages"'));
});

test('a plan names where it publishes', () => {
  assert.deepEqual(validate({ ...plan, repository: 'nope', remote: ' ' }), [
    'remote is required',
    'repository must be owner/name',
  ]);
});

test('a bench check runs every benchmark: no bench target and no Criterion filter', () => {
  assert.deepEqual(
    [
      'cd src/redisearch_rs && cargo bench -p quantile_bencher',
      'cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p geo_bencher',
      'cargo bench -p quantile_bencher small',
      'cargo bench -p quantile_bencher -- small',
      'cargo bench -p quantile_bencher --bench stream',
    ].map(benchFilter),
    [undefined, undefined, 'small', '--', '--bench'],
  );
});
