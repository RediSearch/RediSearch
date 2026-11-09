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
import type { PortPlan } from '../../c-to-rust/port.ts';
import { validatePlan, pullRequest, type Plan } from '../../core/plan.ts';
import { reviewBrief } from '../../core/review.ts';
import type { Vcs } from '../../core/ports.ts';
import { profile } from '../profile.ts';

/** What the profile's plan hook says before it would inspect the repository. */
const inspect = (plan: Plan<PortPlan>) =>
  profile.checkPlan!(plan, { root: '.', vcs: {} as Vcs, base: 'b' });

const LINT =
  'make lint && make fmt CHECK=1 && cargo hakari generate --diff && cargo hakari manage-deps --dry-run';
const NOTES = '- [ ] This PR requires release notes\n- [x] This PR does not require release notes';
const unitsTest = { path: 'tests/pytests/test_geo.py', name: 'testUnits', stage: 'rust' };
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

const plan: Plan<PortPlan> = {
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
    (['rust', 'swap'] as const).map((s) => [
      s,
      {
        deliverable: `${s} deliverable`,
        paths: [`src/redisearch_rs/geo/`],
        title: 't',
        body: `b\n\n${NOTES}`,
        checks: checks[s].map((command, i) => ({ kind: kinds[s][i], command })),
      },
    ]),
  ),
};

test('a plan needs tested contract items and divergences, and the stage checks', async () => {
  assert.deepEqual(validatePlan(plan, profile), []);
  const broken = structuredClone(plan);
  broken.contract = [{ behavior: 'b', tests: [] }];
  broken.divergences[0].test = { ...unitsTest, stage: 'later' };
  broken.stages.rust.checks = broken.stages.rust.checks.filter((c) => c.kind !== 'bench');
  broken.stages.swap.checks[2].command = 'make lint';
  broken.stages.swap.checks[1].command += ' || true';
  assert.deepEqual(validatePlan(broken, profile), [
    'contract[0] needs the tests that pin it',
    'divergences[0] needs a test: { path, name, stage }, stage one of rust, swap',
    'rust needs a bench check running `cargo bench`',
    'swap miri check chains commands with something other than `&&`',
    'swap needs a lint check running `make lint` and running `make fmt CHECK=1` and running ' +
      '`cargo hakari generate --diff` and ' +
      'running `cargo hakari manage-deps --dry-run`',
  ]);
});

test('a port plan needs its module, symbols, PR template and behavior-named tests', async () => {
  const broken = structuredClone(plan);
  broken.module = ['src/geo_index.h'];
  broken.symbols[0].rust = ' ';
  broken.stages.swap.body = NOTES.replace('[ ]', '[x]');
  broken.contract[0].tests[0].name = 'testGeoContract';
  broken.stages.rust.checks[4].command += ' -- small';
  assert.deepEqual(await inspect(broken), [
    'swap body needs both release-notes boxes of the PR template, exactly one of them ticked',
    'module needs the C files being ported: sources and headers',
    'symbols[0] needs c and rust',
    'tests/pytests/test_geo.py::testGeoContract is named for its artifact; name it for the behavior it asserts',
    'rust bench check filters the benchmarks (--); run them all',
  ]);
});

test('a plan names where it publishes', () => {
  assert.deepEqual(validatePlan({ ...plan, repository: 'nope', remote: ' ' }, profile), [
    'remote is required',
    'repository must be owner/name',
  ]);
});

test('every PR lists the divergences, then folds away the plan and the run record', () => {
  const body = pullRequest(plan, profile, 'swap', '### Run').body;
  assert.match(body, /#### Behavior changes from C/);
  assert.match(body, /\*\*Parse failure output\.\*\* C: leaves lon and lat written\./);
  const none = pullRequest({ ...plan, divergences: [] }, profile, 'rust', '').body;
  assert.match(none, /None: the Rust behaves like the C everywhere\./);
  const folded = body.slice(body.indexOf('<details>'));
  assert.match(folded, /Tests: `tests\/pytests\/test_geo\.py::testUnits`/);
  assert.match(folded, /`swap` may change `src\/redisearch_rs\/geo\/` and must pass `test`/);
  assert.match(folded, /- `GeoFilter_Validate`: `geo::RadiusFilter::new`/);
  assert.match(folded, /### Run\n<\/details>/);
  assert.doesNotMatch(folded, /Divergences from C/);
});

test('the brief renders the stage, its limits and the plan as prose, not JSON', () => {
  const text = reviewBrief(profile.reviewers[0], {
    plan,
    profile,
    stage: 'rust',
    candidate: 'c1',
    base: 'b1',
    diff: '/runs/1/diff.patch',
    stat: 'src/lib.rs | +3 -0',
    lines: 120,
  });
  for (const expected of [
    '# rust / Correctness review, stage 1 of 2',
    'rust deliverable',
    'It may change only `src/redisearch_rs/geo/`, and adds 120 lines.',
    'Later stages: `swap`: swap deliverable',
    'src/lib.rs | +3 -0',
    '`cargo miri nextest run -p geo`',
    'Tests: `tests/pytests/test_geo.py::testUnits`',
    '**Parse failure output.**',
    'The C being ported, `src/geo_index.c`, `src/geo_index.h`, is in the working tree until `swap` deletes it.',
    '- `GeoFilter_Validate`: `geo::RadiusFilter::new`',
    plan.interfaces,
  ])
    assert.ok(text.includes(expected), expected);
  assert.ok(!text.includes('"stages"'));
});
