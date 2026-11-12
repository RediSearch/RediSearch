/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import assert from 'node:assert/strict';
import { describe, test } from 'node:test';
import {
  BUDGET,
  closeFinding,
  done,
  repliesDue,
  hash,
  blocksAdmission,
  isAdmitted,
  newState,
  outstanding,
  passed,
  progressMark,
  addFindings,
  runReport,
  replaceFindings,
  stopReason,
  takeNewFindings,
  validateRetro,
  type NewFinding,
  type State,
} from '../job.ts';
import { plan } from './fakes.ts';

const fresh = () => newState('T-1', plan);
const cited = [{ path: 'src/lib.rs', line: 3 }];

function admitted(): State {
  const state = fresh();
  state.job.admitted.a = { candidate: 'rust-1', base: 'main', plan: state.job.planHash };
  state.job.admitted.b = { candidate: 'swap-1', base: 'rust-1', plan: state.job.planHash };
  return state;
}

function finding(overrides: Partial<NewFinding> = {}): NewFinding {
  return {
    key: 'k',
    source: 'review',
    stage: 'b',
    revision: 'r1',
    blocking: true,
    title: 't',
    body: 'b',
    links: ['l1'],
    ...overrides,
  };
}

describe('admission', () => {
  test('a changed stage invalidates the stage built on it', () => {
    const { job } = admitted();
    job.admitted.a = { ...job.admitted.a!, candidate: 'rust-2' };
    assert.deepEqual([isAdmitted(job, 'a'), isAdmitted(job, 'b')], [true, false]);
  });

  test('readmitting an unchanged stage keeps the one built on it admitted', () => {
    const { job } = admitted();
    job.admitted.a = { ...job.admitted.a! };
    assert.deepEqual([isAdmitted(job, 'a'), isAdmitted(job, 'b')], [true, true]);
  });

  test('amending the plan invalidates every stage', () => {
    const { job } = admitted();
    job.planHash = hash('amended');
    assert.deepEqual([isAdmitted(job, 'a'), isAdmitted(job, 'b')], [false, false]);
  });

  test('evidence passes only when no check has a problem', () => {
    const check = { kind: 'test', command: 'c', exitCode: 0, started: 0, ms: 1, log: 'l' };
    const rest = {
      ...{ stage: 'a' as const, candidate: 'c', base: 'b', plan: 'p', patch: 'x' },
      ...{ reviewers: [{ reviewer: 'R' }], size: { lines: 1, code: 1, files: 1 } },
    };
    assert.equal(passed({ checks: [check], ...rest }), true);
    assert.equal(passed({ checks: [{ ...check, problem: 'is too slow' }], ...rest }), false);
  });

  test('suggestions and CI findings do not hold admission', () => {
    const state = fresh();
    addFindings(state, [
      finding({ key: 'a', blocking: false }),
      finding({ key: 'b', source: 'ci' }),
      finding({ key: 'c', stage: 'a' }),
    ]);
    assert.equal(
      outstanding(state).some((f) => blocksAdmission(f, 'b')),
      false,
    );
    assert.equal(
      outstanding(state).some((f) => blocksAdmission(f, 'a')),
      true,
    );
  });

  test('the job is done only when every stage is admitted and nothing is outstanding', () => {
    const state = admitted();
    addFindings(state, [finding({ blocking: false })]);
    assert.equal(done(state), false);
    closeFinding(state, 'F1', 'no-change', 'declined', cited);
    assert.equal(done(state), true);
  });
});

describe('findings', () => {
  test('exact duplicates merge their links; closed keys are never reopened', () => {
    const state = fresh();
    addFindings(state, [finding(), finding({ links: ['l2'] })]);
    assert.deepEqual(Object.keys(state.findings), ['F1']);
    assert.deepEqual(state.findings.F1.links, ['l1', 'l2']);
    closeFinding(state, 'F1', 'fixed', undefined, cited);
    addFindings(state, [finding()]);
    assert.deepEqual(outstanding(state), []);
  });

  test('close refuses what the worker may not close, or closes without its reason', () => {
    const thread = { source: 'comment' as const, replies: [{ pr: 1, comment: 7, thread: 't' }] };
    const cases = [
      [
        'a failed check',
        { source: 'check' as const },
        'no-change',
        'flaky',
        cited,
        /closes itself/,
      ],
      ['CI', { source: 'ci' as const }, 'fixed', undefined, cited, /closes itself/],
      ['no evidence', {}, 'fixed', undefined, [], /Cite evidence/],
      ['no-change without a reason', {}, 'no-change', ' ', cited, /reason/],
      ['a thread without a reason', thread, 'fixed', undefined, cited, /one-line reason/],
      ['a thread with two lines', thread, 'fixed', 'a\nb', cited, /one-line reason/],
      ['a thread with a long reason', thread, 'fixed', 'x'.repeat(281), cited, /one-line reason/],
    ] as const;
    for (const [name, overrides, resolution, reason, evidence, error] of cases) {
      const state = fresh();
      addFindings(state, [finding(overrides)]);
      assert.throws(
        () => closeFinding(state, 'F1', resolution, reason, [...evidence]),
        error,
        name,
      );
      assert.equal(state.findings.F1.closed, undefined, name);
    }
    const state = fresh();
    addFindings(state, [finding(thread)]);
    closeFinding(state, 'F1', 'fixed', 'Now rejects NaN radii.', cited);
    assert.equal(state.findings.F1.closed?.reason, 'Now rejects NaN radii.');
  });

  test('a no-change reply is due at once; a fix once its PR moved past the finding', () => {
    const state = fresh();
    const thread = (key: string) =>
      finding({ key, source: 'comment', revision: 'r1', replies: [{ pr: 1, comment: 7 }] });
    addFindings(state, [thread('a'), thread('b')]);
    closeFinding(state, 'F1', 'no-change', 'Literals only.', cited);
    closeFinding(state, 'F2', 'fixed', 'Rejects NaN.', cited);
    const replies = () => repliesDue(state).map((d) => [d.finding.id, d.reply]);
    assert.deepEqual(replies(), [['F1', 'Literals only.']]);
    state.job.prs.b = { number: 1, url: 'u', head: 'r1' };
    assert.deepEqual(replies(), [['F1', 'Literals only.']]);
    state.job.prs.b.head = 'r2'.padEnd(40, '0');
    assert.deepEqual(replies(), [
      ['F1', 'Literals only.'],
      ['F2', 'Fixed in r200000000: Rejects NaN.'],
    ]);
  });

  test('a comment raised again on a PR it was answered on is new feedback', () => {
    const state = fresh();
    const comment = (pr: number, thread: string) =>
      finding({ source: 'comment', replies: [{ pr, comment: 7, thread }] });
    addFindings(state, [comment(1, 't1')]);
    closeFinding(state, 'F1', 'fixed', 'Rejects NaN.', cited);
    state.findings.F1.replies = [];
    state.findings.F1.answered = [1];
    addFindings(state, [comment(2, 't2'), comment(1, 't9')]);
    assert.deepEqual(
      outstanding(state).map((f) => f.replies?.[0].thread),
      ['t9'],
    );
    assert.deepEqual(
      state.findings.F1.replies?.map((r) => r.pr),
      [2],
    );
  });

  test('a comment repeated on another stacked PR is one finding answered on both', () => {
    const state = fresh();
    const comment = (pr: number) =>
      finding({ source: 'comment', replies: [{ pr, comment: pr * 10, thread: `t${pr}` }] });
    addFindings(state, [comment(1), comment(2), comment(2)]);
    assert.deepEqual(Object.keys(state.findings), ['F1']);
    assert.deepEqual(
      state.findings.F1.replies?.map((r) => r.pr),
      [1, 2],
    );
    closeFinding(state, 'F1', 'no-change', 'Literals only.', cited);
    addFindings(state, [comment(3)]);
    assert.deepEqual(
      state.findings.F1.replies?.map((r) => r.pr),
      [1, 2, 3],
    );
  });

  test('a failure reported again on a new revision supersedes the open finding', () => {
    const state = fresh();
    const failure = (revision: string) => finding({ source: 'ci', key: 'gate', revision });
    replaceFindings(state, 'b', 'ci', [failure('r1')]);
    replaceFindings(state, 'b', 'ci', [failure('r1')]);
    replaceFindings(state, 'b', 'ci', [failure('r2')]);
    assert.equal(state.findings.F1.closed?.resolution, 'superseded');
    assert.deepEqual(
      takeNewFindings(state).map((f) => [f.id, f.revision]),
      [['F2', 'r2']],
    );
    // A rerun on the same head that fails differently replaces the stale details.
    replaceFindings(state, 'b', 'ci', [{ ...failure('r2'), body: 'lint failed' }]);
    assert.equal(state.findings.F2.closed?.resolution, 'superseded');
    assert.equal(state.findings.F3.body, 'lint failed');
    replaceFindings(state, 'b', 'ci', []);
    assert.equal(state.findings.F3.closed?.resolution, 'passed');
  });

  test('each finding is delivered once', () => {
    const state = fresh();
    addFindings(state, [finding({ key: 'a' }), finding({ key: 'b' })]);
    assert.deepEqual(
      takeNewFindings(state).map((f) => f.id),
      ['F1', 'F2'],
    );
    addFindings(state, [finding({ key: 'c' })]);
    assert.deepEqual(
      takeNewFindings(state).map((f) => f.id),
      ['F3'],
    );
    assert.deepEqual(takeNewFindings(state), []);
  });
});

describe('stopping', () => {
  test('grants extend the budget without resetting what was used', () => {
    const { job } = fresh();
    job.used = BUDGET;
    assert.equal(stopReason(job), `the assessment budget (${BUDGET}) is used up`);
    job.grants.push({ amount: 2, at: '', by: '' });
    assert.equal(stopReason(job), undefined);
    assert.equal(job.used, BUDGET);
  });

  test('a blocker the worker reported stops the job', () => {
    const { job } = fresh();
    job.blocked = 'no push access';
    assert.equal(stopReason(job), 'blocked: no push access');
  });

  test('progress changes with assessments, findings and admissions, not with delivery', () => {
    const state = fresh();
    const before = progressMark(state);
    state.job.delivered = 5;
    assert.equal(progressMark(state), before);
    addFindings(state, [finding()]);
    const raised = progressMark(state);
    assert.notEqual(raised, before);
    closeFinding(state, 'F1', 'fixed', undefined, cited);
    assert.notEqual(progressMark(state), raised);
  });
});

test('a postmortem needs a mistake, a cut and an improvement, each with evidence', () => {
  const evidence = [{ finding: 'F1' }];
  const mistake = { what: 'w', cost: 'c', prevention: 'p', evidence };
  const cut = { what: 'w', lines: '40', evidence };
  const improvement = { problem: 'p', proposal: 'q', target: 'checks' as const, evidence };
  assert.deepEqual(
    validateRetro({ mistakes: [mistake], cuts: [cut], improvements: [improvement] }),
    [],
  );
  assert.deepEqual(
    validateRetro({
      mistakes: [],
      cuts: [{ ...cut, evidence: [] }],
      improvements: [{ ...improvement, target: 'elsewhere' as 'checks' }],
    }),
    [
      'mistakes needs at least one item',
      'cuts[0] needs evidence',
      'improvements[0].target must be one of plugin, policy, skills, checks, reviewers',
    ],
  );
});

test('a hash does not depend on the order of keys', () => {
  assert.equal(hash({ a: 1, b: { c: 2, d: [3, 4] } }), hash({ b: { d: [3, 4], c: 2 }, a: 1 }));
  assert.notEqual(hash({ d: [3, 4] }), hash({ d: [4, 3] }));
});

test('the run record counts assessments and findings, and ends in the retrospective', () => {
  const state = admitted();
  state.job.used = 4;
  addFindings(state, [
    finding({ source: 'check' }),
    finding({ key: 'r' }),
    finding({ key: 's', blocking: false }),
  ]);
  closeFinding(state, 'F2', 'fixed', undefined, cited);
  assert.match(runReport(state), new RegExp(`4 of ${BUDGET} assessments used`));
  assert.match(runReport(state), /`b` assessed 0 times, admitted at swap-1/);
  assert.match(
    runReport(state),
    /3 findings: 2 review, 0 comment, 1 check, 0 ci\. 1 of 2 reviewer/,
  );
  assert.match(runReport(state), /Closed: 1 fixed, 0 no-change/);
  assert.doesNotMatch(runReport(state), /Retrospective/);
  // An amendment invalidates the admissions it leaves in place.
  state.job.planHash = hash('amended');
  assert.match(runReport(state), /`b` assessed 0 times\./);
  const evidence = [{ finding: 'F1' }];
  state.job.retro = {
    mistakes: [
      { what: 'Ported the parser twice.', cost: '3 claims.', prevention: 'Plan it.', evidence },
    ],
    cuts: [],
    improvements: [{ problem: 'p', proposal: 'q', target: 'checks', evidence }],
  };
  assert.match(
    runReport(state),
    /- Mistake: Ported the parser twice\. Cost: 3 claims\. Prevention/,
  );
  assert.match(runReport(state), /- Improve checks: p Proposal: q/);
});
