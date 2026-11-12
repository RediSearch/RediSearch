/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** What rejects a claim before anything runs, so a rejection costs no assessment. */
import {
  blankOnlyFiles,
  measureSize,
  filesOutside,
  parseDiff,
  sizeClass,
  type Size,
} from './diff.ts';
import { reviewedEvidence, type State } from './job.ts';
import { citedTests } from './plan.ts';
import type { Profile, Stage, Vcs } from './ports.ts';
import { checkRefs } from './refs.ts';

/**
 * A house rule, as far as a pattern can catch it. Each is a ratchet: a file may not have more
 * violations at the candidate than at the base, so existing code is never blamed and nothing new
 * slips in.
 */
export interface Rule {
  applies(path: string): boolean;
  count(text: string): number;
  message: string;
}

/** What `path` breaks of `rules` that it did not already break at `before`. */
export function ruleViolations(
  rules: Rule[],
  path: string,
  before: string | undefined,
  after: string,
) {
  return rules
    .filter((r) => r.applies(path) && r.count(after) > (before === undefined ? 0 : r.count(before)))
    .map((r) => `${path} ${r.message}`);
}

/**
 * Why the candidate cannot be the stage, and its size: everything that rejects a claim before
 * anything runs, so a rejection costs no assessment.
 */
export async function checkClaim<X extends object>(o: {
  root: string;
  vcs: Vcs;
  profile: Profile<X>;
  state: State<X>;
  stage: Stage;
  base: string;
  candidate: string;
}): Promise<{ problems: string[]; size: Size }> {
  const { root, vcs, profile, state, stage, base, candidate } = o;
  const { plan } = state.job;
  const generated = (path: string) => profile.classify(path) === 'generated';
  const files = parseDiff(await vcs.diff(base, candidate));
  const { paths } = plan.stages[stage];
  // Without rename detection a moved file names both its paths, so neither escapes the scope.
  const stray = filesOutside(
    files.map((f) => f.path).filter((f) => !generated(f)),
    paths,
  );
  if (stray.length)
    return {
      problems: [`it changes ${stray.join(', ')}, outside ${paths.join(', ')}; revert or amend`],
      size: { lines: 0, code: 0, files: 0 },
    };
  for (const f of files)
    f.texts = [await vcs.readFileAt(base, f.path), await vcs.readFileAt(candidate, f.path)];
  const size = measureSize(files, profile);
  const problems = blankOnlyFiles(files.filter((f) => !generated(f.path))).map(
    (path) => `${path} changes only blank lines; restore it`,
  );
  const tooBig = sizeClass(size.lines);
  if (tooBig?.label === 'XL') problems.push(`it adds ${size.lines} lines: ${tooBig.note}`);
  // Once reviewed, a stage's code may not grow: a fix pays for itself in deleted code.
  const floor = Math.min(...reviewedEvidence(state, stage).map((e) => e.size.code));
  if (size.code > floor)
    problems.push(
      `it adds ${size.code} net lines of code (tests aside), its reviewed version ${floor}: ` +
        'a fix must delete as much code as it adds',
    );
  for (const { path, texts } of files) {
    const [before, after] = texts!;
    if (after !== undefined) problems.push(...ruleViolations(profile.rules, path, before, after));
  }
  const tests = citedTests(plan, stage).map((t) => ({ path: t.path, test: t.name }));
  problems.push(
    ...(await checkRefs(root, tests)),
    ...(await missingFixes(vcs, state, stage, candidate)),
  );
  problems.push(
    ...((await profile.checkClaim?.({ root, vcs, plan, stage, base, candidate, files })) ?? []),
  );
  return { problems, size };
}

/**
 * The stage's findings closed as fixed whose fix the candidate lacks: it changes none of the files
 * the fix cites since the revision the finding was raised on.
 */
async function missingFixes(vcs: Vcs, state: State, stage: Stage, candidate: string) {
  const problems = [];
  for (const f of Object.values(state.findings)) {
    if (f.stage !== stage || f.closed?.resolution !== 'fixed') continue;
    const paths = (f.closed.evidence ?? []).map((r) => r.path);
    // A revision this checkout never had cannot be compared, and is not held against it.
    const changed = await vcs.diff(f.revision, candidate, { paths }).catch(() => '?');
    if (!changed)
      problems.push(
        `${f.id} was closed as fixed, but nothing it cites changed since ${f.revision.slice(0, 10)}`,
      );
  }
  return problems;
}
