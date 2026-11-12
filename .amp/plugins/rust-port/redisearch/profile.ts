/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** Everything specific to RediSearch: its layout, checks, CI and house rules. */
import type { CI } from '../core/github.ts';
import type { Rule } from '../core/gates.ts';
import type { Profile } from '../core/ports.ts';
import { multiCratePaths, cargoJudges, isTestCommand } from '../c-to-rust/cargo.ts';
import {
  describePortPlan,
  checkPortPlan,
  portReviewers,
  checkSwap,
  type PortPlan,
} from '../c-to-rust/port.ts';
import {
  artifactNamedTests,
  rustHouseRules,
  countMatches,
  withoutTests,
  type Layout,
} from '../c-to-rust/rust.ts';

/** The Cargo workspace the Rust lives in. */
const WORKSPACE = 'src/redisearch_rs/';
const RULE_FILES = '`.skills/`, CLAUDE.md or AGENTS.md';
/** What the Idiomatic Rust reviewer goes through rule by rule. */
const GUIDES =
  'the generated-code smells and the house rules in .skills/rust-review row by row, and ' +
  '.skills/rust-tests-guidelines and .skills/rust-docs-guidelines';
/** How much slower than C, in percent, Rust may be in any benchmark. */
const SLOWDOWN_PERCENT = 5;
const RELEASE_NOTES = /^- \[([ x])\] This PR (requires|does not require) release notes/gim;

// Miri runs in every stage, because nothing tells reliably whether a change reaches `unsafe` code.
// cargo mutants skips `unsafe extern` functions, so only a swap that adds other Rust needs it.
// CI's lint job also checks formatting, which `make lint` does not.
const LINT = [
  'make lint',
  'make fmt CHECK=1',
  'cargo hakari generate --diff',
  'cargo hakari manage-deps --dry-run',
];

/** Lints a new crate enables for itself until the workspace does (.skills/port-c-module). */
export const CRATE_LINTS = [
  'clippy::unwrap_used',
  'clippy::dbg_macro',
  'clippy::todo',
  'clippy::unimplemented',
  'clippy::print_stdout',
  'clippy::panic',
  'clippy::unnecessary_safety_comment',
  'clippy::ptr_as_ptr',
  'clippy::borrow_as_ptr',
  'clippy::ref_as_ptr',
  'clippy::indexing_slicing',
];

const isTest = (path: string) => /(^|\/)(tests|benches)\//.test(path);
const rust = (path: string) => path.startsWith(WORKSPACE) && path.endsWith('.rs');
export const layout: Layout = {
  ...{ workspace: WORKSPACE, headers: `${WORKSPACE}headers/`, ffi: `${WORKSPACE}c_entrypoint/` },
  isTest,
  isPure: (path) =>
    rust(path) && !/^src\/redisearch_rs\/(c_entrypoint|ffi|c_wrappers|redis_mock)\//.test(path),
};

/** The check that aggregates the others, and the checks that never block. */
export const ci: CI = { gate: 'pr-validation', advisory: /sonar|codecov/i };

/** What the PR template's release-notes check, which `pr-validation` does not include, rejects. */
export function releaseNoteErrors(body: string): string[] {
  const boxes = [...body.matchAll(RELEASE_NOTES)];
  const kinds = new Set(boxes.map((b) => b[2].toLowerCase()));
  const ticked = boxes.filter((b) => b[1].toLowerCase() === 'x').length;
  return kinds.size === 2 && ticked === 1
    ? []
    : ['needs both release-notes boxes of the PR template, exactly one of them ticked'];
}

/** The house rules maintainers raise on every port (.skills/rust-review § 3k). */
export const RULES: Rule[] = [
  ...rustHouseRules(layout),
  {
    applies: (path) => rust(path) && !isTest(path),
    count: (text) => countMatches(text, /RedisModule_(Alloc|Calloc|Realloc)\b/g),
    message: 'calls the Redis allocator directly; allocate with `Box`, the global allocator',
  },
  {
    applies: (path) => rust(path) && path !== `${layout.ffi}redisearch_rs/src/lib.rs`,
    count: (text) => countMatches(text, /#\[global_allocator\]/g),
    message: 'adds an allocator; Miri already catches leaks and double frees',
  },
  {
    applies: (path) => rust(path) || /^tests\/(pytests|cpptests|ctests)\//.test(path),
    count: artifactNamedTests,
    message: 'adds a test named for its artifact (contract, ABI, boundary); name the behavior',
  },
  {
    // An existing crate rolls the lints out on its own, but none may drop one.
    applies: (path) => /^src\/redisearch_rs\/(.+\/)?[^/]+\/src\/lib\.rs$/.test(path),
    count: (text) => CRATE_LINTS.filter((lint) => !text.includes(lint)).length,
    message: `lacks lints of the crate lint block: ${CRATE_LINTS.join(', ')}`,
  },
];

const RUNNERS = ['cargo test', 'cargo nextest run', './build.sh RUN_*'];

/** A port lands as two stacked PRs: the Rust, then the swap that deletes the C. */
export const profile: Profile<PortPlan> = {
  name: 'rust-port',
  stages: [
    {
      name: 'rust',
      checks: {
        test: [],
        miri: ['miri'],
        mutants: ['cargo mutants'],
        lint: LINT,
        bench: ['cargo bench'],
      },
    },
    { name: 'swap', checks: { test: [], miri: ['miri'], lint: LINT } },
  ],
  languages: { from: 'C', to: 'Rust' },
  classify: (path) =>
    /(^|\/)(Cargo\.toml|build\.rs)$/.test(path)
      ? 'build'
      : /^(\.claude\/)?\.?skills\//.test(path) || /(^|\/)(CLAUDE|AGENTS)\.md$/.test(path)
        ? 'rule'
        : path.startsWith(layout.headers) || path.endsWith('Cargo.lock')
          ? 'generated'
          : isTest(path)
            ? 'test'
            : 'source',
  stripTests: (path, text) => (path.endsWith('.rs') ? withoutTests(text) : undefined),
  disabledTestPattern: /#\[(ignore|cfg)\b/,
  rules: RULES,
  ruleFiles: RULE_FILES,
  testRunners: RUNNERS,
  isTestCommand: (command) => isTestCommand(RUNNERS, command),
  judges: cargoJudges(WORKSPACE, SLOWDOWN_PERCENT),
  reviewers: portReviewers(GUIDES, RULE_FILES),
  checkPlan: async (plan, o) => [
    ...Object.entries(plan.stages).flatMap(([s, { body }]) =>
      releaseNoteErrors(body).map((e) => `${s} body ${e}`),
    ),
    ...(await checkPortPlan(plan, profile.stages, { ...o, workspace: WORKSPACE })),
  ],
  checkClaim: async (o) => [
    ...multiCratePaths(o.plan.stages[o.stage].paths, await o.vcs.listFiles(o.candidate)),
    ...(o.stage === 'swap' ? await checkSwap(layout, o) : []),
  ],
  describePlan: (plan) => describePortPlan(plan, 'swap'),
};
