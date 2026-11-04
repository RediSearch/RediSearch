/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * Everything the plugin knows about RediSearch: its layout, tools, CI and house rules. The other
 * modules hold only what any port of C to Rust in a Cargo workspace shares.
 */
import type { Stage } from './plan.ts';
import { artifactTests, bareAsserts, matches, nonTest, unnumbered, type Rule } from './rules.ts';

/** The Cargo workspace the Rust lives in. */
export const WORKSPACE = 'src/redisearch_rs/';
/** C headers cbindgen generates from the FFI crates. */
export const HEADERS = `${WORKSPACE}headers/`;
const FFI = `${WORKSPACE}c_entrypoint/`;

export const isTest = (path: string) => /(^|\/)(tests|benches)\//.test(path);
export const isGenerated = (path: string) =>
  path.startsWith(HEADERS) || path.endsWith('Cargo.lock');
const rust = (path: string) => path.startsWith(WORKSPACE) && path.endsWith('.rs');
/** Rust outside the FFI crates and the C bindings: what a port's pure crate is. */
export const isPure = (path: string) =>
  rust(path) && !/^src\/redisearch_rs\/(c_entrypoint|ffi|c_wrappers|redis_mock)\//.test(path);
const production = (path: string) => rust(path) && !isTest(path);
const ffi = (path: string) => production(path) && path.startsWith(FFI);

/** Rule files: a reviewer finding that cites one blocks. */
export const isRule = (path: string) =>
  /^(\.claude\/)?\.?skills\//.test(path) || /(^|\/)(CLAUDE|AGENTS)\.md$/.test(path);
export const RULE_FILES = '`.skills/`, CLAUDE.md or AGENTS.md';
/** What the Idiomatic Rust reviewer goes through rule by rule. */
export const GUIDES =
  'the generated-code smells and the house rules in .skills/rust-review row by row, and ' +
  '.skills/rust-tests-guidelines and .skills/rust-docs-guidelines';

// Miri runs in every stage, because nothing tells reliably whether a change reaches `unsafe` code.
// cargo mutants skips `unsafe extern` functions, so only a swap that adds other Rust needs it.
const LINT = ['make lint', 'cargo hakari generate --diff', 'cargo hakari manage-deps --dry-run'];
/** The checks each stage's plan must have, by kind, and what each must run. */
export const CHECKS: Record<Stage, Record<string, string[]>> = {
  rust: {
    test: [],
    miri: ['miri'],
    mutants: ['cargo mutants'],
    lint: LINT,
    bench: ['cargo bench'],
  },
  swap: { test: [], miri: ['miri'], lint: LINT },
};

/** What a reproduction may run: a `*` stands for a word. */
export const RUNNERS = ['cargo test', 'cargo nextest run', './build.sh RUN_*'];

/** The check that aggregates the others: the only CI result that decides. */
export const CI_GATE = 'pr-validation';
/** Checks that never block, even before the gate reports. */
export const ADVISORY = /sonar|codecov/i;

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

/** The house rules maintainers raise on every port (.skills/rust-review § 3k). */
export const RULES: Rule[] = [
  {
    applies: production,
    count: (text) => matches(nonTest(text), /\.unwrap\(\)/g),
    message: 'adds `unwrap()` outside tests; use `expect("<the invariant it relies on>")`',
  },
  {
    applies: ffi,
    count: (text) => matches(text, /unsafe\s*\{\s*(&\s*(mut\s+)?\*|\(\s*\*)/g),
    message: 'dereferences a raw pointer with `*`; use `as_ref()`, `as_mut()` or `write()`',
  },
  {
    applies: ffi,
    count: (text) => bareAsserts(nonTest(text)),
    message: 'adds a bare `debug_assert!`; check FFI pre-conditions with a named assertion helper',
  },
  {
    applies: production,
    count: (text) => unnumbered(nonTest(text)),
    message: 'adds a `# Safety` section whose pre-conditions are not numbered',
  },
  {
    applies: production,
    count: (text) => matches(text, /RedisModule_(Alloc|Calloc|Realloc)\b/g),
    message: 'calls the Redis allocator directly; allocate with `Box`, the global allocator',
  },
  {
    applies: production,
    count: (text) => matches(text, /Result<[^;{]*,\s*&'static str\s*>/g),
    message: "returns a `&'static str` error; use an error enum deriving `thiserror::Error`",
  },
  {
    applies: (path) => rust(path) && path !== `${FFI}redisearch_rs/src/lib.rs`,
    count: (text) => matches(text, /#\[global_allocator\]/g),
    message: 'adds an allocator; Miri already catches leaks and double frees',
  },
  {
    applies: (path) => rust(path) || /^tests\/(pytests|cpptests|ctests)\//.test(path),
    count: artifactTests,
    message: 'adds a test named for its artifact (contract, ABI, boundary); name the behavior',
  },
  {
    applies: (path) => isPure(path) && !isTest(path),
    count: (text) => matches(text, /\bextern\s+"C"|#\[(unsafe\()?no_mangle\b/g),
    message: 'adds C ABI items to a pure crate; they belong in its `c_entrypoint/*_ffi` crate',
  },
  {
    // Declarations only: tests may still define stubs for C symbols they do not exercise.
    applies: (path) => isPure(path) && isTest(path),
    count: (text) => matches(text, /\bextern\s+"C"\s*\{/g),
    message: "declares C functions in a pure crate's tests; test the C boundary in the FFI crate",
  },
  {
    // An existing crate rolls the lints out on its own, but none may drop one.
    applies: (path) => /^src\/redisearch_rs\/(.+\/)?[^/]+\/src\/lib\.rs$/.test(path),
    count: (text) => CRATE_LINTS.filter((lint) => !text.includes(lint)).length,
    message: `lacks lints of the crate lint block: ${CRATE_LINTS.join(', ')}`,
  },
];
