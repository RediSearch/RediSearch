/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * Reading Rust sources by pattern: their tests, their blocks, and the house rules any port's Rust
 * follows.
 */
import type { Rule } from '../core/gates.ts';

const TEST_GATE = '#[cfg(test)]';
// Strings, char literals and comments are skipped whole, so a gate or brace in them does not count.
const LITERAL = /b?r(#*)"[\s\S]*?"\1|"(\\.|[^"\\])*"|'(\\.|[^'\\])'|\/\/[^\n]*|\/\*[\s\S]*?\*\//y;

/**
 * The code outside `#[cfg(test)]` items. Each gated item ends at its first top-level `;` or at the
 * `}` closing its first `{`; production code may follow it.
 */
export function withoutTests(text: string): string {
  let kept = '';
  let from = 0;
  let gate = -1;
  let depth = 0;
  for (let i = 0; i < text.length; i++) {
    LITERAL.lastIndex = i;
    if (LITERAL.test(text)) {
      i = LITERAL.lastIndex - 1;
      continue;
    }
    if (gate < 0) {
      if (!text.startsWith(TEST_GATE, i)) continue;
      kept += text.slice(from, i);
      gate = i;
      depth = 0;
      i += TEST_GATE.length - 1;
    } else if (text[i] === '{') depth++;
    else if ((text[i] === '}' && --depth === 0) || (text[i] === ';' && depth === 0)) {
      gate = -1;
      from = i + 1;
    }
  }
  return gate < 0 ? kept + text.slice(from) : kept;
}
/**
 * The text inside the block each match of `header`, a global pattern ending at its `{`, opens.
 * Literals and comments are skipped, so their braces do not count.
 */
export function blockBodies(text: string, header: RegExp): string[] {
  const found: string[] = [];
  for (const m of text.matchAll(header)) {
    const start = m.index + m[0].length;
    let depth = 1;
    for (let i = start; i < text.length; i++) {
      LITERAL.lastIndex = i;
      if (LITERAL.test(text)) {
        i = LITERAL.lastIndex - 1;
        continue;
      }
      if (text[i] === '{') depth++;
      else if (text[i] === '}' && --depth === 0) {
        found.push(text.slice(start, i));
        break;
      }
    }
  }
  return found;
}

export const countMatches = (text: string, pattern: RegExp) => text.match(pattern)?.length ?? 0;

/**
 * Whether a test is named for the artifact it belongs to rather than the behavior it asserts,
 * judged by whole words, so `testGeoFilterABI` counts and `capabilities` does not.
 */
export function namedForArtifact(name: string): boolean {
  const words = name
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .toLowerCase()
    .split('_');
  return words.some((w) => ['contract', 'boundary', 'abi', 'compat'].includes(w));
}

/** Tests named for an artifact: Rust test functions, Python flow tests and C++ tests. */
export function artifactNamedTests(text: string): number {
  const names = [
    ...text.matchAll(
      /#\[(\w+::)*(test|rstest)\b[^\]]*\]\s*(#\[[^\]]*\]\s*)*(async\s+)?fn\s+(\w+)/g,
    ),
  ].map((m) => m[5]);
  for (const m of text.matchAll(/\bdef\s+(test\w*)\s*\(/g)) names.push(m[1]);
  for (const m of text.matchAll(/\bTEST(_F)?\s*\(\s*\w+\s*,\s*(\w+)\s*\)/g)) names.push(m[2]);
  return names.filter(namedForArtifact).length;
}

/** `debug_assert!`s outside the named assertion helpers that are meant to wrap them. */
export function bareAsserts(text: string): number {
  let helper = false;
  let count = 0;
  for (const line of text.split('\n')) {
    const fn = line.match(/\bfn\s+(\w+)/)?.[1];
    if (fn) helper = /^(debug_)?assert_/.test(fn);
    if (/\bdebug_assert(_eq|_ne)?!\s*\(/.test(line) && !helper) count++;
  }
  return count;
}

/** `# Safety` sections whose pre-conditions are not a numbered list. */
export function unnumberedSafetySections(text: string): number {
  const sections = text.split(/^\s*\/\/\/\s*# Safety\s*$/m).slice(1);
  return sections.filter((rest) => {
    const docs = rest.split('\n').filter(Boolean);
    const body = docs.slice(
      0,
      docs.findIndex((l) => !l.trim().startsWith('///')),
    );
    return !body.some((l) => /^\s*\/\/\/\s*\d+\./.test(l));
  }).length;
}

/** Where a workspace's Rust lives, and which of it is tests, bindings or a pure crate. */
export interface Layout {
  /** The Cargo workspace. */
  workspace: string;
  /** The directory of the FFI layout. */
  ffi: string;
  /** The C headers generated from the FFI layout. */
  headers: string;
  isTest(path: string): boolean;
  /** Rust outside the FFI crates and the C bindings: what a port's pure crate is. */
  isPure(path: string): boolean;
}

/** The house rules of any Rust a port adds, as far as a pattern catches them. */
export function rustHouseRules(layout: Layout): Rule[] {
  const rust = (path: string) => path.startsWith(layout.workspace) && path.endsWith('.rs');
  const production = (path: string) => rust(path) && !layout.isTest(path);
  const ffi = (path: string) => production(path) && path.startsWith(layout.ffi);
  return [
    {
      applies: production,
      count: (text) => countMatches(withoutTests(text), /\.unwrap\(\)/g),
      message: 'adds `unwrap()` outside tests; use `expect("<the invariant it relies on>")`',
    },
    {
      applies: ffi,
      count: (text) => countMatches(text, /unsafe\s*\{\s*(&\s*(mut\s+)?\*|\(\s*\*)/g),
      message: 'dereferences a raw pointer with `*`; use `as_ref()`, `as_mut()` or `write()`',
    },
    {
      applies: ffi,
      count: (text) => bareAsserts(withoutTests(text)),
      message:
        'adds a bare `debug_assert!`; check FFI pre-conditions with a named assertion helper',
    },
    {
      applies: production,
      count: (text) => unnumberedSafetySections(withoutTests(text)),
      message: 'adds a `# Safety` section whose pre-conditions are not numbered',
    },
    {
      applies: production,
      count: (text) => countMatches(text, /Result<[^;{]*,\s*&'static str\s*>/g),
      message: "returns a `&'static str` error; use an error enum deriving `thiserror::Error`",
    },
    {
      applies: (path) => layout.isPure(path) && !layout.isTest(path),
      count: (text) => countMatches(text, /\bextern\s+"C"|#\[(unsafe\()?no_mangle\b/g),
      message: 'adds C ABI items to a pure crate; they belong in its FFI crate',
    },
    {
      // Declarations only: tests may still define stubs for C symbols they do not exercise.
      applies: (path) => layout.isPure(path) && layout.isTest(path),
      count: (text) => countMatches(text, /\bextern\s+"C"\s*\{/g),
      message: "declares C functions in a pure crate's tests; test the C boundary in the FFI crate",
    },
  ];
}
