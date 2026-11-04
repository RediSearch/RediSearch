/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

const TEST_GATE = '#[cfg(test)]';

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
export function violations(rules: Rule[], path: string, before: string | undefined, after: string) {
  return rules
    .filter((r) => r.applies(path) && r.count(after) > (before === undefined ? 0 : r.count(before)))
    .map((r) => `${path} ${r.message}`);
}

/**
 * The code outside `#[cfg(test)]` items. Each gated item ends at its first top-level `;` or at the
 * `}` closing its first `{`; production code may follow it.
 */
export function nonTest(text: string): string {
  let kept = '';
  let rest = text;
  for (let at = rest.indexOf(TEST_GATE); at !== -1; at = rest.indexOf(TEST_GATE)) {
    kept += rest.slice(0, at);
    let depth = 0;
    let end = rest.length;
    // Strings, char literals and comments are skipped whole, so their braces do not count.
    const skip = /b?r(#*)"[\s\S]*?"\1|"(\\.|[^"\\])*"|'(\\.|[^'\\])'|\/\/[^\n]*|\/\*[\s\S]*?\*\//y;
    for (let i = at + TEST_GATE.length; i < rest.length; i++) {
      skip.lastIndex = i;
      if (skip.test(rest)) {
        i = skip.lastIndex - 1;
        continue;
      }
      if (rest[i] === '{') depth++;
      else if (rest[i] === '}' && --depth === 0) end = i + 1;
      else if (rest[i] === ';' && depth === 0) end = i + 1;
      if (end !== rest.length) break;
    }
    rest = rest.slice(end);
  }
  return kept + rest;
}
export const matches = (text: string, pattern: RegExp) => text.match(pattern)?.length ?? 0;

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
export function artifactTests(text: string): number {
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
export function unnumbered(text: string): number {
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
