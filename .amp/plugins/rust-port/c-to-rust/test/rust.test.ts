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
import { blockBodies, namedForArtifact, withoutTests } from '../rust.ts';

test('a test name counts as an artifact name only by whole words', () => {
  assert.deepEqual(
    [
      'testGeoFilterABI',
      'geo_filter_boundary_contract',
      'QuantileContract',
      'capabilities',
      'stable_order',
    ].map(namedForArtifact),
    [true, true, true, false, false],
  );
});

test('braces inside strings, chars and comments do not end a test item', () => {
  const text = `#[cfg(test)]
mod tests {
    // a stray } in a comment
    fn t<'a>(s: &'a str) { assert_eq!("{", s); let _ = '{'; }
}
pub fn new() -> u8 { y.unwrap() }`;
  assert.equal(withoutTests(text).trim(), 'pub fn new() -> u8 { y.unwrap() }');
});

test('a raw string in a test item does not hide the production code after it', () => {
  const text = `#[cfg(test)]
mod tests {
    const FIXTURE: &str = r#"{ "a": "}" "#;
}
pub fn new() -> u8 { y.unwrap() }`;
  assert.equal(withoutTests(text).trim(), 'pub fn new() -> u8 { y.unwrap() }');
});

test('a test gate in a comment or string does not hide the production code after it', () => {
  const text = `// Put helpers behind #[cfg(test)]
const NOTE: &str = "#[cfg(test)]";
pub fn new() -> u8 { y.unwrap() }`;
  assert.equal(withoutTests(text), text);
});

test('a block body ends at its own closing brace, whatever literals inside it hold', () => {
  const text = `impl Other {\n    fn new() {}\n}\nimpl RadiusFilter {\n    const OPEN: &str = "}";\n    fn reaches() {}\n}\n`;
  assert.deepEqual(
    blockBodies(text, /^impl\b[^{;]*\bRadiusFilter\b[^{;]*\{/gm).map((b) => b.trim()),
    ['const OPEN: &str = "}";\n    fn reaches() {}'],
  );
});
