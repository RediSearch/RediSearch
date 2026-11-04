/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { readFile } from 'node:fs/promises';
import { isAbsolute, join, normalize } from 'node:path';
import type { Ref } from './state.ts';

/**
 * Why each reference does not hold in the workspace at `root`: a missing file, a line past its
 * end, a test it does not define, or an unknown finding. Empty when every reference holds.
 */
export async function verify(
  root: string,
  refs: unknown,
  findings: Record<string, unknown> = {},
): Promise<string[]> {
  if (!Array.isArray(refs)) return ['evidence must be a list of references'];
  const problems = [];
  for (const ref of refs as Ref[]) {
    if (typeof ref !== 'object' || ref === null) {
      problems.push(`${JSON.stringify(ref)} is not a reference`);
      continue;
    }
    if ('finding' in ref) {
      if (!findings[ref.finding]) problems.push(`there is no finding ${ref.finding}`);
      continue;
    }
    const path = typeof ref?.path === 'string' ? normalize(ref.path) : '';
    if (!path || isAbsolute(path) || path.startsWith('..')) {
      problems.push(`${JSON.stringify(ref)} needs a path inside the workspace`);
      continue;
    }
    if (ref.line !== undefined && typeof ref.line !== 'number') {
      problems.push(`${path}: line must be a number`);
      continue;
    }
    if (ref.test !== undefined && typeof ref.test !== 'string') {
      problems.push(`${path}: test must be a name`);
      continue;
    }
    const text = await readFile(join(root, path), 'utf8').catch(() => undefined);
    if (text === undefined) problems.push(`${path} does not exist`);
    else if (ref.line && !(ref.line >= 1 && ref.line <= text.split('\n').length))
      problems.push(`${path} has no line ${ref.line}`);
    else if (ref.test && !defines(text, ref.test)) problems.push(`${path} defines no ${ref.test}`);
  }
  return problems;
}

/** Whether `text` defines the test `name`: its last path segment, as a whole word. */
export function defines(text: string, name: string): boolean {
  const word = name.split(/::|\./).at(-1)!.replace(/[^\w]/g, '');
  return !!word && new RegExp(`\\b${word}\\b`).test(text);
}

/**
 * A finding's location as `path:line`, whether it names the line, a range starting
 * there, or a symbol defined on it. A location that resolves to neither is returned as given.
 */
export async function anchor(root: string, location: string): Promise<string> {
  const at = location.indexOf(':');
  const [path, spot] = at < 0 ? [location, ''] : [location.slice(0, at), location.slice(at + 1)];
  const line = /^\d+/.exec(spot)?.[0];
  if (line) return `${path}:${line}`;
  const file = normalize(path);
  if (isAbsolute(file) || file.startsWith('..')) return location;
  const text = await readFile(join(root, file), 'utf8').catch(() => '');
  const name = spot.split(/::|\./).at(-1)!.replace(/[^\w]/g, '');
  const lines = text.split('\n');
  const kinds = 'fn|struct|enum|union|trait|type|const|static|def|class';
  const definition = new RegExp(`\\b(${kinds})\\s+${name}\\b`);
  const c = new RegExp(`^\\S.*\\b${name}\\s*\\(`);
  let found = name ? lines.findIndex((l) => definition.test(l)) : -1;
  if (found < 0 && name) found = lines.findIndex((l) => c.test(l));
  return found < 0 ? location : `${path}:${found + 1}`;
}
