/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** What a port of a C module to Rust adds: plan fields, the swap's completeness, its reviewers. */
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { FileChange } from '../core/diff.ts';
import { citedTests, type Plan } from '../core/plan.ts';
import type { Profile, Stage, Vcs } from '../core/ports.ts';
import type { Reviewer } from '../core/review.ts';
import { benchFilter, multiCratePaths, missingCargoTools } from './cargo.ts';
import { blockBodies, namedForArtifact, withoutTests, type Layout } from './rust.ts';

const SOURCES = /\.(c|h|cpp|cc|hpp|rs)$/;

export interface PortPlan {
  /** The C files being ported; the swap deletes them, headers included. */
  module: string[];
  /** One per function the module's headers declare, and the Rust the swap exports under its name. */
  symbols: { c: string; rust: string }[];
}

/**
 * Functions a C header declares: a name with a return type before it, either type words or a
 * macro call on the same line such as `arrayof(char**)`. Column-zero macro calls such as
 * `KHASH_MAP_INIT_INT64(...)` have none, and comments, typedefs and static functions are skipped.
 */
export function headerFunctions(header: string): string[] {
  // Comments become blank space, keeping lines, so prose in them never reads as a prototype.
  // `extern` is dropped, so a variable typed by a macro call, `extern arrayof(int*) x;`, does
  // not read as a function named for its macro.
  const code = header
    .replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, ' '))
    .replace(/\/\/[^\n]*/g, '')
    .replace(/^extern\s+/gm, '');
  const pattern =
    /^(?!typedef\b|static\b|#|\/\/|\/?\*|\s)(?:\w+\([^()]*\)(?:[ \t]*|[ \t]*\n[ \t]+)|(?:[\w*]+[\s*]+)+)(\w+)\s*\(/gm;
  return [...code.matchAll(pattern)].map((m) => m[1]);
}

/**
 * What a port's plan gets wrong: a module without sources and headers, symbols missing or
 * invented against the headers on the base branch, tests named for their artifact, filtered
 * benchmarks, missing Cargo tools, and paths spanning several crates.
 */
export async function checkPortPlan(
  plan: Plan<PortPlan>,
  stages: Profile<PortPlan>['stages'],
  o: { root: string; vcs: Vcs; base: string; workspace: string },
): Promise<string[]> {
  const errors: string[] = [];
  if (!plan.module?.some((p) => p?.endsWith?.('.c')) || !plan.module.some((p) => p.endsWith('.h')))
    errors.push('module needs the C files being ported: sources and headers');
  if (!plan.symbols?.length) errors.push('symbols needs every function the headers declare');
  for (const [i, s] of (plan.symbols ?? []).entries())
    if (![s?.c, s?.rust].every((v) => typeof v === 'string' && v.trim()))
      errors.push(`symbols[${i}] needs c and rust`);
  for (const t of citedTests(plan, stages.at(-1)!.name))
    if (namedForArtifact(t.name))
      errors.push(
        `${t.path}::${t.name} is named for its artifact; name it for the behavior it asserts`,
      );
  for (const { name } of stages) {
    const filter = benchFilter(
      plan.stages[name].checks.find((c) => c.kind === 'bench')?.command ?? '',
    );
    if (filter) errors.push(`${name} bench check filters the benchmarks (${filter}); run them all`);
  }
  if (errors.length) return errors;

  errors.push(...(await missingCargoTools(o.root, o.workspace)));
  const declared = new Set<string>();
  for (const path of plan.module.filter((p) => p.endsWith('.h'))) {
    const header = await o.vcs.readFileAt(o.base, path);
    if (header === undefined) errors.push(`${path} is not on ${plan.baseBranch}`);
    else for (const name of headerFunctions(header)) declared.add(name);
  }
  const mapped = new Set(plan.symbols.map((s) => s.c));
  const unmapped = [...declared].filter((name) => !mapped.has(name));
  const unknown = [...mapped].filter((name) => !declared.has(name));
  if (unmapped.length) errors.push(`symbols leaves out ${unmapped.join(', ')}`);
  if (unknown.length) errors.push(`symbols names undeclared ${unknown.join(', ')}`);
  const files = await o.vcs.listFiles(o.base);
  for (const { name } of stages) errors.push(...multiCratePaths(plan.stages[name].paths, files));
  return errors;
}

/** The plan's symbols as reviewers and PRs read them. */
export const describePortPlan = (
  plan: Plan<PortPlan>,
  swap: Stage,
) => `### What replaces each C function

The C being ported, ${plan.module.map((p) => `\`${p}\``).join(', ')}, is in the working tree until \`${swap}\` deletes it.

${plan.symbols.map((s) => `- \`${s.c}\`: \`${s.rust}\``).join('\n')}`;

/**
 * What the swap leaves undone: a Rust replacement or generated export missing, a module file
 * left, an export no production code calls, or Rust outside the FFI crate without a mutants check.
 * A C definition left elsewhere fails to link against the Rust export of its name. The candidate
 * is the clean checkout at `root`.
 */
export async function checkSwap(
  layout: Layout,
  o: {
    root: string;
    vcs: Vcs;
    plan: Plan<PortPlan>;
    stage: Stage;
    candidate: string;
    files: FileChange[];
  },
): Promise<string[]> {
  const problems: string[] = [];
  const tracked = await o.vcs.listFiles(o.candidate);
  const read = fileReader(o.root);
  // Exports are not mutation candidates, but other Rust the swap adds is.
  const pure = o.files.map((f) => f.path).filter((f) => layout.isPure(f) && !layout.isTest(f));
  if (pure.length && !o.plan.stages[o.stage].checks.some((c) => c.kind === 'mutants'))
    problems.push(
      `${o.stage} changes Rust outside its FFI crate (${pure.join(', ')}): add a mutants check`,
    );
  const headers = tracked.filter((f) => f.startsWith(layout.headers));
  for (const s of o.plan.symbols) {
    if (!(await definesRust(layout, tracked, read, s.rust)))
      problems.push(`${s.rust}, which replaces ${s.c}, is not defined`);
    if (!(await anyFileMatches(headers, read, wholeWord(s.c))))
      problems.push(`no generated header exports ${s.c}`);
  }
  for (const path of o.plan.module)
    if (tracked.includes(path))
      problems.push(`${path} still exists; the swap deletes the module, headers included`);
  for (const name of await orphanedExports(layout, o.files, tracked, read))
    problems.push(
      `${name} is exported, but no production code calls it: delete it, and point any test ` +
        'that calls it at the Rust API',
    );
  return problems;
}

type Read = (path: string) => Promise<string>;

/** Reads files of the checkout at `root` once each. */
export function fileReader(root: string): Read {
  const texts = new Map<string, Promise<string>>();
  return (path) => {
    if (!texts.has(path))
      texts.set(
        path,
        readFile(join(root, path), 'utf8').catch(() => ''),
      );
    return texts.get(path)!;
  };
}

const wholeWord = (pattern: string) => new RegExp(`(?<!\\w)(?:${pattern})(?!\\w)`);

async function anyFileMatches(paths: string[], read: Read, pattern: RegExp) {
  for (const path of paths) if (pattern.test(await read(path))) return true;
  return false;
}

/**
 * Whether the crate a Rust path starts with defines each item after it, so `geo::RadiusFilter::new`
 * needs `fn new` in a file of the `geo` crate that implements `RadiusFilter`, not any `fn new`.
 */
async function definesRust(layout: Layout, tracked: string[], read: Read, path: string) {
  const [crate, ...items] = path.split('::');
  const named = new RegExp(`^name = "${crate}"`, 'm');
  let manifest: string | undefined;
  for (const f of tracked)
    if (f.startsWith(layout.workspace) && f.endsWith('Cargo.toml') && named.test(await read(f))) {
      manifest = f;
      break;
    }
  if (!manifest || !items.length) return false;
  const dir = manifest.replace(/Cargo\.toml$/, '');
  const inCrate = tracked.filter((f) => f.startsWith(dir));
  const files = async (pattern: string) => {
    const hits = [];
    for (const f of inCrate) if (wholeWord(pattern).test(await read(f))) hits.push(f);
    return hits;
  };
  for (const item of items.slice(0, -1))
    if (!(await files(`(struct|enum|trait|type|mod) ${item}`)).length) return false;
  const [owner, item] = [items.at(-2), items.at(-1)!];
  const defs = await files(`(fn|struct|enum|trait|type|const|static|mod) ${item}`);
  if (!owner) return defs.length > 0;
  // A method lives in an impl block of its type; a function in its module's file or block.
  const header = new RegExp(
    `^\\s*(impl\\b[^{;]*\\b${owner}\\b[^{;]*|(pub(\\([^)]*\\))?\\s+)?mod\\s+${owner}\\s*)\\{`,
    'gm',
  );
  const defined = new RegExp(`\\b(fn|const|type|struct|enum)\\s+${item}\\b`);
  for (const hit of defs) {
    if (hit.endsWith(`/${owner}.rs`) || hit.endsWith(`/${owner}/mod.rs`)) return true;
    if (blockBodies(await read(hit), header).some((b) => defined.test(b))) return true;
  }
  return false;
}

/**
 * C ABI exports without a production caller: those the stage adds, and those whose caller it
 * deleted, that no C or Rust code outside tests, generated headers and comments mentions. An
 * export only tests call keeps C ABI alive for nothing; its tests belong on the Rust API.
 */
export async function orphanedExports(
  layout: Layout,
  files: FileChange[],
  tracked: string[],
  read: Read,
): Promise<string[]> {
  const touched = new Set(files.flatMap((f) => f.removed.join('\n').match(/\w+/g) ?? []));
  for (const f of files)
    for (const m of f.added.join('\n').matchAll(/#\[unsafe\(no_mangle\)\][^{;]*?\bfn\s+(\w+)/g))
      touched.add(m[1]);
  const exported = new Set<string>();
  for (const path of tracked.filter((f) => f.endsWith('.rs'))) {
    const lines = (await read(path)).split('\n');
    for (const [i, line] of lines.entries())
      if (line.includes('#[unsafe(no_mangle)]'))
        for (const m of lines
          .slice(i, i + 5)
          .join('\n')
          .matchAll(/\bfn\s+(\w+)/g))
          if (touched.has(m[1])) exported.add(m[1]);
  }
  const callers = tracked.filter(
    (f) => SOURCES.test(f) && !f.startsWith(layout.headers) && !layout.isTest(f),
  );
  const result = [];
  for (const name of exported) {
    const named = wholeWord(name);
    const call = (line: string) =>
      named.test(line) && !/^(\/\/|\/?\*)/.test(line.trim()) && !/\bfn\s/.test(line);
    let used = false;
    for (const path of callers) {
      const text = await read(path);
      if (!named.test(text)) continue;
      // A Rust file's own `#[cfg(test)]` items are tests too.
      if ((path.endsWith('.rs') ? withoutTests(text) : text).split('\n').some(call)) {
        used = true;
        break;
      }
    }
    if (!used) result.push(name);
  }
  return result;
}

/**
 * The reviewers of a C-to-Rust port: correctness against the plan, and idiomatic Rust checked
 * against `guides` rule by rule, citing `ruleFiles`.
 */
export const portReviewers = (guides: string, ruleFiles: string): Reviewer[] => [
  {
    name: 'Correctness',
    instructions: `Review correctness against the plan: its contract, its divergences from C, what
replaces each C function, the stage's deliverable and the recorded interfaces. C behavior the
contract does not list may change, provided the plan lists the divergence.
Look for inputs or states that break a contract item, behave differently from C without a
listed divergence, crash on user input or are undefined behavior; for a deliverable that is
incomplete or departs from the recorded interfaces; and for build scripts, lint exclusions and
CI paths the change breaks. Prove each behavior defect with a failing test as its repro.
Check that each contract item has tests named for the behavior they assert, and that an item
holding over a range of inputs, such as a conversion, a bound or an ordering, has a property
test over generated inputs rather than a few hand-picked values; cite the repository rule.
Audit every panic the safe API can reach: \`assert!\`, \`expect\`, \`unwrap\`, indexing, and calls
into helpers that panic. For each, find a value a constructor or parser accepts that reaches it;
a panic on an accepted value is a crash, and its repro is a test calling the method with it.
At the FFI boundary, check every pointer for three things separately, because one does not imply
another: initialization (writable memory may be uninitialized, so write through the raw pointer
instead of making a reference to it), aliasing (no reference may overlap another live one or
outlive what it points to), and ownership (who frees it, with which allocator, exactly once).`,
  },
  {
    name: 'Idiomatic Rust',
    instructions: `Review only the Rust: idiomatic, small, readable code, documentation and tests.
Make two passes. First bugs and bloat, which matter most: look hardest for code the stage does
not need, and give each such finding a deletion patch. Then design and house style: go through
${guides}, checking the diff against each rule rather than waiting for one to stand out. Cite the
line of the rule a finding breaks, in ${ruleFiles}. Matters of taste cite no rule.
A property test must state an invariant the implementation does not spell out, such as a round
trip, an ordering, or agreement with a simpler model. One that restates the implementation's
expression proves nothing: give it a deletion patch.`,
  },
];
