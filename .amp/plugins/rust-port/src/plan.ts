/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import { execa } from 'execa';
import { criterion, type Check } from './checks.ts';
import { CHECKS } from './redisearch.ts';
import { namedForArtifact } from './rules.ts';

/** A port lands as two stacked PRs: the Rust, then the swap that deletes the C. */
export const STAGES = ['rust', 'swap'] as const;
export type Stage = (typeof STAGES)[number];

export const POLICY = '.amp/plugins/rust-port/policy.md';

/** A test named as evidence: `name` is defined in the file at `path` from `stage` on. */
export interface Test {
  path: string;
  name: string;
  stage: Stage;
}

export interface Plan {
  repository: string;
  remote: string;
  baseBranch: string;
  /** Stage branches are `<branchPrefix>-<stage>`. */
  branchPrefix: string;
  /** The C files being ported; `swap` deletes them, headers included. */
  module: string[];
  /** Behavior callers rely on, and the tests that pin it. */
  contract: { behavior: string; tests: Test[] }[];
  /** Where the Rust behaves differently from the C, for a human to accept on the PRs. */
  divergences: { behavior: string; c: string; rust: string; why: string; test: Test }[];
  /** One per function the module's headers declare, and the Rust that `swap` exports under its name. */
  symbols: { c: string; rust: string }[];
  structure: string;
  interfaces: string;
  stages: Record<
    Stage,
    { deliverable: string; paths: string[]; checks: Check[]; title: string; body: string }
  >;
}

/** The tests the plan cites up to `stage`: a stage's candidate must contain them and run them. */
export const cited = (plan: Plan, stage: Stage): Test[] =>
  [...plan.contract.flatMap((c) => c.tests), ...plan.divergences.map((d) => d.test)].filter(
    (t) => STAGES.indexOf(t.stage) <= STAGES.indexOf(stage),
  );

const blank = (text: unknown) => typeof text !== 'string' || !text.trim();

function testErrors(t: Test | undefined, what: string): string[] {
  if (blank(t?.path) || blank(t?.name) || !STAGES.includes(t?.stage as Stage))
    return [`${what} needs a test: { path, name, stage }, stage one of ${STAGES.join(', ')}`];
  if (namedForArtifact(t!.name))
    return [`${what} names ${t!.name} for its artifact; name it for the behavior it asserts`];
  return [];
}

export function validate(plan: Plan): string[] {
  const errors = (['remote', 'baseBranch', 'branchPrefix', 'structure', 'interfaces'] as const)
    .filter((field) => blank(plan?.[field]))
    .map((field) => `${field} is required`);
  if (!/^[\w.-]+\/[\w.-]+$/.test(plan?.repository)) errors.push('repository must be owner/name');
  if (!plan.module?.some((p) => p?.endsWith?.('.c')) || !plan.module.some((p) => p.endsWith('.h')))
    errors.push('module needs the C files being ported: sources and headers');
  if (!plan.contract?.length) errors.push('contract needs at least one item');
  for (const [i, c] of (plan.contract ?? []).entries()) {
    if (blank(c?.behavior)) errors.push(`contract[${i}] needs a behavior`);
    if (!c?.tests?.length) errors.push(`contract[${i}] needs the tests that pin it`);
    for (const t of c?.tests ?? []) errors.push(...testErrors(t, `contract[${i}]`));
  }
  if (!Array.isArray(plan.divergences)) errors.push('divergences is required, even if empty');
  for (const [i, d] of (plan.divergences ?? []).entries()) {
    if ([d?.behavior, d?.c, d?.rust, d?.why].some(blank))
      errors.push(`divergences[${i}] needs behavior, c, rust and why`);
    errors.push(...testErrors(d?.test, `divergences[${i}]`));
  }
  if (!plan.symbols?.length) errors.push('symbols needs every function the headers declare');
  for (const [i, s] of (plan.symbols ?? []).entries())
    if ([s?.c, s?.rust].some(blank)) errors.push(`symbols[${i}] needs c and rust`);
  for (const stage of STAGES) {
    const s = plan.stages?.[stage];
    if (blank(s?.deliverable)) errors.push(`${stage} needs a deliverable`);
    if (!s?.paths?.length || s.paths.some(blank))
      errors.push(`${stage} needs the paths it may change`);
    if (blank(s?.title) || blank(s?.body)) errors.push(`${stage} needs a PR title and body`);
    const filter = benchFilter(s?.checks?.find((c) => c.kind === 'bench')?.command ?? '');
    if (filter)
      errors.push(`${stage} bench check filters the benchmarks (${filter}); run them all`);
    // A check passes only if every command in it does.
    for (const c of s?.checks ?? [])
      if (/\|\||;|(^|[^&])&(?!&)/.test(c?.command ?? ''))
        errors.push(`${stage} ${c.kind} check chains commands with something other than \`&&\``);
    for (const [kind, runs] of Object.entries(CHECKS[stage])) {
      const command = s?.checks?.find((c) => c.kind === kind)?.command ?? '';
      if (blank(command) || runs.some((r) => !command.includes(r)))
        errors.push(
          `${stage} needs a ${kind} check${runs.map((r) => ` running \`${r}\``).join(' and')}`,
        );
    }
  }
  return errors;
}

/**
 * What in a `cargo bench` command selects only some benchmarks: a bench target, or a filter
 * passed to Criterion. Benchmarks a run skips leave stale results the judge ignores.
 */
export function benchFilter(command: string): string | undefined {
  const args =
    command
      .match(/cargo bench\b([^;&|]*)/)?.[1]
      .trim()
      .split(/\s+/) ?? [];
  const valued = ['-p', '--package', '--manifest-path', '--features', '-F', '--profile'];
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--' || args[i] === '--bench' || args[i].startsWith('--bench=')) return args[i];
    if (valued.includes(args[i]) || args[i] === '--target-dir') i++;
    else if (args[i] && !args[i].startsWith('-')) return args[i];
  }
}

/**
 * Functions a C header declares: a name with a return type before it, either type words or a
 * macro call on the same line such as `arrayof(char**)`. Column-zero macro calls such as
 * `KHASH_MAP_INIT_INT64(...)` have none, and comments, typedefs and static functions are skipped.
 */
export function declarations(header: string): string[] {
  // Comments become blank space, keeping lines, so prose in them never reads as a prototype.
  const code = header
    .replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, ' '))
    .replace(/\/\/[^\n]*/g, '');
  const pattern =
    /^(?!typedef\b|static\b|#|\/\/|\/?\*|\s)(?:\w+\([^()]*\)(?:[ \t]*|[ \t]*\n[ \t]+)|(?:[\w*]+[\s*]+)+)(\w+)\s*\(/gm;
  return [...code.matchAll(pattern)].map((m) => m[1]);
}

/**
 * What the plan gets wrong about the repository and its tools: a remote that is not the
 * repository, where the PRs open from its branches; stage branches git refuses,
 * missing tools, headers not on the base branch, functions the symbols leave out or invent, and
 * paths spanning several crates.
 */
export async function inspect(plan: Plan, root: string, git: (args: string[]) => Promise<string>) {
  const problems: string[] = [];
  const url = await git(['remote', 'get-url', plan.remote]).catch(() => '');
  if (
    !new RegExp(
      `[/@]github\\.com[/:]${plan.repository.replace(/\./g, '\\.')}(\\.git)?/?$`,
      'i',
    ).test(url)
  )
    problems.push(`remote ${plan.remote} is not github.com/${plan.repository}, where the PRs open`);
  for (const stage of STAGES) {
    const branch = `${plan.branchPrefix}-${stage}`;
    await git(['check-ref-format', '--branch', branch]).catch(() =>
      problems.push(`${branch} is not a valid branch name`),
    );
  }
  for (const tool of ['mutants', 'nextest'])
    await execa('cargo', [tool, '--version'], { cwd: root }).catch(() =>
      problems.push(`cargo ${tool} is missing: cargo install --locked cargo-${tool}`),
    );
  await criterion(root).catch((error) =>
    problems.push(`cargo metadata fails, so benchmarks cannot be read: ${error.shortMessage}`),
  );
  await git(['fetch', plan.remote, plan.baseBranch]);
  const declared = new Set<string>();
  for (const path of plan.module.filter((p) => p.endsWith('.h'))) {
    const header = await git(['show', `FETCH_HEAD:${path}`]).catch(() => undefined);
    if (header === undefined) problems.push(`${path} is not on ${plan.baseBranch}`);
    else for (const name of declarations(header)) declared.add(name);
  }
  const mapped = new Set(plan.symbols.map((s) => s.c));
  const unmapped = [...declared].filter((name) => !mapped.has(name));
  const unknown = [...mapped].filter((name) => !declared.has(name));
  if (unmapped.length) problems.push(`symbols leaves out ${unmapped.join(', ')}`);
  if (unknown.length) problems.push(`symbols names undeclared ${unknown.join(', ')}`);
  const files = (await git(['ls-tree', '-r', '--name-only', 'FETCH_HEAD'])).split('\n');
  for (const stage of STAGES) problems.push(...crates(plan.stages[stage].paths, files));
  return problems;
}

/** Paths holding several crates: a stage names each crate it changes, so spreading shows. */
export function crates(paths: string[], files: string[]): string[] {
  return paths.flatMap((path) => {
    const dir = path.replace(/\/*$/, '/');
    const count = files.filter((f) => f.startsWith(dir) && f.endsWith('/Cargo.toml')).length;
    return count > 1 ? [`${path} holds ${count} crates; list each one the stage changes`] : [];
  });
}

const testName = (t: Test) => `\`${t.path}::${t.name}\``;
const contract = (plan: Plan) =>
  plan.contract.map((c) => `- ${c.behavior} Tests: ${c.tests.map(testName).join(', ')}`);

export function divergences(plan: Plan): string {
  if (!plan.divergences.length) return 'None: the Rust behaves like the C everywhere.';
  return plan.divergences
    .map(
      (d) =>
        `- **${d.behavior}.** C: ${d.c} Rust: ${d.rust} Why: ${d.why} Test: ${testName(d.test)}`,
    )
    .join('\n');
}

/**
 * The stage's PR. Every PR lists the divergences, so whoever reviews it can reject one, then, folded
 * away, the plan it was admitted under, so an amendment that widens a stage or weakens its checks
 * shows, and the `record` of the run.
 */
export function pr(plan: Plan, stage: Stage, record: string) {
  const { title, body } = plan.stages[stage];
  return {
    title,
    body: `${body}

#### Behavior changes from C

${divergences(plan)}

<details><summary>How this port ran</summary>

${describe(plan, false)}

${record}
</details>
`,
  };
}

/** The plan as reviewers read it; a PR shows the divergences on their own. */
export function describe(plan: Plan, withDivergences = true): string {
  const stages = STAGES.map((s) => {
    const { paths, checks } = plan.stages[s];
    return `- \`${s}\` may change ${paths.map((p) => `\`${p}\``).join(', ')} and must pass ${checks.map((c) => `\`${c.command}\``).join(', ')}.`;
  });
  return `### Contract

${contract(plan).join('\n')}
${withDivergences ? `\n### Divergences from C\n\n${divergences(plan)}\n` : ''}
### Stages

${stages.join('\n')}

### What replaces each C function

${plan.symbols.map((s) => `- \`${s.c}\`: \`${s.rust}\``).join('\n')}

### Structure, dependencies and handoffs

${plan.structure}

### Interfaces later stages rely on

${plan.interfaces}`;
}
