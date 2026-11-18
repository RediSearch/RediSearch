/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/**
 * The engine runs one job: a module ported as stacked PRs, one per stage of the profile.
 *
 * The worker drives it through the operations: it records a plan, then claims each stage with a
 * committed revision. A claim passes the gates, which cost nothing, then pays for an assessment
 * (checks, then reviewers) unless passing evidence for the same revision exists. A stage nothing
 * holds back is admitted: its PR is published, and the worker that started on it hands the job to
 * a fresh worker for the next stage. Everything left to do is a finding; forge feedback becomes
 * findings at the start of every turn. After each turn the engine decides whether the worker
 * continues on its own. Once every stage is admitted and nothing is outstanding, a retrospective
 * finishes the job.
 *
 * The sections below follow that story: operations, turns, claiming, publishing and handing on,
 * feedback, and the messages the worker reads.
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { readLog, runChecks, testRan, type CheckEnv, type CheckResult } from './checks.ts';
import { confirmProposals, runReproduction } from './confirm.ts';
import { diffStat, parseDiff, patchId, sizeClass, type Size } from './diff.ts';
import { checkClaim } from './gates.ts';
import {
  addFindings,
  blocksAdmission,
  budget,
  closeFinding,
  done,
  formatFindings,
  hash,
  isAdmitted,
  newState,
  outstanding,
  passed,
  previousStage,
  progressMark,
  replaceFindings,
  repliesDue,
  RETRO,
  reviewedEvidence,
  runReport,
  stopReason,
  takeNewFindings,
  TARGETS,
  validateRetro,
  type Evidence,
  type NewFinding,
  type Retro,
  type State,
} from './job.ts';
import { citedTests, describePlan, pullRequest, validatePlan, type Plan } from './plan.ts';
import type { Forge, Host, Profile, Stage, Vcs } from './ports.ts';
import { checkRefs } from './refs.ts';
import { reviewStage, type Review } from './review.ts';
import { stateFile } from './state-file.ts';

/** How long one assessment's checks and reviewers may take. */
const ASSESSMENT_MINUTES = 90;

const REF_SCHEMA = {
  type: 'object',
  properties: { path: { type: 'string' }, line: { type: 'number' }, test: { type: 'string' } },
};
const EVIDENCE_SCHEMA = {
  type: 'array',
  items: { ...REF_SCHEMA, properties: { ...REF_SCHEMA.properties, finding: { type: 'string' } } },
};
const RETRO_SCHEMA = Object.fromEntries(
  Object.entries(RETRO).map(([list, fields]) => [
    list,
    {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          ...Object.fromEntries(
            fields.map((f) => [
              f,
              f === 'target' ? { type: 'string', enum: TARGETS } : { type: 'string' },
            ]),
          ),
          evidence: EVIDENCE_SCHEMA,
        },
        required: [...fields, 'evidence'],
      },
    },
  ]),
);

/** Something the worker can do, which the host offers as a tool named by {@link Host.toolName}. */
export interface Operation {
  name: string;
  description: string;
  schema: { type: 'object'; properties: Record<string, object>; required?: string[] };
  run(input: Record<string, unknown>, thread: string, signal: AbortSignal): Promise<string>;
}

export interface EngineOptions<X> {
  /** The checkout the job works in. */
  root: string;
  /** Where the job's state, run logs and debug log live. */
  dir: string;
  profile: Profile<X>;
  host: Host;
  vcs: Vcs;
  forge: (repository: string) => Forge;
  /** The documents the worker follows, in order. */
  policy: string[];
  log: (message: string) => void;
  /** Called with the job's summary whenever its state is saved. */
  saved?: (summary: string) => void;
  /** Aborts every assessment, as when the host unloads the plugin. */
  disposed: AbortSignal;
}

export function engine<X extends object>(o: EngineOptions<X>) {
  const { root, profile, host, vcs } = o;
  const stages = profile.stages.map((s) => s.name);
  const { toolName: tool } = host;
  const prefix = `[${profile.name}]`;
  const quoted = o.policy.map((p) => `\`${p}\``);
  const policy =
    quoted.length > 1 ? `${quoted.slice(0, -1).join(', ')} and ${quoted.at(-1)}` : quoted[0];
  const follow = `Follow ${policy}`;
  // The job's stage order is the profile's: every plan is recorded in it, and a state written
  // before that rule is brought into it when loaded.
  const inStageOrder = (plan: Plan<X>) => {
    plan.stages = Object.fromEntries(stages.map((s) => [s, plan.stages[s]]));
  };
  const { debug, load, loadQuietly, save, enqueue, withOwnedJob } = stateFile<X>({
    ...{ dir: o.dir, name: profile.name, log: o.log },
    prepare: (state) => inStageOrder(state.job.plan),
    saved: (state) => o.saved?.(summary(state)),
  });

  // ## The worker's operations
  //
  // Every operation but `plan` acts on the job the calling thread owns, in the queue.

  const operations: Operation[] = [
    {
      name: 'plan',
      description: `Record or amend the plan ${policy} describe. Amending invalidates every admission and keeps the budget.`,
      schema: { type: 'object', properties: { plan: { type: 'object' } }, required: ['plan'] },
      run: (input, thread) =>
        enqueue(tool('plan'), async () => {
          const plan = input.plan as Plan<X>;
          const errors = validatePlan(plan, profile);
          if (!errors.length) {
            inStageOrder(plan);
            const branches = stages.map((s) => `${plan.branchPrefix}-${s}`);
            const forge = o.forge(plan.repository);
            errors.push(...(await forge.checkRemote({ remote: plan.remote, branches })));
            const base = await vcs.fetchBranch(plan.remote, plan.baseBranch);
            errors.push(...((await profile.checkPlan?.(plan, { root, vcs, base })) ?? []));
          }
          if (errors.length) throw new Error(errors.join('\n'));
          const state = (await load()) ?? newState(thread, plan);
          if (state.job.owner !== thread) throw new Error('Another thread owns the job');
          const where = (p: Plan) => [p.repository, p.remote, p.baseBranch, p.branchPrefix];
          if (
            Object.keys(state.job.prs).length &&
            hash(where(state.job.plan)) !== hash(where(plan))
          )
            throw new Error(
              'The stage PRs are open: repository, remote, baseBranch and branchPrefix are fixed',
            );
          state.job.plan = plan;
          state.job.planHash = hash(plan);
          await save(state);
          return `${follow}. ${summary(state)}`;
        }),
    },
    {
      name: 'claim',
      description: `Claim that a committed, checked-out revision completes a stage. Blocks while checks and ${profile.reviewers.length} reviewers run, opens or updates the stage PR on admission, and returns new findings.`,
      schema: {
        type: 'object',
        properties: {
          stage: { type: 'string', enum: stages },
          revision: {
            type: 'string',
            description: 'The checked-out commit; the tree must be clean',
          },
        },
        required: ['stage', 'revision'],
      },
      run: ({ stage, revision }, thread, signal) =>
        withOwnedJob(`${tool('claim')} ${stage} ${revision}`, thread, (state) =>
          claim(state, String(stage), String(revision), signal),
        ),
    },
    {
      name: 'blocked',
      description:
        'Report a blocker only a human can resolve, such as missing access or a plan that cannot work. Stops automatic continuation; your next claim clears it.',
      schema: { type: 'object', properties: { reason: { type: 'string' } }, required: ['reason'] },
      run: (input, thread) =>
        withOwnedJob(tool('blocked'), thread, async (state) => {
          state.job.blocked = String(input.reason);
          return 'Reported. Summarize where the work stands and end your turn.';
        }),
    },
    {
      name: 'close',
      description:
        'Close a reviewer finding or review comment: fixed, or no-change with the reason it does not apply, citing the files, lines or tests that show it. Failed checks and CI close themselves.',
      schema: {
        type: 'object',
        properties: {
          id: { type: 'string' },
          resolution: { type: 'string', enum: ['fixed', 'no-change'] },
          reason: {
            type: 'string',
            description:
              'Required for no-change and review threads, where it is the reply: one plain line of at most 280 characters.',
          },
          evidence: { type: 'array', items: { ...REF_SCHEMA, required: ['path'] } },
        },
        required: ['id', 'resolution', 'evidence'],
      },
      run: (input, thread) =>
        withOwnedJob(`${tool('close')} ${input.id} ${input.resolution}`, thread, async (state) => {
          const { id, resolution, reason, evidence } = input as any;
          const problems = await checkRefs(root, evidence);
          if (problems.length)
            throw new Error(`The evidence does not hold:\n- ${problems.join('\n- ')}`);
          const finding = closeFinding(state, id, resolution, reason, evidence);
          // The admitted candidate does not contain the fix, so the stage must be claimed again.
          if (resolution === 'fixed') delete state.job.admitted[finding.stage];
          // A no-change answer goes out now; a fix is answered when it is published.
          if (resolution === 'no-change') await postReplies(state);
          return `Closed ${id}. ${outstanding(state).length} outstanding.`;
        }),
    },
    {
      name: 'retro',
      description: `Once every stage is admitted, nothing is outstanding and CI is green, record the postmortem ${policy} describe, each item citing evidence. It finishes the job.`,
      schema: { type: 'object', properties: RETRO_SCHEMA, required: Object.keys(RETRO) },
      run: (input, thread) =>
        withOwnedJob(tool('retro'), thread, async (state) => {
          const pending = await collectFeedback(state);
          if (!done(state)) throw new Error('Not done: admit every stage and close every finding');
          if (pending) throw new Error('CI is still running; end your turn until it finishes');
          const retro = input as unknown as Retro;
          const errors = validateRetro(retro);
          if (!errors.length)
            for (const list of Object.keys(RETRO) as (keyof Retro)[])
              for (const [i, item] of retro[list].entries())
                for (const problem of await checkRefs(root, item.evidence, state.findings))
                  errors.push(`${list}[${i}]: ${problem}`);
          if (errors.length) throw new Error(errors.join('\n'));
          // The PRs carry the run's record, which now ends in the retrospective. A failed update
          // throws, and a failed call saves nothing, so the job stays unfinished until both show it.
          state.job.retro = retro;
          for (const stage of stages)
            await publish(state, stage, state.job.admitted[stage]!.candidate);
          return 'Recorded. The job is finished; end your turn.';
        }),
    },
    {
      name: 'findings',
      description: 'List every outstanding finding.',
      schema: { type: 'object', properties: {} },
      // Queued, so a new worker asking while its predecessor's claim saves sees itself the owner.
      run: (_, thread) =>
        withOwnedJob(tool('findings'), thread, async (state) => {
          const listed = formatFindings(outstanding(state), tool('close'));
          return `${summary(state)}\n\n${listed || 'No outstanding findings.'}`;
        }),
    },
  ];

  // ## Turns
  //
  // A turn of the owner starts with what arrived since the last one. A finished turn continues on
  // its own only while the worker can act without a human.

  /** What a turn of `thread` starts with: the new findings for the worker, a warning for a former one. */
  async function startTurn(thread: string): Promise<string | undefined> {
    const peeked = await loadQuietly();
    // A former worker still wakes for its subscriptions and its user, but must not touch the
    // checkout its successor works in.
    if (peeked?.job.former?.includes(thread))
      return `${prefix} This thread handed the job to ${host.threadUrl(peeked.job.owner)}, which works in this checkout now. Change no files, make no commits and call no ${tool('*')} tools: answer what you were asked, then end your turn.`;
    if (peeked?.job.owner !== thread) return;
    return enqueue('start', async () => {
      const state = (await load())!;
      await collectFeedback(state).catch((e) => debug(`forge feedback collection failed: ${e}`));
      const content = newFindingsMessage(state, `${prefix} ${follow}. ${summary(state)}`);
      await save(state);
      return content;
    });
  }

  /**
   * Whether the worker continues after a finished turn, and with what: only while it is not
   * blocked, has budget left, and the last continued turn moved the job. A finished job waiting
   * for CI is not continued either: its forge subscription wakes the worker when CI reports.
   */
  async function endTurn(
    thread: string,
  ): Promise<{ continue: string } | { stop: string } | undefined> {
    if ((await loadQuietly())?.job.owner !== thread) return;
    return enqueue('end', async () => {
      const state = (await load())!;
      const { job } = state;
      if (job.retro) return;
      // An unreachable forge counts as pending, so the worker sleeps rather than spins.
      if (done(state) && (await collectFeedback(state).catch(() => true)))
        return void (await save(state));
      const now = progressMark(state);
      const reason =
        stopReason(job) ??
        (job.continued === now ? 'the last continued turn made no progress' : '');
      if (reason) {
        debug(`stopped: ${reason}`);
        return { stop: reason };
      }
      job.continued = now;
      await save(state);
      return {
        continue: done(state)
          ? `Every stage is admitted, nothing is outstanding and CI is green. Write the retrospective with ${tool('retro')} as ${policy} describe: a self-critical postmortem about the next iteration, not a recap.`
          : `Continue the ${profile.name} job.`,
      };
    });
  }

  // ## Claiming a stage
  //
  // A claim names a committed, checked-out revision. Gates reject it for free; otherwise it is
  // assessed, unless passing evidence for it exists, and admitted if nothing holds it back.

  async function claim(state: State<X>, stage: Stage, revision: string, signal: AbortSignal) {
    const { job } = state;
    delete job.blocked;
    const candidate = await vcs.resolveRevision(revision);
    if ((await vcs.currentRevision()) !== candidate)
      throw new Error(`Check out ${candidate} before claiming it`);
    const base = await stageBase(state, stage, candidate);
    const { problems, size } = await checkClaim({
      root,
      vcs,
      profile,
      state,
      stage,
      base,
      candidate,
    });
    if (problems.length)
      throw new Error(`${stage} at ${candidate} cannot be admitted:\n- ${problems.join('\n- ')}`);

    // Evidence is bound to stage, candidate, base and plan; changing any misses.
    const key = `${stage}:${candidate}:${base}:${job.planHash}`;
    if (state.evidence[key] && passed(state.evidence[key])) debug('reusing passing evidence');
    else {
      const reason = stopReason(job);
      if (reason)
        throw new Error(`Stopped: ${reason}. Report where the work stands and end your turn.`);
      const { evidence, findings } = await assess(
        state,
        stage,
        candidate,
        base,
        size,
        signal,
      ).finally(discardCheckChanges);
      if ((await vcs.currentRevision().catch(() => '')) !== candidate)
        throw new Error('The checkout changed during assessment; its evidence was discarded');
      job.used++;
      // Only failed evidence is assessed again; keeping it lets the run record count every attempt.
      if (state.evidence[key]) state.evidence[`${key}#${job.used}`] = state.evidence[key];
      state.evidence[key] = evidence;
      replaceFindings(state, stage, 'check', await checkFailures(evidence));
      addFindings(state, findings);
      // Saved before the forge is asked, so a forge failure cannot lose the evidence.
      await save(state);
    }
    await collectFeedback(state);

    const holding = outstanding(state).filter((f) => blocksAdmission(f, stage));
    const admitted = passed(state.evidence[key]) && !holding.length;
    debug(
      `${stage} ${admitted ? 'admitted' : 'not admitted'}, held by [${holding.map((f) => f.id)}]`,
    );
    let handedTo = '';
    if (admitted) {
      // Recorded first, so the PR's run record shows it; a failed publication throws, and a
      // failed call saves nothing.
      job.admitted[stage] = { candidate, base, plan: job.planHash };
      await publish(state, stage, candidate);
      const next = stages[stages.indexOf(stage) + 1];
      // Only the worker of this stage hands on; a later one re-admitting it keeps the job.
      if (next && stages.indexOf(stage) === (job.former?.length ?? 0))
        handedTo = await handOff(state, stage, next, candidate);
    }
    const big = sizeClass(size.lines);
    const result = newFindingsMessage(
      state,
      `${admitted ? `Stage ${stage} admitted at ${candidate}. ${job.prs[stage]?.url ?? ''}` : `Stage ${stage} at ${candidate} was not admitted.`} ` +
        `It adds ${size.lines} lines in ${size.files} files, ${size.code} net lines of code.` +
        (big ? ` ${big.label}: ${big.note}` : ''),
    );
    return handedTo
      ? `${result}\n\nThe job continues in a fresh thread, ${handedTo}, which owns this checkout now. Change nothing more and end your turn.`
      : result;
  }

  /** The base the stage's diff starts from: the base branch, or the admitted stage before it. */
  async function stageBase(state: State<X>, stage: Stage, candidate: string): Promise<string> {
    const { plan, admitted } = state.job;
    const prev = previousStage(state.job, stage);
    if (!prev) return vcs.mergeBase(candidate, await vcs.fetchBranch(plan.remote, plan.baseBranch));
    if (!isAdmitted(state.job, prev)) throw new Error(`Get ${prev} admitted before ${stage}`);
    const base = admitted[prev]!.candidate;
    if ((await vcs.mergeBase(base, candidate).catch(() => '')) !== base)
      throw new Error(`${candidate} does not descend from the admitted ${prev} revision ${base}`);
    return base;
  }

  /**
   * Runs the stage's checks, re-runs the reproductions of its fixed findings, and, if all pass,
   * the reviewers: fresh ones on the first review under the plan, then the same threads asked
   * about the change since. A rebase that leaves the patch unchanged keeps its reviews. A reviewer
   * that fails twice, or a cancelled claim, throws and records nothing, because that says nothing
   * about the candidate.
   */
  async function assess(
    state: State<X>,
    stage: Stage,
    candidate: string,
    base: string,
    size: Size,
    signal: AbortSignal,
  ) {
    const { job } = state;
    const dir = join(o.dir, 'runs', `${job.used + 1}-${stage}`);
    await mkdir(dir, { recursive: true });
    // Reviewers read renames as such; the gates, which must see every path, do not.
    const text = await vcs.diff(base, candidate, { renames: true });
    const diff = join(dir, 'diff.patch');
    await writeFile(diff, text);
    const patch = patchId(text);
    const timeoutMs = ASSESSMENT_MINUTES * 60_000;
    const cancel = AbortSignal.any([signal, o.disposed]);
    const stop = AbortSignal.any([cancel, AbortSignal.timeout(timeoutMs)]);
    const env: CheckEnv = {
      ...{ root, dir, vcs, signal: stop, judges: profile.judges, debug },
      vars: { CLAIM_BASE: base, CLAIM_CANDIDATE: candidate },
    };
    const planned = job.plan.stages[stage].checks;
    const checks: CheckResult[] = await runChecks(env, planned);
    cancel.throwIfAborted();
    const fail = (command: string, problem: string, exitCode: number | null = 0) =>
      checks.push({
        kind: 'tests',
        command,
        exitCode,
        started: Date.now(),
        ms: 0,
        log: '',
        problem,
      });
    // A failed check already rejects the candidate, and the checks after it ran none of their
    // tests, so neither the cited tests nor the fixes' reproductions are looked at.
    const rejected = checks.some((c) => c.problem);
    if (!rejected) {
      const logs = (await Promise.all(checks.map((c) => readLog(c.log)))).join('\n');
      const unrun = citedTests(job.plan, stage).filter((t) => !testRan(logs, t.name));
      if (unrun.length)
        fail(
          'the tests the plan cites',
          `never ran ${unrun.map((t) => `${t.path}::${t.name}`).join(', ')}`,
        );
    }
    // A fix stands only while its reproduction passes: a later candidate can bring the failure back.
    let n = 0;
    for (const f of rejected ? [] : Object.values(state.findings)) {
      if (f.stage !== stage || f.closed?.resolution !== 'fixed' || !f.repro) continue;
      const result = await runReproduction(env, f.repro, `fix-${++n}`);
      const exitCode = typeof result === 'string' ? 1 : result.exitCode;
      if (exitCode !== 0)
        fail(f.repro.command, `does not fix ${f.id}: its reproduction still fails`, exitCode);
    }

    const earlier = reviewedEvidence(state, stage);
    const failing = checks.some((c) => c.problem);
    const same = failing ? undefined : earlier.find((e) => e.patch === patch);
    const prior = earlier.at(-1);
    let reviews: Review[] = [];
    if (same) debug(`reusing the reviews of ${same.candidate}, whose patch is the same`);
    else if (!failing) {
      // After a rebase the change since is not a diff of the two candidates.
      const delta = prior?.base === base ? join(dir, 'delta.patch') : undefined;
      if (delta)
        await writeFile(delta, await vcs.diff(prior!.candidate, candidate, { renames: true }));
      reviews = await reviewStage(host, {
        ...{
          plan: job.plan,
          profile,
          stage,
          owner: job.owner,
          candidate,
          base,
          diff,
          delta,
          prior,
        },
        ...{ stat: diffStat(parseDiff(text)), lines: size.lines },
        ...{ signal: stop, timeoutMs, debug },
      });
    }
    cancel.throwIfAborted();
    const evidence: Evidence = {
      ...{ stage, candidate, base, plan: job.planHash, patch, checks, size },
      reviewers:
        same?.reviewers ?? reviews.map(({ label, thread }) => ({ reviewer: label, thread })),
    };
    const findings = await confirmProposals(env, profile, reviews, {
      ...{ stage, candidate, tests: planned.filter((c) => c.kind === 'test') },
      link: host.threadUrl,
    });
    // Timed-out trials would weaken findings to suggestions, so a timeout discards the assessment.
    stop.throwIfAborted();
    return { evidence, findings };
  }

  /** Failed checks, as findings that only a later passing assessment closes. */
  async function checkFailures(evidence: Evidence): Promise<NewFinding[]> {
    const { stage, candidate } = evidence;
    return Promise.all(
      evidence.checks
        .filter((c) => c.problem)
        .map(async (c) => {
          const tail = (await readLog(c.log)).trimEnd().split('\n').slice(-20).join('\n');
          return {
            ...{ key: `check:${stage}:${c.command}`, source: 'check' as const, stage },
            ...{ revision: candidate, blocking: true, title: `${c.kind} check ${c.problem}` },
            body: [`\`${c.command}\``, c.log && `Log: ${c.log}`, tail].filter(Boolean).join('\n'),
            links: [],
          };
        }),
    );
  }

  /**
   * Puts back what an assessment changed or created, such as the sources `cargo mutants
   * --in-place` mutates when killed; the claim started from a clean tree. Ignored files stay.
   */
  async function discardCheckChanges() {
    if (!(await vcs.discardChanges())) return;
    debug('restored the files the assessment changed or created');
    throw new Error('The checks changed files, now restored; the evidence was discarded');
  }

  // ## Publishing and handing on

  /** Opens or updates the stage's PR, stacked on the stage before it, and answers what it fixed. */
  async function publish(state: State<X>, stage: Stage, candidate: string) {
    const { job } = state;
    const branch = (s: Stage) => `${job.plan.branchPrefix}-${s}`;
    const prev = previousStage(job, stage);
    job.prs[stage] = await o.forge(job.plan.repository).publish({
      ...pullRequest(job.plan, profile, stage, runReport(state)),
      ...{ remote: job.plan.remote, branch: branch(stage), candidate, existing: job.prs[stage] },
      base: prev ? branch(prev) : job.plan.baseBranch,
    });
    await postReplies(state);
  }

  /**
   * Hands the job to a fresh worker thread for `next`. Every request re-reads a thread's whole
   * history, and the plugin holds all a stage needs, so the next stage starts without the earlier
   * one's.
   */
  async function handOff(state: State<X>, stage: Stage, next: Stage, candidate: string) {
    const { job } = state;
    const from = job.owner;
    const { deliverable, paths, checks } = job.plan.stages[next];
    const prs = stages.flatMap((s) => job.prs[s]?.url ?? []);
    const briefing = `${prefix} You take over a ${profile.name} job at stage \`${next}\` from ${host.threadUrl(from)}, which got \`${stage}\` admitted at ${candidate}; the checkout is at that revision. ${follow} and read the skills they name. The plugin holds the plan, the evidence and every finding, so you need nothing from earlier threads.

First subscribe to the CI and comments of ${prs.join(' and ')}.

## Stage \`${next}\`

${deliverable}

It may change ${paths.map((p) => `\`${p}\``).join(', ')}, and must pass ${checks.map((c) => `\`${c.command}\``).join(', ')}.

## Outstanding findings

${formatFindings(outstanding(state), tool('close')) || 'None.'}

## The plan

${describePlan(job.plan, profile)}`;
    // Ownership moves only once the new thread has its briefing. Its first tool calls queue behind
    // this claim, by when it owns the job; if anything fails, the current worker carries on.
    try {
      const child = await host.startThread({ parent: from, labels: [next] });
      await host.sendMessage(child, briefing);
      job.former = [...(job.former ?? []), from];
      job.owner = child;
      debug(`handed the job to ${child} for ${next}`);
      return host.threadUrl(child);
    } catch (error) {
      debug(`handing the job on failed, so ${from} keeps it: ${error}`);
      return '';
    }
  }

  // ## Forge feedback
  //
  // Comments and CI on the stage PRs become findings; closed comment findings are answered.

  /** Takes in the feedback on every PR and answers what is due; returns whether CI is pending. */
  async function collectFeedback(state: State<X>): Promise<boolean> {
    const { job } = state;
    const forge = o.forge(job.plan.repository);
    let pending = false;
    for (const stage of stages) {
      const pr = job.prs[stage];
      if (!pr) continue;
      const feedback = await forge.fetchFeedback({
        ...{ stage, number: pr.number, posted: job.posted, head: pr.head },
      });
      const ci = feedback.findings.filter((f) => f.source === 'ci');
      const comments = feedback.findings.filter((f) => f.source !== 'ci');
      debug(
        `PR #${pr.number}: ${comments.length} comments, CI ${feedback.pending ? 'pending' : 'reported'}`,
      );
      pending ||= feedback.pending;
      addFindings(state, comments);
      // CI still running on a new head has not passed yet, so its finding stays open.
      const running = feedback.pending
        ? outstanding(state).filter((f) => f.stage === stage && f.source === 'ci')
        : [];
      replaceFindings(state, stage, 'ci', [...ci, ...running]);
    }
    await postReplies(state);
    return pending;
  }

  /**
   * Posts the {@link repliesDue} and resolves their threads, once on each PR a comment was posted
   * on. One that fails stays due, so the next collection retries it.
   */
  async function postReplies(state: State<X>) {
    const replies = repliesDue(state);
    if (!replies.length) return;
    const forge = o.forge(state.job.plan.repository);
    for (const { finding: f, reply } of replies)
      for (const r of [...f.replies!])
        try {
          if (r.comment) state.job.posted.push(await forge.replyToComment(r.pr, r.comment, reply));
          delete r.comment;
          if (r.thread) await forge.resolveThread(r.thread);
          f.answered = [...new Set([...(f.answered ?? []), r.pr])];
          f.replies = f.replies!.filter((other) => other !== r);
        } catch (error) {
          debug(`answering ${f.id} on PR #${r.pr} failed: ${error}`);
        }
  }

  // ## What the worker reads

  function summary(state: State<X>): string {
    const { job } = state;
    const reason = stopReason(job);
    const listed = stages.map((s) =>
      `${s} ${isAdmitted(job, s) ? 'admitted' : 'pending'} ${job.prs[s]?.url ?? ''}`.trim(),
    );
    return `${reason ? `stopped: ${reason}. ` : ''}${listed.join('; ')}. ${job.used}/${budget(job)} assessments.`;
  }

  /** The new findings as a numbered list, after a one-line headline. */
  function newFindingsMessage(state: State<X>, headline: string): string {
    const fresh = takeNewFindings(state);
    const total = outstanding(state).length;
    debug(`delivering [${fresh.map((f) => f.id)}], ${total} outstanding`);
    const findings = `Findings: ${fresh.length} new, ${total} outstanding (${tool('findings')} lists all).`;
    return [`${headline} ${findings}`, formatFindings(fresh, tool('close'))]
      .filter(Boolean)
      .join('\n\n');
  }

  return {
    operations,
    debug,
    startTurn,
    endTurn,

    async summary(): Promise<string | undefined> {
      const state = await load();
      return state && summary(state);
    },

    /** Grants an operator's further assessments; false when there is no job. */
    grant: (amount: number, by: string) =>
      enqueue(`grant ${amount}`, async () => {
        const state = await load();
        if (!state) return false;
        state.job.grants.push({ amount, at: new Date().toISOString(), by });
        await save(state);
        return true;
      }),
  };
}
