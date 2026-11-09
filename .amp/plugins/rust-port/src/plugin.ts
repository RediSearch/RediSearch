/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
import type { PluginAPI, ThreadID } from '@ampcode/plugin';
import { createHash } from 'node:crypto';
import { appendFile, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { stripVTControlCharacters } from 'node:util';
import { git, ran, runChecks, type CheckResult } from './checks.ts';
import { confirm, reproduction, type Run } from './confirm.ts';
import { gates, sizeClass, type Size } from './gates.ts';
import { github } from './github.ts';
import {
  cited,
  describe,
  inspect,
  POLICY,
  pr,
  STAGES,
  validate,
  type Plan,
  type Stage,
} from './plan.ts';
import { verify } from './refs.ts';
import { REVIEWERS, reviewStage, type Review } from './review.ts';
import {
  budget,
  close,
  done,
  due,
  hash,
  holds,
  isAdmitted,
  newState,
  outstanding,
  passed,
  previous,
  progress,
  report,
  receive,
  reconcile,
  render,
  RETRO,
  reviewed,
  stopped,
  TARGETS,
  undelivered,
  validateRetro,
  type Evidence,
  type NewFinding,
  type Retro,
  type State,
} from './state.ts';

/** How long one assessment's checks and reviewers may take. */
const ASSESSMENT_MINUTES = 90;
const tool = (name: string) => `port_${name}`;

export default function (amp: PluginAPI) {
  if (!amp.system.workspaceRoot) return;
  const root = amp.helpers.filePathFromURI(amp.system.workspaceRoot);
  const dir = join(root, '.amp', 'rust-port');
  const file = join(dir, 'state.json');
  const disposed = new AbortController();
  amp.onDispose(() => disposed.abort());
  // Experimental in Amp, so optional: the job's state at a glance, next to the prompt.
  const statusItem = amp.experimental?.createStatusItem();
  const sh = (args: string[], input?: string) => git(root, args, input);
  const link = (thread: string) => new URL(`/threads/${thread}`, amp.system.ampURL).toString();

  /** Goes to Amp's log and to `debug.log` in the state directory, which is easier to follow. */
  function debug(message: string) {
    amp.logger.log(message);
    mkdir(dir, { recursive: true })
      .then(() => appendFile(join(dir, 'debug.log'), `${new Date().toISOString()} ${message}\n`))
      .catch(() => {});
  }
  // Names the running build, so a fix can be confirmed loaded before a retry.
  readFile(fileURLToPath(import.meta.url))
    .then((code) =>
      debug(`plugin loaded: ${createHash('sha1').update(code).digest('hex').slice(0, 10)}`),
    )
    .catch(() => {});

  async function load(): Promise<State | undefined> {
    const text = await readFile(file, 'utf8').catch((error) => {
      if (error.code === 'ENOENT') return undefined;
      throw error;
    });
    if (text === undefined) return;
    const state = JSON.parse(text);
    if (typeof state?.job?.planHash !== 'string')
      throw new Error(`${file} was not written by this version; move it aside to start a job`);
    return state;
  }
  /** Hooks must not fail the turn, so an unreadable state means the thread owns no job. */
  const owns = (thread: string) =>
    load().then(
      (state) => state?.job.owner === thread,
      (error) => (debug(`ignoring ${thread}: ${error}`), false),
    );
  async function save(state: State) {
    // Written aside and renamed, so a crash never leaves a half-written state.
    await mkdir(dir, { recursive: true });
    await writeFile(`${file}.tmp`, JSON.stringify(state, null, 2));
    await rename(`${file}.tmp`, file);
    statusItem?.update({ text: `rust-port: ${summary(state)}` });
  }

  // Every state transition goes through this queue. Hooks check ownership before queueing, so
  // other threads never wait behind an assessment.
  let tail: Promise<unknown> = Promise.resolve();
  function serial<T>(name: string, fn: () => Promise<T>): Promise<T> {
    const run = tail.then(async () => {
      const start = Date.now();
      try {
        const result = await fn();
        debug(`${name}: done in ${Date.now() - start}ms`);
        return result;
      } catch (error) {
        debug(`${name}: failed in ${Date.now() - start}ms: ${error}`);
        throw error;
      }
    });
    tail = run.catch(() => {});
    return run;
  }
  /** Runs `fn` in the queue on the job `thread` owns, and saves it after. */
  function update<T>(name: string, thread: string, fn: (state: State) => Promise<T>) {
    return serial(name, async () => {
      const state = await load();
      if (state?.job.owner !== thread) throw new Error('This thread does not own a rust-port job');
      const result = await fn(state);
      await save(state);
      return result;
    });
  }

  function summary(state: State): string {
    const { job } = state;
    const reason = stopped(job);
    const stages = STAGES.map((s) =>
      `${s} ${isAdmitted(job, s) ? 'admitted' : 'pending'} ${job.prs[s]?.url ?? ''}`.trim(),
    );
    return `${reason ? `stopped: ${reason}. ` : ''}${stages.join('; ')}. ${job.used}/${budget(job)} assessments.`;
  }

  /** The new findings as a numbered list, after a one-line headline. */
  function digest(state: State, headline: string): string {
    const fresh = undelivered(state);
    const total = outstanding(state).length;
    debug(`delivering [${fresh.map((f) => f.id)}], ${total} outstanding`);
    const findings = `Findings: ${fresh.length} new, ${total} outstanding (${tool('findings')} lists all).`;
    return [`${headline} ${findings}`, render(fresh, tool('close'))].filter(Boolean).join('\n\n');
  }

  /** Takes in GitHub feedback on every PR, answers what is due; returns whether CI is pending. */
  async function collect(state: State): Promise<boolean> {
    const { job } = state;
    const gh = await github(root, job.plan.repository);
    let pending = false;
    for (const stage of STAGES) {
      const pr = job.prs[stage];
      const feedback = pr && (await gh.feedback(stage, pr.number, job.posted, pr.head));
      if (!feedback) continue;
      debug(
        `PR #${pr.number}: ${feedback.comments.length} comments, CI ${feedback.pending ? 'pending' : 'reported'}`,
      );
      pending ||= feedback.pending;
      receive(state, feedback.comments);
      // CI still running on a new head has not passed yet, so its finding stays open.
      const running = feedback.pending
        ? outstanding(state).filter((f) => f.stage === stage && f.source === 'ci')
        : [];
      reconcile(state, stage, 'ci', [...feedback.failures, ...running]);
    }
    await settle(state);
    return pending;
  }

  /**
   * Posts the replies {@link due} and resolves their threads, once on each PR a comment was posted
   * on. One that fails stays due, so the next collection retries it.
   */
  async function settle(state: State) {
    const replies = due(state);
    if (!replies.length) return;
    const gh = await github(root, state.job.plan.repository);
    for (const { finding: f, reply } of replies)
      for (const r of [...f.replies!])
        try {
          if (r.comment) state.job.posted.push((await gh.reply(r.pr, r.comment, reply)).data.id);
          delete r.comment;
          if (r.thread) await gh.resolve(r.thread);
          f.answered = [...new Set([...(f.answered ?? []), r.pr])];
          f.replies = f.replies!.filter((other) => other !== r);
        } catch (error) {
          debug(`answering ${f.id} on PR #${r.pr} failed: ${error}`);
        }
  }

  async function head(): Promise<string> {
    if (await sh(['status', '--porcelain'])) throw new Error('The working tree is dirty');
    return sh(['rev-parse', 'HEAD']);
  }

  /** The base the stage's diff starts from: the base branch, or the admitted stage before it. */
  async function baseOf(state: State, stage: Stage, candidate: string): Promise<string> {
    const { plan, admitted } = state.job;
    const prev = previous(stage);
    if (!prev) {
      await sh(['fetch', plan.remote, plan.baseBranch]);
      return sh(['merge-base', candidate, 'FETCH_HEAD']);
    }
    if (!isAdmitted(state.job, prev)) throw new Error(`Get ${prev} admitted before ${stage}`);
    const base = admitted[prev]!.candidate;
    await sh(['merge-base', '--is-ancestor', base, candidate]).catch(() => {
      throw new Error(`${candidate} does not descend from the admitted ${prev} revision ${base}`);
    });
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
    state: State,
    stage: Stage,
    candidate: string,
    base: string,
    size: Size,
    signal: AbortSignal,
  ) {
    const { job } = state;
    const run = join(dir, 'runs', `${job.used + 1}-${stage}`);
    await mkdir(run, { recursive: true });
    const text = await sh(['diff', base, candidate]);
    const diff = join(run, 'diff.patch');
    await writeFile(diff, text);
    // Verbatim, so whitespace inside a string or Python indentation makes a new patch.
    const patch = (await sh(['patch-id', '--verbatim'], text)).split(' ')[0];
    const timeoutMs = ASSESSMENT_MINUTES * 60_000;
    const cancel = AbortSignal.any([signal, disposed.signal]);
    const stop = AbortSignal.any([cancel, AbortSignal.timeout(timeoutMs)]);
    const trials: Run = {
      ...{ root, dir: run, signal: stop, debug },
      vars: { CLAIM_BASE: base, CLAIM_CANDIDATE: candidate },
    };
    const planned = job.plan.stages[stage].checks;
    const checks: CheckResult[] = await runChecks(root, run, planned, stop, debug, trials.vars);
    cancel.throwIfAborted();
    const extra = (command: string, problem: string, exitCode: number | null = 0) =>
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
      const logs = (
        await Promise.all(checks.map((c) => readFile(c.log, 'utf8').catch(() => '')))
      ).join('\n');
      const unrun = cited(job.plan, stage).filter((t) => !ran(logs, t.name));
      if (unrun.length)
        extra(
          'the tests the plan cites',
          `never ran ${unrun.map((t) => `${t.path}::${t.name}`).join(', ')}`,
        );
    }
    // A fix stands only while its reproduction passes: a later candidate can bring the failure back.
    let n = 0;
    for (const f of rejected ? [] : Object.values(state.findings)) {
      if (f.stage !== stage || f.closed?.resolution !== 'fixed' || !f.repro) continue;
      const result = await reproduction(trials, f.repro, `fix-${++n}`);
      const exitCode = typeof result === 'string' ? 1 : result.exitCode;
      if (exitCode !== 0)
        extra(f.repro.command, `does not fix ${f.id}: its reproduction still fails`, exitCode);
    }

    const earlier = reviewed(state, stage);
    const failing = checks.some((c) => c.problem);
    const same = failing ? undefined : earlier.find((e) => e.patch === patch);
    const prior = earlier.at(-1);
    let reviews: Review[] = [];
    if (same) debug(`reusing the reviews of ${same.candidate}, whose patch is the same`);
    else if (!failing) {
      // After a rebase the change since is not a diff of the two candidates.
      const delta = prior?.base === base ? join(run, 'delta.patch') : undefined;
      if (delta) await writeFile(delta, await sh(['diff', prior!.candidate, candidate]));
      reviews = await reviewStage(amp, {
        ...{ plan: job.plan, stage, owner: job.owner, candidate, base, diff, delta, prior },
        ...{ stat: await sh(['diff', '--stat', base, candidate]), lines: size.lines },
        ...{ signal: stop, timeoutMs, debug },
      });
    }
    cancel.throwIfAborted();
    const evidence: Evidence = {
      ...{ stage, candidate, base, plan: job.planHash, patch, checks, size },
      reviewers:
        same?.reviewers ?? reviews.map(({ label, thread }) => ({ reviewer: label, thread })),
    };
    const findings = await confirm(trials, reviews, {
      ...{ stage, candidate, tests: planned.filter((c) => c.kind === 'test') },
      threadURL: link,
    });
    // Timed-out trials would weaken findings to suggestions, so a timeout discards the assessment.
    stop.throwIfAborted();
    return { evidence, findings };
  }

  /** Failed checks, as findings that only a later passing assessment closes. */
  async function failures(evidence: Evidence): Promise<NewFinding[]> {
    const { stage, candidate } = evidence;
    return Promise.all(
      evidence.checks
        .filter((c) => c.problem)
        .map(async (c) => {
          const log = stripVTControlCharacters(await readFile(c.log, 'utf8').catch(() => ''));
          const tail = log.trimEnd().split('\n').slice(-20).join('\n');
          return {
            ...{
              key: `check:${stage}:${c.command}`,
              source: 'check' as const,
              stage,
              revision: candidate,
            },
            ...{ blocking: true, title: `${c.kind} check ${c.problem}`, links: [] },
            body: [`\`${c.command}\``, c.log && `Log: ${c.log}`, tail].filter(Boolean).join('\n'),
          };
        }),
    );
  }

  /** Opens or updates the stage's PR, stacked on the stage before it, and answers what it fixed. */
  async function publish(state: State, stage: Stage, candidate: string) {
    const { job } = state;
    const branch = (s: Stage) => `${job.plan.branchPrefix}-${s}`;
    const prev = previous(stage);
    const gh = await github(root, job.plan.repository);
    job.prs[stage] = await gh.publish({
      ...pr(job.plan, stage, report(state)),
      ...{ remote: job.plan.remote, branch: branch(stage), candidate, existing: job.prs[stage] },
      base: prev ? branch(prev) : job.plan.baseBranch,
    });
    await settle(state);
  }

  /**
   * Puts back what an assessment changed or created, such as the sources `cargo mutants
   * --in-place` mutates when killed; the claim started from a clean tree. Ignored files stay.
   */
  async function restore() {
    if (!(await sh(['status', '--porcelain']))) return;
    debug('restoring the files the assessment changed or created');
    await sh(['restore', '--source=HEAD', '--staged', '--worktree', '--', '.']);
    await sh(['clean', '-fd']);
    throw new Error('The checks changed files, now restored; the evidence was discarded');
  }

  async function claim(state: State, stage: Stage, revision: string, signal: AbortSignal) {
    const { job } = state;
    delete job.blocked;
    const candidate = await sh(['rev-parse', '--verify', `${revision}^{commit}`]);
    if ((await head()) !== candidate) throw new Error(`Check out ${candidate} before claiming it`);
    const base = await baseOf(state, stage, candidate);
    const { problems, size } = await gates(sh, root, state, stage, base, candidate);
    if (problems.length)
      throw new Error(`${stage} at ${candidate} cannot be admitted:\n- ${problems.join('\n- ')}`);

    // Evidence is bound to stage, candidate, base and plan; changing any misses.
    const key = `${stage}:${candidate}:${base}:${job.planHash}`;
    if (state.evidence[key] && passed(state.evidence[key])) debug('reusing passing evidence');
    else {
      const reason = stopped(job);
      if (reason)
        throw new Error(`Stopped: ${reason}. Report where the work stands and end your turn.`);
      const { evidence, findings } = await assess(
        state,
        stage,
        candidate,
        base,
        size,
        signal,
      ).finally(restore);
      if ((await head().catch(() => '')) !== candidate)
        throw new Error('The checkout changed during assessment; its evidence was discarded');
      job.used++;
      // Only failed evidence is assessed again; keeping it lets the run record count every attempt.
      if (state.evidence[key]) state.evidence[`${key}#${job.used}`] = state.evidence[key];
      state.evidence[key] = evidence;
      reconcile(state, stage, 'check', await failures(evidence));
      receive(state, findings);
      // Saved before GitHub is asked, so a GitHub failure cannot lose the evidence.
      await save(state);
    }
    await collect(state);

    const holding = outstanding(state).filter((f) => holds(f, stage));
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
      const next = STAGES[STAGES.indexOf(stage) + 1];
      // Only the worker of this stage hands on; a later one re-admitting it keeps the job.
      if (next && STAGES.indexOf(stage) === (job.former?.length ?? 0))
        handedTo = await handOff(state, stage, next, candidate);
    }
    const big = sizeClass(size.lines);
    const result = digest(
      state,
      `${admitted ? `Stage ${stage} admitted at ${candidate}. ${job.prs[stage]?.url ?? ''}` : `Stage ${stage} at ${candidate} was not admitted.`} ` +
        `It adds ${size.lines} lines in ${size.files} files, ${size.code} net lines of code.` +
        (big ? ` ${big.label}: ${big.note}` : ''),
    );
    return handedTo
      ? `${result}\n\nThe job continues in a fresh thread, ${handedTo}, which owns this checkout now. Change nothing more and end your turn.`
      : result;
  }

  /**
   * Hands the job to a fresh worker thread for `next`. Every request re-reads a thread's whole
   * history, and the plugin holds all a stage needs, so the next stage starts without the earlier
   * one's.
   */
  async function handOff(state: State, stage: Stage, next: Stage, candidate: string) {
    const { job } = state;
    const previous = job.owner as ThreadID;
    const { deliverable, paths, checks } = job.plan.stages[next];
    const prs = STAGES.flatMap((s) => job.prs[s]?.url ?? []);
    const content = `[rust-port] You take over a rust-port job at stage \`${next}\` from ${link(previous)}, which got \`${stage}\` admitted at ${candidate}; the checkout is at that revision. Follow ${POLICY} and read the skills it names. The plugin holds the plan, the evidence and every finding, so you need nothing from earlier threads.

First subscribe to the CI and comments of ${prs.join(' and ')} with \`create_github_subscription\`.

## Stage \`${next}\`

${deliverable}

It may change ${paths.map((p) => `\`${p}\``).join(', ')}, and must pass ${checks.map((c) => `\`${c.command}\``).join(', ')}.

## Outstanding findings

${render(outstanding(state), tool('close')) || 'None.'}

## The plan

${describe(job.plan)}`;
    // Ownership moves only once the new thread has its briefing. Its first tool calls queue behind
    // this claim, by when it owns the job; if anything fails, the current worker carries on.
    try {
      const agent = await amp.threads.get(previous).agent();
      const child = await agent.createThread({ parentThreadID: previous });
      await child.addLabels(['rust-port', next]).catch(() => {});
      await child.appendUserMessage({ type: 'user-message', content });
      job.former = [...(job.former ?? []), previous];
      job.owner = child.id;
      debug(`handed the job to ${child.id} for ${next}`);
      return link(child.id);
    } catch (error) {
      debug(`handing the job on failed, so ${previous} keeps it: ${error}`);
      return '';
    }
  }

  amp.registerTool({
    name: tool('plan'),
    description: `Record or amend the plan ${POLICY} describes. Amending invalidates every admission and keeps the budget.`,
    inputSchema: { type: 'object', properties: { plan: { type: 'object' } }, required: ['plan'] },
    execute: (input, ctx) =>
      serial(tool('plan'), async () => {
        const plan = input.plan as Plan;
        const errors = validate(plan);
        if (!errors.length) errors.push(...(await inspect(plan, root, sh)));
        if (errors.length) throw new Error(errors.join('\n'));
        const existing = await load();
        const state = existing ?? newState(ctx.thread.id, plan);
        if (state.job.owner !== ctx.thread.id) throw new Error('Another thread owns the job');
        const where = (p: Plan) => [p.repository, p.remote, p.baseBranch, p.branchPrefix];
        if (Object.keys(state.job.prs).length && hash(where(state.job.plan)) !== hash(where(plan)))
          throw new Error(
            'The stage PRs are open: repository, remote, baseBranch and branchPrefix are fixed',
          );
        state.job.plan = plan;
        state.job.planHash = hash(plan);
        await save(state);
        return `Follow ${POLICY}. ${summary(state)}`;
      }),
  });

  amp.registerTool({
    name: tool('claim'),
    description: `Claim that a committed, checked-out revision completes a stage. Blocks while checks and ${REVIEWERS.length} reviewers run, opens or updates the stage PR on admission, and returns new findings.`,
    inputSchema: {
      type: 'object',
      properties: {
        stage: { type: 'string', enum: [...STAGES] },
        revision: { type: 'string', description: 'The checked-out commit; the tree must be clean' },
      },
      required: ['stage', 'revision'],
    },
    execute: ({ stage, revision }, ctx) =>
      update(`${tool('claim')} ${stage} ${revision}`, ctx.thread.id, (state) =>
        claim(state, stage as Stage, String(revision), ctx.signal),
      ),
  });

  amp.registerTool({
    name: tool('blocked'),
    description:
      'Report a blocker only a human can resolve, such as missing access or a plan that cannot work. Stops automatic continuation; your next claim clears it.',
    inputSchema: {
      type: 'object',
      properties: { reason: { type: 'string' } },
      required: ['reason'],
    },
    execute: (input, ctx) =>
      update(tool('blocked'), ctx.thread.id, async (state) => {
        state.job.blocked = String(input.reason);
        return 'Reported. Summarize where the work stands and end your turn.';
      }),
  });

  const ref = {
    type: 'object',
    properties: { path: { type: 'string' }, line: { type: 'number' }, test: { type: 'string' } },
  };
  amp.registerTool({
    name: tool('close'),
    description:
      'Close a reviewer finding or review comment: fixed, or no-change with the reason it does not apply, citing the files, lines or tests that show it. Failed checks and CI close themselves.',
    inputSchema: {
      type: 'object',
      properties: {
        id: { type: 'string' },
        resolution: { type: 'string', enum: ['fixed', 'no-change'] },
        reason: {
          type: 'string',
          description:
            'Required for no-change and review threads, where it is the reply: one plain line of at most 280 characters.',
        },
        evidence: { type: 'array', items: { ...ref, required: ['path'] } },
      },
      required: ['id', 'resolution', 'evidence'],
    },
    execute: (input, ctx) =>
      update(`${tool('close')} ${input.id} ${input.resolution}`, ctx.thread.id, async (state) => {
        const { id, resolution, reason, evidence } = input as any;
        const problems = await verify(root, evidence);
        if (problems.length)
          throw new Error(`The evidence does not hold:\n- ${problems.join('\n- ')}`);
        const finding = close(state, id, resolution, reason, evidence);
        // The admitted candidate does not contain the fix, so the stage must be claimed again.
        if (resolution === 'fixed') delete state.job.admitted[finding.stage];
        // A no-change answer goes out now; a fix is answered when it is published.
        if (resolution === 'no-change') await settle(state);
        return `Closed ${id}. ${outstanding(state).length} outstanding.`;
      }),
  });

  const evidence = {
    type: 'array',
    items: { ...ref, properties: { ...ref.properties, finding: { type: 'string' } } },
  };
  const field = (f: string) => [
    f,
    f === 'target' ? { type: 'string', enum: TARGETS } : { type: 'string' },
  ];
  const lists = Object.entries(RETRO).map(([list, fields]) => [
    list,
    {
      type: 'array',
      items: {
        type: 'object',
        properties: { ...Object.fromEntries(fields.map(field)), evidence },
        required: [...fields, 'evidence'],
      },
    },
  ]);
  amp.registerTool({
    name: tool('retro'),
    description: `Once every stage is admitted, nothing is outstanding and CI is green, record the postmortem ${POLICY} describes, each item citing evidence. It finishes the job.`,
    inputSchema: {
      type: 'object',
      properties: Object.fromEntries(lists),
      required: Object.keys(RETRO),
    },
    execute: (input, ctx) =>
      update(tool('retro'), ctx.thread.id, async (state) => {
        const pending = await collect(state);
        if (!done(state)) throw new Error('Not done: admit every stage and close every finding');
        if (pending) throw new Error('CI is still running; end your turn until it finishes');
        const retro = input as unknown as Retro;
        const errors = validateRetro(retro);
        if (!errors.length)
          for (const list of Object.keys(RETRO) as (keyof Retro)[])
            for (const [i, item] of retro[list].entries())
              for (const problem of await verify(root, item.evidence, state.findings))
                errors.push(`${list}[${i}]: ${problem}`);
        if (errors.length) throw new Error(errors.join('\n'));
        // The PRs carry the run's record, which now ends in the retrospective. A failed update
        // throws, and a failed call saves nothing, so the job stays unfinished until both show it.
        state.job.retro = retro;
        for (const stage of STAGES)
          await publish(state, stage, state.job.admitted[stage]!.candidate);
        return 'Recorded. The job is finished; end your turn.';
      }),
  });

  amp.registerTool({
    name: tool('findings'),
    description: 'List every outstanding finding.',
    inputSchema: { type: 'object', properties: {} },
    // Queued, so a new worker asking while its predecessor's claim saves sees itself the owner.
    execute: (_, ctx) =>
      serial(tool('findings'), async () => {
        const state = await load();
        if (state?.job.owner !== ctx.thread.id)
          throw new Error('This thread does not own a rust-port job');
        return `${summary(state)}\n\n${render(outstanding(state), tool('close')) || 'No outstanding findings.'}`;
      }),
  });

  amp.registerCommand(
    'grant',
    {
      title: 'Grant assessments',
      category: 'rust-port',
      description: 'Operator-only budget grant',
    },
    async (ctx) => {
      const state = await load();
      if (!state) return ctx.ui.notify('No rust-port job in this checkout');
      const amount = Number(
        await ctx.ui.input({
          title: 'Grant how many more assessments?',
          helpText: summary(state),
          requireHuman: true,
        }),
      );
      if (!(amount > 0)) return;
      await serial(`grant ${amount}`, async () => {
        const fresh = (await load())!;
        fresh.job.grants.push({
          amount,
          at: new Date().toISOString(),
          by: amp.system.user?.email ?? 'unknown',
        });
        await save(fresh);
      });
    },
  );

  amp.on('agent.start', async (event) => {
    const former = await load().then(
      (state) => state?.job.former?.includes(event.thread.id) && state.job.owner,
      () => undefined,
    );
    // A former worker still wakes for its subscriptions and its user, but must not touch the
    // checkout its successor works in.
    if (former)
      return {
        message: {
          content: `[rust-port] This thread handed the job to ${link(former)}, which works in this checkout now. Change no files, make no commits and call no ${tool('*')} tools: answer what you were asked, then end your turn.`,
          display: true,
        },
      };
    if (!(await owns(event.thread.id))) return {};
    return serial('agent.start', async () => {
      const state = (await load())!;
      await collect(state).catch((e) => debug(`GitHub feedback collection failed: ${e}`));
      const content = digest(state, `[rust-port] Follow ${POLICY}. ${summary(state)}`);
      await save(state);
      return { message: { content, display: true } };
    });
  });

  // Continues only while the worker can act without a human: not blocked, budget left, and the
  // last continued turn moved the job. A finished job waiting for CI is not continued either: its
  // GitHub subscription wakes the worker when CI reports.
  amp.on('agent.end', async (event) => {
    if (event.status !== 'done' || !(await owns(event.thread.id))) return;
    return serial('agent.end', async () => {
      const state = (await load())!;
      const { job } = state;
      if (job.retro) return;
      // An unreachable GitHub counts as pending, so the worker sleeps rather than spins.
      if (done(state) && (await collect(state).catch(() => true))) return void (await save(state));
      const now = progress(state);
      const reason =
        stopped(job) ?? (job.continued === now ? 'the last continued turn made no progress' : '');
      if (reason) {
        debug(`stopped: ${reason}`);
        return void amp.ui.notify(`rust-port stopped: ${reason}`).catch(() => {});
      }
      job.continued = now;
      await save(state);
      const userMessage = done(state)
        ? `Every stage is admitted, nothing is outstanding and CI is green. Write the retrospective with ${tool('retro')} as ${POLICY} describes: a self-critical postmortem about the next iteration, not a recap.`
        : 'Continue the rust-port job.';
      // The progress check is the bound; Amp's default cap of 5 would stop a healthy job.
      return { action: 'continue', userMessage, maxContinuations: 1000 };
    });
  });
}
