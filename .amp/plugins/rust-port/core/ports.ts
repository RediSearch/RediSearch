/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/** Everything outside the workflow: the agent host, the forge, the VCS and the project. */
import type { CheckResult } from './checks.ts';
import type { FileChange } from './diff.ts';
import type { Rule } from './gates.ts';
import type { NewFinding } from './job.ts';
import type { Plan } from './plan.ts';
import type { Reviewer } from './review.ts';

export type Stage = string;

/** Where the worker and its reviewers run as threads. */
export interface Host {
  threadUrl(thread: string): string;
  /** The name the worker calls an operation by. */
  toolName(operation: string): string;
  /** A thread under `parent`: a read-only reviewer when given `role`, a worker otherwise. */
  startThread(o: { parent: string; labels: string[]; role?: string }): Promise<string>;
  sendMessage(thread: string, message: string): Promise<void>;
  /** Sends `message` and waits for the reply; aborting `signal` cancels the thread's turn. */
  ask(
    thread: string,
    message: string,
    o: { signal: AbortSignal; timeoutMs: number },
  ): Promise<string>;
}

export interface PR {
  number: number;
  url: string;
  head: string;
}

/** Where the stage PRs open, for one repository. */
export interface Forge {
  /** What keeps the stage PRs from opening from `remote` on `branches`. */
  checkRemote(o: { remote: string; branches: string[] }): Promise<string[]>;
  /** Pushes `candidate` to `branch` and opens the PR from it into `base`, or updates `existing`. */
  publish(o: {
    remote: string;
    branch: string;
    base: string;
    candidate: string;
    title: string;
    body: string;
    existing?: PR;
  }): Promise<PR>;
  /**
   * The review comments and the CI verdict of a stage PR. `posted` are the comments the plugin
   * wrote; `head` is the revision it published.
   */
  fetchFeedback(o: {
    stage: Stage;
    number: number;
    posted: number[];
    head: string;
  }): Promise<{ findings: NewFinding[]; pending: boolean }>;
  /** Replies to a review thread's first comment; the reply's ID. */
  replyToComment(pr: number, comment: number, body: string): Promise<number>;
  resolveThread(thread: string): Promise<void>;
}

/** The checkout the job works in. */
export interface Vcs {
  /** The checked-out revision; throws if the tree is dirty. */
  currentRevision(): Promise<string>;
  resolveRevision(revision: string): Promise<string>;
  /** Fetches `branch` from `remote`; the fetched revision. */
  fetchBranch(remote: string, branch: string): Promise<string>;
  mergeBase(a: string, b: string): Promise<string>;
  /**
   * The unified diff between two revisions, limited to `paths` if given. Renames show as a delete
   * and an add, so every path a change touches is named, unless `renames` asks for them as such.
   */
  diff(from: string, to: string, o?: { paths?: string[]; renames?: boolean }): Promise<string>;
  readFileAt(revision: string, path: string): Promise<string | undefined>;
  listFiles(revision: string): Promise<string[]>;
  applyPatch(patch: string, o?: { reverse: true }): Promise<boolean>;
  /** Puts back what the checks changed or created, ignored files aside; whether anything had. */
  discardChanges(): Promise<boolean>;
}

/** Judges a check that exited 0 from its log, or the files it wrote under `root`. */
export type Judge = (o: {
  check: CheckResult;
  log: string;
  root: string;
}) => Promise<string | undefined> | string | undefined;

/** What a project brings: its stages, layout, rules, runners, judges, reviewers and hooks. */
export interface Profile<X = {}> {
  /** Names the job in labels, messages and its state directory. */
  name: string;
  /** In order; each kind of check a stage's plan must have, and what it must run. */
  stages: { name: Stage; checks: Record<string, string[]> }[];
  /** The language ported from and to; the divergence fields are their lowercase names. */
  languages: { from: string; to: string };
  classify(path: string): 'build' | 'rule' | 'generated' | 'test' | 'source';
  /** The text outside the tests a source file holds, where its language puts tests there. */
  stripTests?(path: string, text: string): string | undefined;
  /** An added line that switches a test off. */
  disabledTestPattern: RegExp;
  rules: Rule[];
  /** The files of the {@link classify} `rule`, as reviewers read them. */
  ruleFiles: string;
  /** What a reproduction may run: a `*` stands for a word. */
  testRunners: string[];
  /** Whether a reproduction command only runs tests with one of {@link testRunners}. */
  isTestCommand(command: string): boolean;
  judges: Record<string, Judge>;
  reviewers: Reviewer[];
  /** What the plan gets wrong about the repository at `base`, once it is well formed. */
  checkPlan?(plan: Plan<X>, o: { root: string; vcs: Vcs; base: string }): Promise<string[]>;
  /** What else rejects a claim before anything runs. */
  checkClaim?(o: {
    root: string;
    vcs: Vcs;
    plan: Plan<X>;
    stage: Stage;
    base: string;
    candidate: string;
    files: FileChange[];
  }): Promise<string[]>;
  /** Sections of the plan the core does not know, as reviewers and PRs read them. */
  describePlan?(plan: Plan<X>): string;
}
