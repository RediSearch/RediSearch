/**
 * Swamp extension model `@lucapalmieri1993/ci-repair`.
 *
 * A coordinator-free, bounded CI-wait -> repair -> re-wait loop (same Garfield
 * pattern as `verify-repair`, but the "check" is GitHub CI). Combines gh (poll
 * checks), claude (repair actor), and jj (push):
 *
 *   loop (<= maxCiRuns):
 *     wait for CI on the PR, FAILING FAST — return as soon as any blocking check
 *       goes red, without waiting for the other (~70 min) checks to settle, so a
 *       repair starts at ~10 min not ~70. Green still costs the full CI.
 *     if green -> converged.
 *     else a repair actor (one continuous claude session across attempts, with
 *       human `guidance` in its first prompt) fetches the failing logs via gh and
 *       fixes the code; we `jj git push` (triggering a fresh CI run) and re-wait.
 *
 * Converges to green, or throws so the workflow escalates to a human.
 * Bounded by maxCiRuns / maxActorCalls. Emits a typed `result` resource.
 *
 * @module
 */
import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  workingDir: z.string().optional().describe("Dir to run gh/jj/claude in (repo root)."),
  repo: z.string().optional().describe("Explicit owner/name; else gh auto-detects."),
  repairModel: z.string().optional().describe("Optional claude model for repair."),
  extraClaudeArgs: z
    .array(z.string())
    .default([])
    .describe("Flags for the repair `claude -p` (needs edit + Bash perms headless)."),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const CheckSchema = z.object({
  name: z.string(),
  bucket: z.string(),
  state: z.string(),
  link: z.string(),
});

const ResultSchema = z.object({
  number: z.number().int(),
  conclusion: z.string(), // "success" | "failure" | "timeout"
  passed: z.boolean(),
  converged: z.boolean(),
  ciRuns: z.number().int(),
  actorCalls: z.number().int(),
  failedChecks: z.array(z.string()),
  guidance: z.string(),
  repairedAt: z.string(),
});

interface ExecContext {
  globalArgs: GlobalArgs;
  repoDir?: string;
  writeResource: (
    specName: string,
    name: string,
    data: Record<string, unknown>,
  ) => Promise<{ name: string }>;
  logger: {
    info: (msg: string, props?: Record<string, unknown>) => void;
    warning: (msg: string, props?: Record<string, unknown>) => void;
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Run a command, returning {code, stdout, stderr} WITHOUT throwing. */
async function runRaw(
  bin: string,
  args: string[],
  cwd?: string,
): Promise<{ code: number; stdout: string; stderr: string }> {
  const out = await new Deno.Command(bin, {
    args,
    cwd,
    stdout: "piped",
    stderr: "piped",
  }).output();
  const dec = new TextDecoder();
  return {
    code: out.code,
    stdout: dec.decode(out.stdout),
    stderr: dec.decode(out.stderr),
  };
}

function ghArgs(base: string[], g: GlobalArgs): string[] {
  return g.repo ? [base[0], base[1], "--repo", g.repo, ...base.slice(2)] : base;
}

/**
 * Reject branch names that aren't a safe flat token: no slashes (swamp instance
 * names map to disk paths), no leading `-`, no shell metacharacters. Matches the
 * workflows' validate-head guard.
 */
function assertSafeBranch(head: string): void {
  if (!/^[A-Za-z0-9_][A-Za-z0-9._-]*$/.test(head)) {
    throw new Error(`unsafe branch name: ${JSON.stringify(head)}`);
  }
}

/**
 * Resolve the PR number from a head branch. Uses `gh pr list --head <b> --state
 * open` (exact open-PR match) rather than `gh pr view <b>`, which would also
 * match a CLOSED PR or misread a numeric branch name as a PR number.
 */
async function resolvePrNumber(
  head: string,
  g: GlobalArgs,
  cwd?: string,
): Promise<number> {
  assertSafeBranch(head);
  const r = await runRaw(
    "gh",
    ghArgs(["pr", "list", "--head", head, "--state", "open", "--json", "number"], g),
    cwd,
  );
  if (r.code !== 0) {
    throw new Error(`gh pr list --head ${head} failed: ${r.stderr.trim() || r.stdout.trim()}`);
  }
  const list = JSON.parse(r.stdout) as Array<{ number: number }>;
  if (list.length === 0) {
    throw new Error(`no open PR found for head '${head}'`);
  }
  return list[0].number as number;
}

type WaitOutcome = {
  conclusion: "success" | "failure" | "timeout";
  passed: boolean;
  failedChecks: string[];
};

/**
 * Poll the PR's checks until they settle green, or FAIL FAST the instant any
 * check goes red (bucket fail/cancel) even while others are still pending.
 */
async function waitForCiFailFast(
  prNumber: number,
  timeoutSeconds: number,
  pollIntervalSeconds: number,
  g: GlobalArgs,
  cwd: string | undefined,
  logger: ExecContext["logger"],
): Promise<WaitOutcome> {
  const deadline = Date.now() + timeoutSeconds * 1000;
  const prRef = String(prNumber);
  while (Date.now() < deadline) {
    const r = await runRaw(
      "gh",
      ghArgs(["pr", "checks", prRef, "--json", "name,bucket,state,link"], g),
      cwd,
    );
    // gh pr checks exits non-zero while checks are pending/failing; parse the
    // JSON from stdout regardless. "no checks reported" => treat as pending.
    let checks: z.infer<typeof CheckSchema>[] = [];
    const jsonStart = r.stdout.indexOf("[");
    if (jsonStart !== -1) {
      try {
        checks = JSON.parse(r.stdout.slice(jsonStart)) as typeof checks;
      } catch {
        checks = [];
      }
    }

    const failed = checks.filter((c) => c.bucket === "fail" || c.bucket === "cancel");
    if (failed.length > 0) {
      // FAIL FAST: don't wait for the rest of the (~70 min) suite.
      return {
        conclusion: "failure",
        passed: false,
        failedChecks: failed.map((c) => c.name),
      };
    }
    const pending = checks.filter(
      (c) =>
        c.bucket === "pending" || c.state === "PENDING" ||
        c.state === "QUEUED" || c.state === "IN_PROGRESS",
    );
    if (checks.length > 0 && pending.length === 0) {
      // Settled. Green requires every check to be pass/skipping AND at least
      // one actual pass — otherwise an all-"skipping" (or unknown-bucket) PR
      // would be marked green with no CI having really run.
      const notGreen = checks.filter(
        (c) => c.bucket !== "pass" && c.bucket !== "skipping",
      );
      const passes = checks.filter((c) => c.bucket === "pass");
      if (notGreen.length === 0 && passes.length > 0) {
        return { conclusion: "success", passed: true, failedChecks: [] };
      }
      return {
        conclusion: "failure",
        passed: false,
        failedChecks: notGreen.length > 0
          ? notGreen.map((c) => c.name)
          : ["(no passing checks — all skipped?)"],
      };
    }
    logger.info("PR #{n}: {p}/{t} checks pending, waiting {i}s", {
      n: prNumber,
      p: pending.length,
      t: checks.length,
      i: pollIntervalSeconds,
    });
    await sleep(pollIntervalSeconds * 1000);
  }
  return { conclusion: "timeout", passed: false, failedChecks: [] };
}

/** Repair actor: fix CI. One continuous session across attempts. */
async function repairCi(
  prNumber: number,
  failedChecks: string[],
  guidance: string,
  g: GlobalArgs,
  cwd: string | undefined,
  sessionId: string,
  resume: boolean,
): Promise<void> {
  const guidanceBlock = !resume && guidance.trim()
    ? `Human guidance for this run (follow it):\n${guidance.trim()}\n\n`
    : "";
  const prompt = resume
    ? `CI is still red on PR #${prNumber} after your last change. Failing checks: ` +
      `${failedChecks.join(", ") || "unknown"}. Use gh to fetch the latest failing ` +
      `logs and fix the remaining problems without repeating approaches that ` +
      `already failed or reverting prior progress. Do not commit or push.`
    : guidanceBlock +
      `GitHub CI failed on pull request #${prNumber}. Failing checks: ` +
      `${failedChecks.join(", ") || "unknown"}. Use gh (e.g. \`gh pr checks ` +
      `${prNumber}\`, \`gh run view --log-failed\`) to fetch the failing logs, ` +
      `diagnose the root cause, and edit the code so CI will pass. Make the ` +
      `smallest changes that fix it; preserve intent. Do not spawn subagents, and ` +
      `do not commit or push. When done, briefly summarize what you changed.`;
  const args = ["-p"];
  args.push(resume ? "--resume" : "--session-id", sessionId);
  if (g.repairModel) args.push("--model", g.repairModel);
  args.push(...g.extraClaudeArgs);
  args.push(prompt);
  const r = await runRaw("claude", args, cwd);
  if (r.code !== 0) {
    throw new Error(`claude CI-repair failed (exit ${r.code}): ${r.stderr.trim()}`);
  }
}

/** Model definition for the bounded CI wait+repair loop. */
export const model = {
  type: "@lucapalmieri1993/ci-repair",
  version: "2026.07.22.3",
  globalArguments: GlobalArgsSchema,
  resources: {
    result: {
      description: "Outcome of the bounded CI wait+repair loop.",
      schema: ResultSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
  },
  methods: {
    run: {
      description:
        "Wait for CI (fail-fast) and auto-repair failures in a bounded loop; converge green or throw to escalate.",
      arguments: z.object({
        head: z.string().min(1).describe("Head branch whose PR CI to watch/repair."),
        guidance: z
          .string()
          .default("")
          .describe("Optional human steer for the repair actor (used on restart)."),
        maxCiRuns: z.number().int().min(1).max(10).default(3),
        maxActorCalls: z.number().int().min(0).max(5).default(2),
        timeoutSeconds: z.number().int().positive().default(7200),
        pollIntervalSeconds: z.number().int().positive().default(90),
      }),
      execute: async (
        args: {
          head: string;
          guidance: string;
          maxCiRuns: number;
          maxActorCalls: number;
          timeoutSeconds: number;
          pollIntervalSeconds: number;
        },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs: g, logger } = context;
        const cwd = g.workingDir ?? context.repoDir;
        const prNumber = await resolvePrNumber(args.head, g, cwd);
        const repairSession = crypto.randomUUID();

        let ciRuns = 0;
        let actorCalls = 0;
        let converged = false;
        let conclusion = "timeout";
        let failedChecks: string[] = [];

        while (ciRuns < args.maxCiRuns) {
          ciRuns++;
          logger.info("CI wait {n} for PR #{pr} (repairs so far: {r})", {
            n: ciRuns,
            pr: prNumber,
            r: actorCalls,
          });
          const outcome = await waitForCiFailFast(
            prNumber,
            args.timeoutSeconds,
            args.pollIntervalSeconds,
            g,
            cwd,
            logger,
          );
          conclusion = outcome.conclusion;
          failedChecks = outcome.failedChecks;

          if (outcome.passed) {
            converged = true;
            break;
          }
          logger.warning("CI {c} for PR #{pr}: {f}", {
            c: conclusion,
            pr: prNumber,
            f: failedChecks.join(", ") || "(timeout)",
          });
          if (actorCalls >= args.maxActorCalls) break;
          if (ciRuns >= args.maxCiRuns) break;

          actorCalls++;
          logger.info("CI-repair attempt {n} for PR #{pr}", {
            n: actorCalls,
            pr: prNumber,
          });
          await repairCi(
            prNumber,
            failedChecks,
            args.guidance,
            g,
            cwd,
            repairSession,
            actorCalls > 1,
          );

          // Push the fix (triggers a fresh CI run on the new head SHA).
          // `--bookmark=<head>` (equals form) so a `-`-leading branch can't be
          // parsed as a flag.
          const push = await runRaw("jj", ["git", "push", "--bookmark=" + args.head], cwd);
          if (push.code !== 0) {
            throw new Error(`jj git push failed: ${push.stderr.trim()}`);
          }
          // Give the new CI run a moment to register on the new SHA before we
          // re-poll, so we don't read the just-superseded run's state.
          await sleep(args.pollIntervalSeconds * 1000);
        }

        await context.writeResource("result", "result", {
          number: prNumber,
          conclusion,
          passed: converged,
          converged,
          ciRuns,
          actorCalls,
          failedChecks,
          guidance: args.guidance,
          repairedAt: new Date().toISOString(),
        });

        if (!converged) {
          throw new Error(
            `CI did not go green for PR #${prNumber} after ${ciRuns} wait(s) / ` +
              `${actorCalls} repair(s) (last: ${conclusion}${
                failedChecks.length ? " — " + failedChecks.join(", ") : ""
              }). Inspect: swamp data get ci-repair-${args.head} result --json`,
          );
        }
        logger.info("CI green for PR #{pr} after {r} wait(s), {a} repair(s)", {
          pr: prNumber,
          r: ciRuns,
          a: actorCalls,
        });
        return { dataHandles: [] };
      },
    },
  },
};
