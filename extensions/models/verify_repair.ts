/**
 * Swamp extension model `@lucapalmieri1993/verify-repair`.
 *
 * A coordinator-free, bounded verify->repair->re-verify loop (the same Garfield
 * pattern as `adversarial-review`, but the "check" is a deterministic shell
 * verification command rather than an LLM reviewer). Runs the verify command;
 * on failure an AI repair actor edits the workspace to fix it; re-verifies, up
 * to maxVerifyRuns / maxActorCalls. Converges to a green verify, or throws so
 * the gate escalates to a human instead of silently aborting the run.
 *
 * The loop control is deterministic; the LLM is invoked only to repair, a
 * bounded number of times.
 *
 * @module
 */
import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  workingDir: z
    .string()
    .optional()
    .describe("Directory to run verify/repair in (defaults to the repo root)."),
  repairModel: z
    .string()
    .optional()
    .describe("Optional claude model id for the repair actor."),
  extraClaudeArgs: z
    .array(z.string())
    .default([])
    .describe(
      "Flags appended to the repair `claude -p` call (e.g. permission flags). The repair actor needs edit + Bash permissions to work headlessly.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const ResultSchema = z.object({
  passed: z.boolean(),
  converged: z.boolean(),
  verifyRuns: z.number().int(),
  actorCalls: z.number().int(),
  lastExitCode: z.number().int(),
  lastOutputTail: z.string(),
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

const TAIL = 8000;
const tail = (s: string) => (s.length > TAIL ? s.slice(-TAIL) : s);

/** Run a shell command, returning combined output + exit code WITHOUT throwing. */
async function runShell(
  command: string,
  cwd?: string,
): Promise<{ code: number; output: string }> {
  const cmd = new Deno.Command("bash", {
    args: ["-c", command],
    cwd,
    stdout: "piped",
    stderr: "piped",
  });
  const out = await cmd.output();
  const dec = new TextDecoder();
  return {
    code: out.code,
    output: dec.decode(out.stdout) + dec.decode(out.stderr),
  };
}

/**
 * Run the repair actor via `claude -p`, keeping ONE session across the loop so
 * it retains context (loaded files + what it already tried) instead of
 * cold-starting each iteration. First call opens the session with a fixed uuid
 * (`--session-id`); later calls continue it (`--resume`). Throws on non-zero.
 */
async function runRepairAgent(
  prompt: string,
  g: GlobalArgs,
  cwd: string | undefined,
  sessionId: string,
  resume: boolean,
): Promise<string> {
  const args = ["-p"];
  args.push(resume ? "--resume" : "--session-id", sessionId);
  if (g.repairModel) args.push("--model", g.repairModel);
  args.push(...g.extraClaudeArgs);
  args.push(prompt);
  const cmd = new Deno.Command("claude", {
    args,
    cwd,
    stdout: "piped",
    stderr: "piped",
  });
  const out = await cmd.output();
  const dec = new TextDecoder();
  if (!out.success) {
    throw new Error(
      `claude repair failed (exit ${out.code}): ${
        dec.decode(out.stderr).trim() || dec.decode(out.stdout).trim()
      }`,
    );
  }
  return dec.decode(out.stdout);
}

/** Model definition for the bounded verify+repair loop. */
export const model = {
  type: "@lucapalmieri1993/verify-repair",
  version: "2026.07.22.3",
  globalArguments: GlobalArgsSchema,
  resources: {
    result: {
      description: "Outcome of the bounded verify+repair loop.",
      schema: ResultSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
  },
  methods: {
    run: {
      description:
        "Bounded verify->repair->re-verify loop; converge to a green verify or throw to escalate.",
      arguments: z.object({
        verifyCommand: z
          .string()
          .min(1)
          .describe("Shell command whose zero exit means 'verification passed'."),
        maxVerifyRuns: z.number().int().min(1).max(10).default(4),
        maxActorCalls: z.number().int().min(0).max(5).default(2),
        guidance: z
          .string()
          .default("")
          .describe("Optional human steer for the repair actor (used on restart)."),
      }),
      execute: async (
        args: {
          verifyCommand: string;
          maxVerifyRuns: number;
          maxActorCalls: number;
          guidance: string;
        },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs: g, logger } = context;
        const cwd = g.workingDir ?? context.repoDir;

        // One repair session for the whole loop, so the actor keeps context
        // across attempts (no cold-start re-read; remembers what it tried).
        const repairSession = crypto.randomUUID();
        let verifyRuns = 0;
        let actorCalls = 0;
        let converged = false;
        let lastCode = 1;
        let lastOut = "";

        while (verifyRuns < args.maxVerifyRuns) {
          verifyRuns++;
          logger.info("Verify run {n} (repairs so far: {r})", {
            n: verifyRuns,
            r: actorCalls,
          });
          const res = await runShell(args.verifyCommand, cwd);
          lastCode = res.code;
          lastOut = res.output;

          if (res.code === 0) {
            converged = true;
            break;
          }
          logger.warning("Verify failed (exit {code}) on run {n}", {
            code: res.code,
            n: verifyRuns,
          });
          if (actorCalls >= args.maxActorCalls) break;
          if (verifyRuns >= args.maxVerifyRuns) break;

          actorCalls++;
          logger.info("Repair attempt {n}: asking the actor to fix verify", {
            n: actorCalls,
          });
          const first = actorCalls === 1;
          const guidanceBlock = args.guidance.trim()
            ? `Human guidance for this attempt (follow it):\n${args.guidance.trim()}\n\n`
            : "";
          const prompt = first
            ? guidanceBlock +
              `Local verification failed. The command was:\n\n${args.verifyCommand}\n\n` +
              `Its output (tail):\n\n${tail(res.output)}\n\n` +
              `Edit the code in this repository to make that command pass. Make the ` +
              `smallest changes that fix the failures while preserving intent. Do not ` +
              `spawn subagents, do not commit or push. When done, briefly summarize ` +
              `what you changed.`
            : `Verification still fails after your last change. Latest output ` +
              `(tail):\n\n${tail(res.output)}\n\n` +
              `Keep fixing — do not repeat approaches that already failed, and do ` +
              `not revert the progress you have made.`;
          // First attempt opens the session; later attempts resume it.
          await runRepairAgent(prompt, g, cwd, repairSession, !first);
        }

        await context.writeResource("result", "result", {
          passed: converged,
          converged,
          verifyRuns,
          actorCalls,
          lastExitCode: lastCode,
          lastOutputTail: tail(lastOut),
          guidance: args.guidance,
          repairedAt: new Date().toISOString(),
        });

        if (!converged) {
          throw new Error(
            `Local verification did not pass after ${verifyRuns} run(s) / ` +
              `${actorCalls} repair attempt(s) (last exit ${lastCode}). ` +
              `Inspect the failure in this run's logs, or the 'result' resource ` +
              `of this verify-repair instance (swamp data get <instance> result --json).`,
          );
        }
        logger.info("Verify PASSED after {r} run(s), {a} repair(s)", {
          r: verifyRuns,
          a: actorCalls,
        });
        return { dataHandles: [] };
      },
    },
  },
};
