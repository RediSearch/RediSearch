/**
 * Swamp extension model `@lucapalmieri1993/adversarial-review`.
 *
 * A coordinator-free, bounded review+repair loop (Garfield pattern) implemented
 * as ordinary TypeScript inside the method — the loop control is deterministic,
 * the LLM is invoked a bounded number of times, and there is no coordinator
 * agent. Two methods:
 *
 *   - `run`     Bounded loop: review the local branch diff -> if findings, a
 *               repair actor edits the workspace -> re-review, up to
 *               maxReviewCalls / maxActorCalls. Converges to PASS or returns
 *               unresolved findings (throws) so the gate escalates to a human.
 *   - `analyze` A single review pass (no repair) for quick manual/iterate checks.
 *
 * Both emit a TYPED, versioned `findings` resource keyed by branch and thread
 * the prior round's findings into the next review ("verify these are fixed,
 * then scan for new"). Reviews the LOCAL working copy — nothing is pushed here;
 * the workflow pushes the repaired branch once after the loop converges.
 *
 * @module
 */
import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  workingDir: z
    .string()
    .optional()
    .describe("Repo checkout to review/repair in."),
  base: z
    .string()
    .optional()
    .describe("Base branch to diff against; defaults to the repo trunk."),
  reviewModel: z
    .string()
    .optional()
    .describe("Optional claude model id for the reviewer (model tiering)."),
  repairModel: z
    .string()
    .optional()
    .describe("Optional claude model id for the repair actor."),
  extraClaudeArgs: z
    .array(z.string())
    .default([])
    .describe(
      "Flags appended to every `claude -p` call (e.g. permission / allowed-tools flags). The repair actor needs edit + Bash permissions to work headlessly.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const FindingSchema = z.object({
  severity: z.string().describe("blocking | nit | note"),
  file: z.string().nullable(),
  line: z.number().int().nullable(),
  summary: z.string(),
  evidence: z.string().nullable().describe("Quoted code / file:line grounding."),
});

const FindingsResultSchema = z.object({
  branch: z.string(),
  verdict: z.string(), // "pass" | "fail"
  passed: z.boolean(),
  converged: z.boolean(),
  reviewCalls: z.number().int(),
  actorCalls: z.number().int(),
  findings: z.array(FindingSchema),
  priorCount: z.number().int(),
  guidance: z.string(),
  reviewedAt: z.string(),
  raw: z.string(),
});

type FindingsResult = z.infer<typeof FindingsResultSchema>;
type Finding = z.infer<typeof FindingSchema>;

interface ExecContext {
  globalArgs: GlobalArgs;
  repoDir?: string;
  writeResource: (
    specName: string,
    name: string,
    data: Record<string, unknown>,
  ) => Promise<{ name: string }>;
  readResource?: (
    instanceName: string,
    version?: number,
  ) => Promise<Record<string, unknown> | null>;
  logger: {
    info: (msg: string, props?: Record<string, unknown>) => void;
    warning: (msg: string, props?: Record<string, unknown>) => void;
  };
}

/** Run a command, returning stdout; throws on non-zero exit. */
async function run(
  bin: string,
  args: string[],
  cwd?: string,
): Promise<string> {
  const cmd = new Deno.Command(bin, {
    args,
    cwd,
    stdout: "piped",
    stderr: "piped",
  });
  const out = await cmd.output();
  const stdout = new TextDecoder().decode(out.stdout);
  if (!out.success) {
    const stderr = new TextDecoder().decode(out.stderr);
    throw new Error(
      `${bin} ${args[0]} failed (exit ${out.code}): ${
        stderr.trim() || stdout.trim()
      }`,
    );
  }
  return stdout;
}

/** Extract the last ```json fenced block from text and parse verdict+findings. */
function parseReview(text: string): { verdict: string; findings: Finding[] } {
  const matches = [...text.matchAll(/```json\s*([\s\S]*?)```/g)];
  if (matches.length === 0) throw new Error("no json block in review output");
  const obj = JSON.parse(matches[matches.length - 1][1].trim());
  const verdict = String(obj.verdict ?? "fail").toLowerCase();
  const findings: Finding[] = Array.isArray(obj.findings)
    ? obj.findings.map((f: Record<string, unknown>) => ({
      severity: String(f.severity ?? "note"),
      file: f.file != null ? String(f.file) : null,
      line: typeof f.line === "number" ? f.line : null,
      summary: String(f.summary ?? ""),
      evidence: f.evidence != null ? String(f.evidence) : null,
    }))
    : [];
  return { verdict, findings };
}

/** Keep only blocking findings that carry grounding evidence + a file. */
function groundedBlocking(findings: Finding[]): Finding[] {
  return findings.filter(
    (f) =>
      f.severity.toLowerCase() === "blocking" &&
      f.file != null &&
      f.evidence != null &&
      f.evidence.trim() !== "",
  );
}

function claudeArgs(prompt: string, model: string | undefined, g: GlobalArgs) {
  const a = ["-p", prompt];
  if (model) a.push("--model", model);
  a.push(...g.extraClaudeArgs);
  return a;
}

function priorBlock(prior: FindingsResult | null): string {
  if (!prior || prior.findings.length === 0) return "";
  return (
    "The previous round flagged the following. First verify each is addressed " +
    "in the current diff; report any still unresolved, then look for NEW issues:\n" +
    prior.findings
      .map(
        (f, i) =>
          `${i + 1}. [${f.severity}] ${f.file ?? "?"}:${f.line ?? "?"} — ${f.summary}`,
      )
      .join("\n") +
    "\n\n"
  );
}

/** One reviewer pass over the local branch diff. */
async function reviewOnce(
  head: string,
  base: string,
  prior: FindingsResult | null,
  g: GlobalArgs,
  cwd: string | undefined,
): Promise<{ verdict: string; findings: Finding[]; raw: string }> {
  const prompt =
    `You are an adversarial code reviewer. Review the changes on the current ` +
    `git/jj branch '${head}' relative to the base branch '${base}'. Use the ` +
    `available tools (jj/git) to obtain the diff. Hunt hard for correctness, ` +
    `memory-safety, undefined-behaviour, and security bugs following the repo's ` +
    `/code-review and /rust-review guidelines. Assume bugs exist.\n\n` +
    priorBlock(prior) +
    `End your reply with a fenced json code block containing exactly:\n` +
    "```json\n" +
    `{"verdict": "pass" | "fail", "findings": [{"severity": "blocking" | "nit" | "note", "file": string | null, "line": number | null, "summary": string, "evidence": string | null}]}\n` +
    "```\n" +
    `Set verdict "pass" only when there are no blocking findings. Every ` +
    `blocking finding MUST include a file and an evidence snippet.`;
  const raw = await run("claude", claudeArgs(prompt, g.reviewModel, g), cwd);
  try {
    return { ...parseReview(raw), raw };
  } catch {
    return {
      verdict: "fail",
      raw,
      findings: [
        {
          severity: "blocking",
          file: null,
          line: null,
          summary: "Review output could not be parsed — inspect `raw`.",
          evidence: null,
        },
      ],
    };
  }
}

/**
 * One repair-actor pass. The repair actor keeps ONE session across the loop
 * (first call opens it with `--session-id`, later calls `--resume` it) so it
 * retains context and its own prior attempts instead of cold-starting. The
 * reviewer stays fresh each round (independent reviews are more adversarial).
 */
async function repairOnce(
  head: string,
  base: string,
  findings: Finding[],
  g: GlobalArgs,
  cwd: string | undefined,
  sessionId: string,
  resume: boolean,
  guidance: string,
): Promise<string> {
  const list = findings
    .map(
      (f, i) =>
        `${i + 1}. [${f.severity}] ${f.file ?? "?"}:${f.line ?? "?"} — ${f.summary}` +
        (f.evidence ? `\n   evidence: ${f.evidence}` : ""),
    )
    .join("\n");
  const guidanceBlock = guidance.trim()
    ? `Human guidance for this run (follow it):\n${guidance.trim()}\n\n`
    : "";
  const prompt = resume
    ? `The review still reports blocking findings after your last change on ` +
      `branch '${head}'. Fix these without repeating approaches that already ` +
      `failed or reverting prior progress:\n${list}\n\n` +
      `When done, briefly summarize what you changed.`
    : guidanceBlock +
      `You are the repair actor for branch '${head}' (base '${base}'). Edit the ` +
      `workspace directly to fix the review findings below. Make the smallest ` +
      `changes that resolve each finding while preserving the change's intent. ` +
      `Do not spawn subagents, do not commit or push. Findings:\n${list}\n\n` +
      `When done, briefly summarize what you changed.`;
  const args = ["-p"];
  args.push(resume ? "--resume" : "--session-id", sessionId);
  if (g.repairModel) args.push("--model", g.repairModel);
  args.push(...g.extraClaudeArgs);
  args.push(prompt);
  return await run("claude", args, cwd);
}

function branchKey(head: string): string {
  return "branch-" + head.replace(/[^A-Za-z0-9_.-]/g, "-");
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

async function loadPrior(
  context: ExecContext,
  key: string,
): Promise<FindingsResult | null> {
  if (!context.readResource) return null;
  return (await context.readResource(key)) as FindingsResult | null;
}

/** Model definition for the LLM-backed bounded review+repair loop. */
export const model = {
  type: "@lucapalmieri1993/adversarial-review",
  version: "2026.07.22.5",
  globalArguments: GlobalArgsSchema,
  resources: {
    findings: {
      description: "Typed adversarial-review findings for a branch (latest round).",
      schema: FindingsResultSchema,
      lifetime: "infinite" as const,
      garbageCollection: 20,
    },
  },
  methods: {
    run: {
      description:
        "Bounded review->repair->re-review loop over the local branch diff; converge or return unresolved findings.",
      arguments: z.object({
        head: z.string().min(1).describe("Branch/bookmark under review."),
        base: z.string().optional().describe("Base branch (overrides global)."),
        maxReviewCalls: z.number().int().min(1).max(20).default(6),
        maxActorCalls: z.number().int().min(0).max(5).default(3),
        guidance: z
          .string()
          .default("")
          .describe("Optional human steer for the repair actor (used on restart)."),
      }),
      execute: async (
        args: {
          head: string;
          base?: string;
          maxReviewCalls: number;
          maxActorCalls: number;
          guidance: string;
        },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs: g, logger } = context;
        assertSafeBranch(args.head);
        const base = args.base ?? g.base ?? "main";
        const cwd = g.workingDir ?? context.repoDir;
        const key = branchKey(args.head);
        // One repair session across the whole loop (reviewer stays fresh).
        const repairSession = crypto.randomUUID();

        let prior = await loadPrior(context, key);
        let last: { verdict: string; findings: Finding[]; raw: string } | null =
          null;
        let reviewCalls = 0;
        let actorCalls = 0;
        let converged = false;

        while (reviewCalls < args.maxReviewCalls) {
          reviewCalls++;
          logger.info("Review round {n} (repairs so far: {r})", {
            n: reviewCalls,
            r: actorCalls,
          });
          const review = await reviewOnce(args.head, base, prior, g, cwd);
          last = review;

          const blocking = groundedBlocking(review.findings);
          const passed = review.verdict === "pass" && blocking.length === 0;

          // Persist this round's findings (correct data even mid-loop).
          await context.writeResource("findings", key, {
            branch: args.head,
            verdict: review.verdict,
            passed,
            converged: passed,
            reviewCalls,
            actorCalls,
            findings: review.findings,
            priorCount: prior?.findings.length ?? 0,
            guidance: args.guidance,
            reviewedAt: new Date().toISOString(),
            raw: review.raw,
          });

          if (passed) {
            converged = true;
            break;
          }
          // Out of repair budget, or no re-review budget left -> stop looping.
          if (actorCalls >= args.maxActorCalls) break;
          if (reviewCalls >= args.maxReviewCalls) break;

          actorCalls++;
          logger.info("Repair round {n}: fixing {c} blocking finding(s)", {
            n: actorCalls,
            c: blocking.length || review.findings.length,
          });
          await repairOnce(
            args.head,
            base,
            blocking.length ? blocking : review.findings,
            g,
            cwd,
            repairSession,
            actorCalls > 1, // first repair opens the session; later ones resume it
            args.guidance,
          );
          // Feed this round's findings forward as prior context.
          prior = {
            branch: args.head,
            verdict: review.verdict,
            passed: false,
            converged: false,
            reviewCalls,
            actorCalls,
            findings: review.findings,
            priorCount: prior?.findings.length ?? 0,
            guidance: args.guidance,
            reviewedAt: new Date().toISOString(),
            raw: review.raw,
          };
        }

        if (!converged) {
          // Count all findings, not just grounded-blocking: on a parse failure
          // the synthetic finding has file:null, so groundedBlocking would be 0
          // and the message would misleadingly read "0 findings".
          const n = last ? last.findings.length : 0;
          throw new Error(
            `Adversarial review did not converge after ${reviewCalls} review(s) / ` +
              `${actorCalls} repair(s): ${n} unresolved finding(s) on ` +
              `'${args.head}'. Inspect: swamp data get ai-review-${args.head} findings --json`,
          );
        }
        logger.info("Converged: PASS for '{head}' ({r} reviews, {a} repairs)", {
          head: args.head,
          r: reviewCalls,
          a: actorCalls,
        });
        return { dataHandles: [] };
      },
    },

    analyze: {
      description:
        "Single review pass (no repair) over the local branch diff; emits typed findings.",
      arguments: z.object({
        head: z.string().min(1).describe("Branch/bookmark under review."),
        base: z.string().optional().describe("Base branch (overrides global)."),
      }),
      execute: async (
        args: { head: string; base?: string },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs: g, logger } = context;
        assertSafeBranch(args.head);
        const base = args.base ?? g.base ?? "main";
        const cwd = g.workingDir ?? context.repoDir;
        const key = branchKey(args.head);
        const prior = await loadPrior(context, key);

        logger.info("Single review pass for '{head}' (prior: {p})", {
          head: args.head,
          p: prior?.findings.length ?? 0,
        });
        const review = await reviewOnce(args.head, base, prior, g, cwd);
        const blocking = groundedBlocking(review.findings);
        const passed = review.verdict === "pass" && blocking.length === 0;

        await context.writeResource("findings", key, {
          branch: args.head,
          verdict: review.verdict,
          passed,
          converged: passed,
          reviewCalls: 1,
          actorCalls: 0,
          findings: review.findings,
          priorCount: prior?.findings.length ?? 0,
          guidance: "",
          reviewedAt: new Date().toISOString(),
          raw: review.raw,
        });

        if (!passed) {
          throw new Error(
            `Adversarial review found ${blocking.length} blocking finding(s) on ` +
              `'${args.head}'. Inspect: swamp data get ai-review-${args.head} findings --json`,
          );
        }
        logger.info("Review PASS for '{head}'", { head: args.head });
        return { dataHandles: [] };
      },
    },
  },
};
