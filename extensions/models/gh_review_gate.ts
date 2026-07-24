/**
 * Swamp extension model `@lucapalmieri1993/gh-review-gate`.
 *
 * Wraps the `gh` CLI (reusing the operator's existing `gh auth` session, no
 * stored token) to drive the "ready → PR → CI" portion of a review gate:
 *
 *   - `open_pr`     idempotently opens a pull request for a pushed branch,
 *                   reusing an existing open PR for the same head if present.
 *   - `wait_for_ci` polls the PR's checks until they settle, failing the step
 *                   (throwing) if any check is not green so downstream steps
 *                   (e.g. "notify a human") do not run on a red build.
 *
 * No existing extension covers open-PR / watch-CI via `gh` (only list-PR
 * models exist), so this is a purpose-built local model. See the repo's swamp
 * setup notes for the tracked feature-request gap.
 *
 * @module
 */
import { z } from "npm:zod@4";

const GlobalArgsSchema = z.object({
  workingDir: z
    .string()
    .optional()
    .describe(
      "Directory to run `gh` in; when set, gh auto-detects the repo from it.",
    ),
  repo: z
    .string()
    .optional()
    .describe(
      "Explicit GitHub repo as `owner/name`; overrides workingDir detection.",
    ),
});

type GlobalArgs = z.infer<typeof GlobalArgsSchema>;

const PrResultSchema = z.object({
  number: z.number().int(),
  url: z.string(),
  head: z.string(),
  base: z.string(),
  title: z.string(),
  // true if this run created the PR, false if an open PR already existed.
  created: z.boolean(),
});

const CheckSchema = z.object({
  name: z.string(),
  bucket: z.string(),
  state: z.string(),
  link: z.string(),
});

const CiResultSchema = z.object({
  number: z.number().int(),
  // "success" | "failure" | "timeout"
  conclusion: z.string(),
  passed: z.boolean(),
  checks: z.array(CheckSchema),
  polledSeconds: z.number().int(),
});

/** Context shape provided by the swamp runtime to `execute`. */
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
    error: (msg: string, props?: Record<string, unknown>) => void;
  };
}

/** Run `gh` with the given args, returning stdout; throws on non-zero exit. */
async function runGh(
  args: string[],
  globalArgs: GlobalArgs,
  cwd?: string,
): Promise<string> {
  const finalArgs = globalArgs.repo
    ? [args[0], args[1], "--repo", globalArgs.repo, ...args.slice(2)]
    : args;
  const cmd = new Deno.Command("gh", {
    args: finalArgs,
    // Prefer an explicit workingDir; otherwise run in the repo root that swamp
    // resolved (context.repoDir), so no absolute path is baked in.
    cwd: globalArgs.workingDir ?? cwd,
    stdout: "piped",
    stderr: "piped",
  });
  const output = await cmd.output();
  const stdout = new TextDecoder().decode(output.stdout);
  if (!output.success) {
    const stderr = new TextDecoder().decode(output.stderr);
    throw new Error(
      `gh ${finalArgs.join(" ")} failed (exit ${output.code}): ${
        stderr.trim() || stdout.trim()
      }`,
    );
  }
  return stdout;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Reject branch names that aren't a safe flat token: no slashes (swamp instance
 * names map to disk paths), no leading `-` (CLI option injection), no shell
 * metacharacters. Matches the workflows' validate-head guard exactly.
 */
function assertSafeBranch(head: string): void {
  if (!/^[A-Za-z0-9_][A-Za-z0-9._-]*$/.test(head)) {
    throw new Error(`unsafe branch name: ${JSON.stringify(head)}`);
  }
}

/**
 * Resolve a PR number from an explicit `number` or by looking it up from the
 * `head` branch. Uses `gh pr list --head <b> --state open` (an exact open-PR
 * match) rather than `gh pr view <b>`, which would also match a CLOSED PR or
 * misread a numeric branch name as a PR number.
 */
async function resolvePrNumber(
  args: { number?: number; head?: string },
  globalArgs: GlobalArgs,
  cwd?: string,
): Promise<number> {
  if (typeof args.number === "number") return args.number;
  if (!args.head) {
    throw new Error("expected either `number` or `head` to identify the PR");
  }
  assertSafeBranch(args.head);
  const raw = await runGh(
    ["pr", "list", "--head", args.head, "--state", "open", "--json", "number"],
    globalArgs,
    cwd,
  );
  const list = JSON.parse(raw) as Array<{ number: number }>;
  if (list.length === 0) {
    throw new Error(`no open PR found for head '${args.head}'`);
  }
  return list[0].number;
}

/** Model definition for the gh-backed review gate. */
export const model = {
  type: "@lucapalmieri1993/gh-review-gate",
  version: "2026.07.22.5",
  globalArguments: GlobalArgsSchema,
  resources: {
    pr: {
      description: "The opened (or reused) pull request",
      schema: PrResultSchema,
      lifetime: "infinite" as const,
      garbageCollection: 10,
    },
    ci: {
      description: "The settled CI check status for the pull request",
      schema: CiResultSchema,
      lifetime: "infinite" as const,
      garbageCollection: 10,
    },
  },
  methods: {
    open_pr: {
      description:
        "Idempotently open a PR for a pushed branch, reusing an existing open PR for the same head.",
      arguments: z.object({
        head: z
          .string()
          .min(1)
          .describe("Head branch (already pushed to the remote)."),
        title: z.string().min(1).describe("PR title."),
        base: z
          .string()
          .optional()
          .describe("Base branch; defaults to the repo's default branch."),
        body: z.string().default("").describe("PR body."),
        draft: z.boolean().default(false).describe("Open as a draft PR."),
      }),
      execute: async (
        args: {
          head: string;
          title: string;
          base?: string;
          body: string;
          draft: boolean;
        },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs, logger } = context;
        assertSafeBranch(args.head);

        // 1. Reuse an existing open PR for this head, if any.
        const existingRaw = await runGh(
          ["pr", "list", "--head", args.head, "--state", "open", "--json",
            "number,url,title,baseRefName"],
          globalArgs,
          context.repoDir,
        );
        const existing = JSON.parse(existingRaw) as Array<{
          number: number;
          url: string;
          title: string;
          baseRefName: string;
        }>;

        let prData: z.infer<typeof PrResultSchema>;
        if (existing.length > 0) {
          const pr = existing[0];
          logger.info("Reusing existing open PR #{number} for {head}", {
            number: pr.number,
            head: args.head,
          });
          prData = {
            number: pr.number,
            url: pr.url,
            head: args.head,
            base: pr.baseRefName,
            title: pr.title,
            created: false,
          };
        } else {
          // 2. Create a new PR.
          const createArgs = [
            "pr", "create",
            "--head", args.head,
            "--title", args.title,
            "--body", args.body,
          ];
          if (args.base) createArgs.push("--base", args.base);
          if (args.draft) createArgs.push("--draft");
          await runGh(createArgs, globalArgs, context.repoDir);

          // gh pr create prints the URL but not structured data; fetch it.
          const viewRaw = await runGh(
            ["pr", "view", args.head, "--json",
              "number,url,title,baseRefName"],
            globalArgs,
            context.repoDir,
          );
          const view = JSON.parse(viewRaw) as {
            number: number;
            url: string;
            title: string;
            baseRefName: string;
          };
          logger.info("Opened PR #{number} for {head}", {
            number: view.number,
            head: args.head,
          });
          prData = {
            number: view.number,
            url: view.url,
            head: args.head,
            base: view.baseRefName,
            title: view.title,
            created: true,
          };
        }

        const handle = await context.writeResource("pr", "pr", prData);
        return { dataHandles: [handle] };
      },
    },

    wait_for_ci: {
      description:
        "Poll a PR's checks until they settle; throw if any check is not green.",
      arguments: z.object({
        head: z
          .string()
          .optional()
          .describe("Head branch to resolve the PR from (alternative to number)."),
        number: z
          .number()
          .int()
          .optional()
          .describe("PR number to watch (alternative to head)."),
        // Default sized for slow pipelines (RediSearch CI is ~70 min); 2h gives
        // headroom for queue delays. Override per-step for faster projects.
        timeoutSeconds: z
          .number()
          .int()
          .positive()
          .default(7200)
          .describe("Give up after this many seconds."),
        pollIntervalSeconds: z
          .number()
          .int()
          .positive()
          .default(90)
          .describe("Seconds between poll attempts."),
        requireChecks: z
          .boolean()
          .default(true)
          .describe("Fail if the PR reports no checks at all."),
      }),
      execute: async (
        args: {
          head?: string;
          number?: number;
          timeoutSeconds: number;
          pollIntervalSeconds: number;
          requireChecks: boolean;
        },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs, logger } = context;
        const prNumber = await resolvePrNumber(args, globalArgs, context.repoDir);
        const deadline = Date.now() + args.timeoutSeconds * 1000;
        const started = Date.now();
        const prRef = String(prNumber);

        let checks: Array<z.infer<typeof CheckSchema>> = [];
        let conclusion = "timeout";
        let passed = false;

        while (Date.now() < deadline) {
          // `gh pr checks` exits non-zero when checks are pending or failing,
          // so read the JSON regardless of exit code by tolerating failure.
          let raw: string;
          try {
            raw = await runGh(
              ["pr", "checks", prRef, "--json",
                "name,bucket,state,link"],
              globalArgs,
              context.repoDir,
            );
          } catch (err) {
            // Non-zero exit with JSON on stdout still throws in runGh, so parse
            // the message tail when it looks like JSON; otherwise re-check.
            const msg = err instanceof Error ? err.message : String(err);
            const jsonStart = msg.indexOf("[");
            if (jsonStart === -1) {
              // No checks yet reported — keep waiting unless it's a hard error.
              if (msg.includes("no checks reported")) {
                if (!args.requireChecks) {
                  conclusion = "success";
                  passed = true;
                  break;
                }
                logger.info("No checks reported yet for PR #{number}", {
                  number: prNumber,
                });
                await sleep(args.pollIntervalSeconds * 1000);
                continue;
              }
              throw err;
            }
            raw = msg.slice(jsonStart);
          }

          checks = JSON.parse(raw) as Array<z.infer<typeof CheckSchema>>;
          if (checks.length === 0) {
            if (!args.requireChecks) {
              conclusion = "success";
              passed = true;
              break;
            }
            await sleep(args.pollIntervalSeconds * 1000);
            continue;
          }

          const pending = checks.filter((c) =>
            c.bucket === "pending" || c.state === "PENDING" ||
            c.state === "QUEUED" || c.state === "IN_PROGRESS"
          );

          if (pending.length === 0) {
            // Green requires (a) every settled check is pass or an intentional
            // skip, AND (b) at least one actual pass. Otherwise a PR whose
            // required checks all report "skipping"/"neutral" (or unknown) would
            // be marked green with no CI having actually run.
            const notGreen = checks.filter(
              (c) => c.bucket !== "pass" && c.bucket !== "skipping",
            );
            const passes = checks.filter((c) => c.bucket === "pass");
            passed = notGreen.length === 0 && passes.length > 0;
            conclusion = passed ? "success" : "failure";
            break;
          }

          logger.info(
            "PR #{number}: {pending}/{total} checks pending, waiting {interval}s",
            {
              number: prNumber,
              pending: pending.length,
              total: checks.length,
              interval: args.pollIntervalSeconds,
            },
          );
          await sleep(args.pollIntervalSeconds * 1000);
        }

        const polledSeconds = Math.round((Date.now() - started) / 1000);

        // Persist the (correct) CI result even on failure, then fail the step
        // so downstream "notify" steps do not run on a red or timed-out build.
        await context.writeResource("ci", "ci", {
          number: prNumber,
          conclusion,
          passed,
          checks,
          polledSeconds,
        });

        if (!passed) {
          const failedNames = checks
            .filter((c) => c.bucket === "fail" || c.bucket === "cancel")
            .map((c) => c.name);
          throw new Error(
            conclusion === "timeout"
              ? `CI did not settle within ${args.timeoutSeconds}s for PR #${prNumber}`
              : `CI failed for PR #${prNumber}: ${
                failedNames.join(", ") || "unknown checks"
              }`,
          );
        }

        logger.info("CI passed for PR #{number} ({checks} checks)", {
          number: prNumber,
          checks: checks.length,
        });
        return { dataHandles: [] };
      },
    },

    mark_ready: {
      description:
        "Mark a draft PR as ready for review (gh pr ready). No-op if already ready.",
      arguments: z.object({
        head: z
          .string()
          .optional()
          .describe("Head branch to resolve the PR from (alternative to number)."),
        number: z
          .number()
          .int()
          .optional()
          .describe("PR number to mark ready (alternative to head)."),
      }),
      execute: async (
        args: { head?: string; number?: number },
        context: ExecContext,
      ): Promise<{ dataHandles: unknown[] }> => {
        const { globalArgs, logger } = context;
        const prNumber = await resolvePrNumber(args, globalArgs, context.repoDir);
        await runGh(["pr", "ready", String(prNumber)], globalArgs, context.repoDir);
        logger.info("Marked PR #{number} ready for review", {
          number: prNumber,
        });
        return { dataHandles: [] };
      },
    },
  },
};
