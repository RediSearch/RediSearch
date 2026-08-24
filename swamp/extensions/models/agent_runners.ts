/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * The coding agents a phase of the implement-task flow can be run on.
 *
 * Everything about *the flow* — which phases there are, what each one is told,
 * the shape it must answer in, and the gates its answer is held to — lives in
 * `agent_task.ts` and is the same whichever agent runs it. What differs is
 * narrow and mechanical: how the process is invoked, where the answer is found
 * in what it printed, what a run cost, and which failures another attempt might
 * survive. That is what a runner is.
 *
 * Two exist. Claude Code enforces the answer's shape itself, through
 * `--json-schema`; Amp has no equivalent for a CLI turn — its structured output
 * is a plugin-API feature — so the schema goes in the prompt and the reply is
 * parsed and, once, repaired. The difference is real and is stated on
 * {@linkcode AmpRunner}: under Claude the shape is enforced, under Amp it is
 * detected.
 *
 * Both speak the same transcript, which is what makes this a seam at all: `amp
 * --stream-json` emits Claude Code's stream-json events, so the streaming, the
 * progress mirror and the transcript file are shared rather than duplicated.
 *
 * @module
 */

/** The terminal event of a stream-json transcript, as both CLIs emit it. */
export interface ResultEvent {
  is_error?: boolean;
  subtype?: string;
  session_id?: string;
  num_turns?: number;
  total_cost_usd?: number;
  structured_output?: unknown;
  result?: unknown;
  permission_denials?: Array<Record<string, unknown>>;
  /**
   * Why the run ended, which `subtype` does not say.
   *
   * `subtype` is `"success"` on a run the CLI itself flagged `is_error` — an
   * abort on a rate limit reports exactly that — so a message built from it
   * reads "failed: success" and names nothing. This carries the reason:
   * `completed`, `api_error`, and whatever else the CLI grows.
   *
   * Claude Code only. Amp does not emit it, and {@linkcode terminalReason}
   * falls back to `subtype` there.
   */
  terminal_reason?: string;
  /** The HTTP status when the run ended on an API error, else absent or null. */
  api_error_status?: number | null;
  /** Amp's own field for what went wrong, absent on Claude Code. */
  error?: string;
}

/** What a phase run needs to know about, whichever agent runs it. */
export interface RunnerInvocation {
  /** JSON Schema of the answer the phase must produce. */
  schema: Record<string, unknown>;
  /** Model or mode to run on, empty to inherit the CLI's own default. */
  model: string;
  /** Permission mode, in the vocabulary the model's caller uses. */
  permissionMode: string;
  /** A session to continue instead of starting a new one, empty for a new one. */
  resumeSession: string;
}

export interface Runner {
  /** The runner's name, as the model's `runner` argument spells it. */
  readonly name: string;
  /** Default executable when the caller names none. */
  readonly defaultBin: string;
  /**
   * Whether the agent is made to answer in the schema, rather than asked to.
   *
   * False means the shape is checked after the fact and one repair attempt is
   * available, which is a materially weaker contract — a caller deciding how
   * much to trust an answer should be able to read this rather than infer it
   * from the runner's name.
   */
  readonly enforcesSchema: boolean;
  /** The argument vector that starts a phase. */
  argv(invocation: RunnerInvocation): string[];
  /**
   * Text appended to the prompt, empty when the CLI carries the schema itself.
   */
  promptSuffix(invocation: RunnerInvocation): string;
  /** The answer the agent gave, or null when it gave none. */
  answerOf(event: ResultEvent): unknown;
  /**
   * The argument vector that asks a finished session to correct its answer,
   * or null when the runner cannot continue a session.
   */
  repairArgv(sessionId: string, complaint: string): string[] | null;
  /**
   * What the run cost, in USD, or null when the CLI did not say.
   *
   * Takes the run's signal because a runner that has to ask a second process
   * is asking after the answer is in hand and before it is recorded — the one
   * place where a stall would outlive the phase timeout that was meant to end
   * it.
   */
  costOf(
    event: ResultEvent,
    bin: string,
    cwd: string,
    signal: AbortSignal,
  ): Promise<number | null>;
  /** Whether running the phase again might get further. */
  isRetryable(event: ResultEvent): boolean;
}

/**
 * Why the CLI says the run ended, in one word where it says so.
 *
 * Amp reports the reason in `error` and nothing in `terminal_reason`, so its
 * runs describe themselves through `subtype` and the message built from it
 * carries `error` separately.
 */
export function terminalReason(event: ResultEvent): string {
  const reason = (event.terminal_reason ?? "").trim();
  if (reason) return reason;
  const subtype = (event.subtype ?? "").trim();
  return subtype || "an unknown reason";
}

/**
 * What the CLI said on the way out, ready to append to a message, or empty.
 *
 * The `result` field of a failed run carries the human-readable text — "You've
 * hit your session limit · resets 6:50pm" — which is the whole diagnosis for
 * the cases an operator can actually fix. Amp puts that text in `error`
 * instead and leaves `result` unset on a failure, so both are read. Empty when
 * the run answered normally, where `result` holds the serialized answer and
 * repeating it would bury the message.
 */
export function saidOnTheWayOut(event: ResultEvent): string {
  const amp = (event.error ?? "").trim();
  if (amp) return `: ${amp}`;
  if (typeof event.result !== "string") return "";
  const said = event.result.trim();
  return said ? `: ${said}` : "";
}

/** Claude Code, which is made to answer in the schema it is given. */
export const ClaudeRunner: Runner = {
  name: "claude",
  defaultBin: "claude",
  enforcesSchema: true,

  argv(invocation) {
    const argv = [
      "--print",
      "--output-format",
      "stream-json",
      "--verbose",
      "--permission-mode",
      invocation.permissionMode,
      "--json-schema",
      JSON.stringify(invocation.schema),
    ];
    if (invocation.model) argv.push("--model", invocation.model);
    if (invocation.resumeSession) {
      argv.push("--resume", invocation.resumeSession);
    }
    return argv;
  },

  // The CLI is given the schema, so the prompt does not carry it. Saying it
  // twice would be two statements of the same fact that can drift apart, and
  // the one in argv is the one that is enforced.
  promptSuffix() {
    return "";
  },

  answerOf(event) {
    return event.structured_output ?? null;
  },

  // Nothing to repair: an answer that reached here matched the schema the CLI
  // was holding it to, and one that did not never became an answer.
  repairArgv() {
    return null;
  },

  costOf(event) {
    return Promise.resolve(
      typeof event.total_cost_usd === "number" ? event.total_cost_usd : null,
    );
  },

  isRetryable(event) {
    const status = event.api_error_status;
    if (typeof status === "number" && (status === 429 || status >= 500)) {
      return true;
    }
    return terminalReason(event) === "api_error";
  },
};

/**
 * The Amp thread id in an `amp threads usage` line, if there is one.
 *
 * Amp session ids are thread ids, which is what makes the cost recoverable at
 * all: the run does not report what it spent, but the thread it created can be
 * asked afterwards.
 */
const USAGE_COST = /Cost:\s*\$([0-9]+(?:\.[0-9]+)?)/;

/**
 * How much of an Amp run's output is read back when looking for its answer.
 *
 * The answer is the last assistant message, which execute mode prints on its
 * own — this bounds what a fenced-block search reads when that message is a
 * runaway.
 */
const MAX_ANSWER_CHARS = 2_000_000;

/**
 * How long the cost lookup may take before it is given up on.
 *
 * It is one `gh`-sized call about a thread that has just finished, so seconds
 * is generous; and what it protects is larger than itself — the phase's answer
 * is complete by then, and a cost nobody could read is not worth losing it for.
 */
const COST_TIMEOUT_MS = 30_000;

/**
 * Pull a JSON object out of what an agent said.
 *
 * Asked for bare JSON, an agent frequently gives a fenced block, and sometimes
 * a sentence in front of it. Both are recoverable and neither is worth failing
 * a forty-minute phase over, so the fence is stripped and, failing that, the
 * outermost braces are taken. What is not recoverable is prose with no object
 * in it at all, which is what the null is for.
 *
 * Exported for unit testing; not part of the model's public surface.
 */
export function extractJson(text: string): unknown {
  const body = text.slice(0, MAX_ANSWER_CHARS).trim();
  if (!body) return null;

  const fenced = body.match(/```(?:json)?\s*\n([\s\S]*?)\n?```/);
  const candidates = [
    fenced ? fenced[1] : null,
    body,
    // The outermost braces, for a reply that wrapped the object in prose.
    body.slice(body.indexOf("{"), body.lastIndexOf("}") + 1),
  ];
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      const parsed = JSON.parse(candidate);
      // An object, not merely JSON: every phase answers in one, so an array or
      // a bare string here is a reply that missed the shape rather than an
      // answer that fails validation — and saying "answered without structured
      // output" is the truer of the two messages.
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // Not this one; the next candidate is a different reading of the same
      // text rather than a different text.
    }
  }
  return null;
}

/**
 * The transient failures Amp reports, which have no status code to read.
 *
 * Claude Code says `terminal_reason: api_error` and carries an HTTP status;
 * Amp reports `error_during_execution` with the reason in prose — an observed
 * one being "OpenAI WebSocket connection timed out after 15000ms". So the
 * classification has to be made from that text, and it is made narrowly: the
 * cost of retrying a permanent failure forever is unbounded, while the cost of
 * stopping on a transient one is a run an operator restarts.
 */
const AMP_TRANSIENT =
  /timed out|timeout|rate limit|429|overloaded|capacity|temporarily|connection (?:reset|closed|refused)|socket hang up|ECONNRESET|EAI_AGAIN|502|503|504|internal server error/i;

/**
 * Amp, which is asked to answer in the schema and checked afterwards.
 *
 * Amp has no `--json-schema`: structured output is available to its plugins and
 * not to a CLI turn. So the schema is put in the prompt, the last assistant
 * message is parsed as JSON — execute mode prints exactly that message, which
 * is why this is workable at all — and a reply that does not match is given one
 * chance to correct itself in the same thread.
 *
 * The consequence is worth being explicit about, because a gate downstream
 * cannot see it: under Claude an off-schema answer is impossible, under Amp it
 * is merely caught. Everything a schema-valid answer is then held to — the
 * blockers-beside-success check, the reviewer-wrote-to-the-tree check, the
 * push-without-a-commit check — is unchanged, and those are what most of the
 * flow's safety actually rests on.
 */
export const AmpRunner: Runner = {
  name: "amp",
  defaultBin: "amp",
  enforcesSchema: false,

  argv(invocation) {
    // A resumed phase continues its thread rather than starting one. Amp has
    // no `--resume`; the thread is the session, and continuing it is a
    // subcommand — so the flags follow the thread id rather than standing on
    // their own. Without this the option was silently Claude-only: an Amp
    // phase asked to resume started fresh, having lost every decision the
    // session it was meant to continue had already taken.
    const argv = invocation.resumeSession
      ? ["threads", "continue", invocation.resumeSession]
      : [];
    // A headless phase must not inherit whichever files and selection happen
    // to be open in the operator's IDE. They are unrelated ambient context and
    // make the same workflow input produce a different prompt from one run to
    // the next.
    argv.push(
      "--execute",
      "--stream-json",
      "--no-notifications",
      "--no-color",
      "--no-ide",
    );
    // Amp's `--mode` picks the model, the system prompt and the tool set
    // together; there is no flag naming a model directly. An empty one leaves
    // the CLI's own default, as everywhere else here.
    if (invocation.model) argv.push("--mode", invocation.model);
    return argv;
  },

  promptSuffix(invocation) {
    return [
      "",
      "## The shape of your answer",
      "",
      "Your final message must be a single JSON object and nothing else: no",
      "prose before or after it, no explanation, no code fence. It must validate",
      "against this JSON Schema, whose `description` on each field says what",
      "belongs in it:",
      "",
      "```json",
      JSON.stringify(invocation.schema, null, 2),
      "```",
      "",
      "Every required field must be present. A field you have nothing to say in",
      "takes its empty value — an empty string, an empty array — rather than",
      "being left out or filled with a guess. If you could not do the work, that",
      "is `succeeded: false` with the reason in `blockers`; it is not a reason to",
      "answer in a different shape.",
    ].join("\n");
  },

  // Whatever the last assistant message was. Execute mode prints only that
  // message, so this is the answer rather than a search through the transcript.
  answerOf(event) {
    return typeof event.result === "string" ? extractJson(event.result) : null;
  },

  repairArgv(sessionId, complaint) {
    return [
      "threads",
      "continue",
      sessionId,
      // Bound to the flag rather than left trailing: `threads continue` takes
      // a *variadic* list of threads, so a message sitting on its own at the
      // end is read as another thread id and the correction is never asked
      // for.
      "--execute",
      complaint,
      "--stream-json",
      "--no-notifications",
      "--no-color",
      "--no-ide",
    ];
  },

  // The run does not report what it spent — there is no `total_cost_usd` in
  // Amp's result event — but the thread it created can be asked afterwards.
  // One extra process per phase, against a phase that takes minutes.
  async costOf(event, bin, cwd, signal) {
    const session = (event.session_id ?? "").trim();
    if (!session) return null;
    try {
      const { success, stdout } = await new Deno.Command(bin, {
        args: ["threads", "usage", session],
        cwd,
        stdin: "null",
        stdout: "piped",
        stderr: "null",
        // The phase's own signal, and a bound of its own beside it. This runs
        // after the agent has answered and before the answer is recorded, so a
        // stalled lookup would hold a finished phase open past the timeout that
        // was meant to end it — and lose an answer that was already in hand.
        signal: AbortSignal.any([signal, AbortSignal.timeout(COST_TIMEOUT_MS)]),
      }).output();
      if (!success) return null;
      const match = new TextDecoder().decode(stdout).match(USAGE_COST);
      return match ? Number(match[1]) : null;
    } catch {
      return null;
    }
  },

  isRetryable(event) {
    const status = event.api_error_status;
    if (typeof status === "number" && (status === 429 || status >= 500)) {
      return true;
    }
    return AMP_TRANSIENT.test(`${event.error ?? ""} ${event.result ?? ""}`);
  },
};

/** Every runner, by the name the model's `runner` argument takes. */
export const RUNNERS: Record<string, Runner> = {
  [ClaudeRunner.name]: ClaudeRunner,
  [AmpRunner.name]: AmpRunner,
};

/** The runner a name selects, or the Claude one when the name is empty. */
export function runnerFor(name: string): Runner {
  const runner = RUNNERS[name];
  if (!runner) {
    throw new Error(
      `Unknown agent runner ${JSON.stringify(name)}. Known runners: ${
        Object.keys(RUNNERS).sort().join(", ")
      }.`,
    );
  }
  return runner;
}
