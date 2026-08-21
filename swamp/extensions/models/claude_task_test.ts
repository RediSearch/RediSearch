/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Tests for prompt and argument construction, and for reading a stream-json
 * transcript back.
 *
 * The schema tests are the load-bearing ones. A phase's schema is both what the
 * agent is told to answer in and what the next step reads, so the two cannot be
 * checked against each other by any later stage — if the schema handed to the
 * CLI stopped matching the resource, every gate downstream would still pass
 * while reading fields nothing had filled in.
 *
 * @module
 */
import { assert, assertEquals, assertStringIncludes } from "jsr:@std/assert@1";
import {
  buildArgs,
  buildPrompt,
  jsonSchemaFor,
  model,
  parseResult,
  progressText,
} from "./claude_task.ts";

/**
 * Every phase the model exposes, read off the model rather than listed here so
 * that a phase added later cannot quietly escape the checks below.
 */
// deno-lint-ignore no-explicit-any
const KINDS = Object.keys(model.methods) as any[];

/** Method arguments with every default filled in, as swamp supplies them. */
// deno-lint-ignore no-explicit-any
function args(overrides: Record<string, unknown> = {}): any {
  return {
    task: "Port the union iterator to Rust",
    context: {},
    instructions: "",
    resumeSession: "",
    digestWorkflow: "verify",
    model: "",
    ignoreFailure: false,
    quiet: true,
    ...overrides,
  };
}

/** Global arguments with every default filled in. */
// deno-lint-ignore no-explicit-any
function globals(overrides: Record<string, unknown> = {}): any {
  return {
    claudeBin: "claude",
    model: "",
    permissionMode: "auto",
    workingDir: ".",
    timeout: 1000,
    ...overrides,
  };
}

Deno.test("every phase's schema is closed and demands the outcome fields", () => {
  for (const kind of KINDS) {
    const schema = jsonSchemaFor(kind) as {
      required: string[];
      additionalProperties: boolean;
      properties: Record<string, unknown>;
    };

    // A phase that could answer with fields nobody asked for, or omit the
    // verdict a gate reads, would defeat the point of having a schema at all.
    for (const field of ["succeeded", "summary", "blockers"]) {
      assert(
        schema.required.includes(field),
        `${kind} does not require ${field}`,
      );
    }
    assertEquals(schema.additionalProperties, false, `${kind} is open`);
    assert(!("$schema" in schema), `${kind} kept the $schema key`);
  }
});

Deno.test("the review schema carries the field its loop ends on", () => {
  const schema = jsonSchemaFor("review") as {
    required: string[];
    properties: Record<string, { type?: string }>;
  };

  assert(schema.required.includes("clean"));
  assertEquals(schema.properties.clean.type, "boolean");
  assertEquals(schema.properties.findings.type, "array");
});

Deno.test("the prompt carries the task, the context, and the extra instructions", () => {
  const prompt = buildPrompt(
    "design",
    args({
      context: { tests: { testsAdded: ["test_union_read"] } },
      instructions: "Prefer an enum over a trait object.",
    }),
  );

  assertStringIncludes(prompt, "Port the union iterator to Rust");
  assertStringIncludes(prompt, "test_union_read");
  assertStringIncludes(prompt, "Prefer an enum over a trait object.");
  // The phase has to be told what it is doing, or it will pick for itself.
  assertStringIncludes(prompt, "design document");
});

Deno.test("each phase is told a different job", () => {
  // The prompts are the only thing distinguishing one phase from another —
  // they share an implementation, and differ in their schema and their brief.
  // Two phases given the same brief would do the same work twice under
  // different names, which no schema check would catch.
  const briefs = KINDS.map((kind) => buildPrompt(kind, args()));
  assertEquals(new Set(briefs).size, KINDS.length);
});

Deno.test("triage is pointed at the digest of the suite that failed", () => {
  // The digest is per workflow. A triage of a `verify-cluster` failure given
  // the default would read the last `verify` run instead — a different suite on
  // a different topology, quite possibly green — and answer "nothing failed",
  // which the gate above it reads as permission to continue.
  const cluster = buildPrompt(
    "triage",
    args({ digestWorkflow: "verify-cluster" }),
  );

  assertStringIncludes(cluster, "--workflow verify-cluster --markdown");
  assert(!cluster.includes("{{digestWorkflow}}"));
  assertStringIncludes(
    buildPrompt("triage", args()),
    "--workflow verify --markdown",
  );
});

Deno.test("an empty context and empty instructions add no empty sections", () => {
  const prompt = buildPrompt("design", args());

  assert(!prompt.includes("Results of the earlier phases"));
  assert(!prompt.includes("Additional instructions"));
});

Deno.test("the reviewer is told not to change code", () => {
  // The loop depends on the reviewer being independent of the author. A
  // reviewer that fixed what it found would report a clean review of its own
  // work, and the revise phase would have nothing to do.
  assertStringIncludes(buildPrompt("review", args()), "report, do not fix");
});

Deno.test("a missing Jira ticket does not stop the pull request", () => {
  const prompt = buildPrompt("pr", args());

  // A ticket is optional here: the repository asks for the [MOD-xyz] title when
  // one exists, not for the work to be withheld when it does not. Inventing an
  // id is the thing that is actually forbidden — a wrong one attaches the change
  // to somebody else's issue.
  assertStringIncludes(prompt, "A Jira ticket is not required");
  assertStringIncludes(prompt, "open it anyway");
  assertStringIncludes(prompt, "Never invent a ticket id");
});

Deno.test("the argument vector asks for a streamed, schema-bound run", () => {
  const argv = buildArgs("review", args(), globals());

  // The prompt is not in argv — it goes over stdin, because one argument is
  // capped at 128KB and a phase's prompt carries the earlier phases' results.
  assert(!argv.some((a) => a.includes("You are an independent reviewer")));
  assert(argv.includes("--print"));
  assert(argv.includes("--verbose"));
  assertEquals(argv[argv.indexOf("--output-format") + 1], "stream-json");
  assertEquals(argv[argv.indexOf("--permission-mode") + 1], "auto");

  // The schema goes over the command line as JSON text, not as a path.
  const schema = JSON.parse(argv[argv.indexOf("--json-schema") + 1]);
  assertEquals(schema, jsonSchemaFor("review"));
});

Deno.test("the model is inherited unless one is pinned", () => {
  assert(!buildArgs("design", args(), globals()).includes("--model"));

  const instance = buildArgs("design", args(), globals({ model: "opus" }));
  assertEquals(instance[instance.indexOf("--model") + 1], "opus");

  // A per-call model beats the instance's, so one phase can be pinned without
  // pinning the rest.
  const call = buildArgs(
    "design",
    args({ model: "haiku" }),
    globals({ model: "opus" }),
  );
  assertEquals(call[call.indexOf("--model") + 1], "haiku");
});

Deno.test("a session is resumed only when one was asked for", () => {
  assert(!buildArgs("revise", args(), globals()).includes("--resume"));

  const argv = buildArgs("revise", args({ resumeSession: "abc" }), globals());
  assertEquals(argv[argv.indexOf("--resume") + 1], "abc");
});

Deno.test("the last result event wins and the rest are progress", () => {
  const event = parseResult([
    '{"type":"system","subtype":"init"}',
    '{"type":"assistant","message":{"content":[{"type":"text","text":"hi"}]}}',
    '{"type":"result","subtype":"success","structured_output":{"succeeded":true}}',
  ]);

  assertEquals(event?.subtype, "success");
  assertEquals(event?.structured_output, { succeeded: true });
});

Deno.test("a transcript with no result event reads as none", () => {
  assertEquals(parseResult(['{"type":"system"}', "not json", ""]), null);
});

Deno.test("unparsable lines do not lose a result that follows them", () => {
  // The CLI is free to add event shapes and to write the odd non-JSON line.
  // Failing a phase that ran correctly over one would be the wrong trade.
  const event = parseResult([
    "Warning: something on stdout",
    "{ truncated",
    '{"type":"result","subtype":"success"}',
  ]);

  assertEquals(event?.subtype, "success");
});

Deno.test("progress is the assistant's words, truncated, and nothing else", () => {
  assertEquals(
    progressText(
      '{"type":"assistant","message":{"content":[{"type":"text","text":"Reading\\n  union.rs"}]}}',
    ),
    "Reading union.rs",
  );
  assertEquals(progressText('{"type":"system","subtype":"init"}'), null);
  assertEquals(progressText("not json"), null);
  // Tool calls carry no text block, so they mirror nothing.
  assertEquals(
    progressText(
      '{"type":"assistant","message":{"content":[{"type":"tool_use"}]}}',
    ),
    null,
  );

  const long = progressText(
    `{"type":"assistant","message":{"content":[{"type":"text","text":"${
      "x".repeat(500)
    }"}]}}`,
  );
  assertEquals(long?.length, 201);
  assert(long?.endsWith("…"));
});
