/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the lint model's `execute`.
 *
 * This model has the most moving parts of any here: it regenerates headers,
 * reads the excluded-crate list out of build.sh, and then runs cargo once per
 * profile. None of that is visible to the diagnostic parser — the sequencing,
 * the exclude list reaching argv, and the refusal to lint after a failed
 * regeneration can only be observed by driving the whole method.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./rust_lint.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const run = model.methods.run;

/**
 * Lay out the minimum a lint run needs: a workspace, a build.sh carrying the
 * exclude list, and a header regeneration script. Each fake records that it
 * ran, so a test can assert the order.
 */
async function repo(
  dir: string,
  options: { cargo?: string; regen?: string } = {},
): Promise<string> {
  await Deno.mkdir(`${dir}/ws`, { recursive: true });
  await Deno.writeTextFile(`${dir}/ws/Cargo.toml`, "[workspace]\n");
  await Deno.writeTextFile(
    `${dir}/build.sh`,
    'EXCLUDE_RUST_BENCHING_CRATES_LINKING_C="--exclude a_bencher --exclude b_ffi"\n',
  );

  await Deno.mkdir(`${dir}/src/redisearch_rs`, { recursive: true });
  await writeScript(
    `${dir}/src/redisearch_rs`,
    "regen_headers.sh",
    ['echo "regen" >> "' + dir + '/order"', options.regen ?? "true"].join("\n"),
  );

  return await writeScript(
    dir,
    "fake-cargo",
    [
      'echo "cargo $@" >> "' + dir + '/order"',
      options.cargo ?? 'echo "ARGS: $@"',
    ]
      .join("\n"),
  );
}

/** What ran, in order. */
async function order(dir: string): Promise<string[]> {
  try {
    return (await Deno.readTextFile(`${dir}/order`)).trimEnd().split("\n");
  } catch {
    return [];
  }
}

function globals(cargoBin: string, overrides: Record<string, unknown> = {}) {
  return {
    tool: "clippy",
    workingDir: "ws",
    cargoBin,
    profiles: ["debug", "release"],
    excludeFrom: "build.sh",
    regenerateHeaders: true,
    ...overrides,
  };
}

Deno.test("regenerates headers before linting", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    const { context } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    const ran = await order(dir);
    // Headers first: linting stale headers would report errors that do not
    // exist once they are regenerated.
    assertEquals(ran[0], "regen");
    assertEquals(ran.length, 3);
  });
});

/**
 * Turn the fake repo into a real git one with the headers committed, so the
 * staleness check has something to diff against. Without a repository git
 * cannot answer at all, which is deliberately not treated as a failure.
 */
async function gitRepoWithHeaders(dir: string): Promise<void> {
  const headers = `${dir}/src/redisearch_rs/headers`;
  await Deno.mkdir(headers, { recursive: true });
  await Deno.writeTextFile(`${headers}/generated.h`, "// generated\n");
  const git = async (...args: string[]): Promise<void> => {
    await new Deno.Command("git", { args, cwd: dir }).output();
  };
  await git("init", "-q");
  await git("add", "-A");
  await git(
    "-c",
    "user.email=t@t",
    "-c",
    "user.name=t",
    "commit",
    "-qm",
    "init",
  );
}

Deno.test("fails when regeneration leaves the headers changed", async () => {
  await withTempRepo(async (dir) => {
    // Regeneration succeeding is not the same as the headers being current: it
    // rewrites them in place, so stale committed headers come back as a dirty
    // tree. CI fails on exactly this diff, and this runs before CI.
    const cargo = await repo(dir, {
      regen:
        `echo "// regenerated" > "${dir}/src/redisearch_rs/headers/generated.h"`,
    });
    await gitRepoWithHeaders(dir);
    const { context } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("committed headers are stale"), true);
    // Linting never started: the answer would describe headers that are about
    // to change anyway.
    assertEquals(await order(dir), ["regen"]);
  });
});

Deno.test("fails when regeneration writes a header never committed", async () => {
  await withTempRepo(async (dir) => {
    // A new FFI crate makes cheadergen write a header that is not in the index
    // at all. git diff compares tracked files and says nothing about it, so a
    // check built on diff alone would pass while leaving a required header
    // uncommitted — which is the CI failure this gate exists to catch first.
    const cargo = await repo(dir, {
      regen: `echo "// brand new" > "${dir}/src/redisearch_rs/headers/added.h"`,
    });
    await gitRepoWithHeaders(dir);
    const { context } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("committed headers are stale"), true);
    assertEquals(error?.message.includes("added.h"), true);
    assertEquals(await order(dir), ["regen"]);
  });
});

Deno.test("lints normally when regeneration changes nothing", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    await gitRepoWithHeaders(dir);
    const { context } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    const ran = await order(dir);
    assertEquals(ran[0], "regen");
    assertEquals(ran.length, 3);
  });
});

Deno.test("skips regeneration when the model opts out", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    const { context } = makeContext(
      dir,
      globals(cargo, { regenerateHeaders: false }),
    );

    await run.execute({ quiet: true }, context);

    const ran = await order(dir);
    assertEquals(ran.some((line) => line === "regen"), false);
    assertEquals(ran.length, 2);
  });
});

Deno.test("does not lint when header regeneration fails", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir, {
      regen: 'echo "cheadergen: version mismatch" >&2; exit 3',
    });
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(
      error?.message.includes("regen_headers.sh exited with code 3"),
      true,
    );
    // Linting against headers that failed to regenerate would report
    // diagnostics for a state the tree is not in.
    assertEquals(await order(dir), ["regen"]);
    assertEquals(recorded.resources.length, 0);
    // There is no summary on this path, so the log is the only account of what
    // went wrong — and the error sends the reader to it. Throwing before the
    // writer is finalised would leave that pointing at nothing.
    assertEquals(recorded.files.length, 1);
    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "cheadergen: version mismatch",
      ),
      true,
    );
  });
});

Deno.test("runs each configured profile in order", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    const ran = await order(dir);
    assertEquals(ran[1].includes("--release"), false);
    assertEquals(ran[2].includes("--release"), true);
    assertEquals(summaryOf(recorded).profilesRun, ["debug", "release"]);
  });
});

Deno.test("lints a single profile when asked", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    const { context, recorded } = makeContext(
      dir,
      globals(cargo, { profiles: ["release"] }),
    );

    await run.execute({ quiet: true }, context);

    assertEquals((await order(dir)).length, 2);
    assertEquals(summaryOf(recorded).profilesRun, ["release"]);
  });
});

Deno.test("passes the exclude list read from build.sh to cargo", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    const { context } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    const lint = (await order(dir))[1];
    // Reading the list from build.sh rather than restating it is what keeps
    // the model from drifting away from the Makefile.
    assertEquals(lint.includes("--exclude a_bencher --exclude b_ffi"), true);
  });
});

Deno.test("fails when the exclude list cannot be read", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir);
    await Deno.remove(`${dir}/build.sh`);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("exclude list"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("records the diagnostics a lint run produced", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir, {
      cargo: [
        "cat >&2 <<'OUT'",
        "warning: unused variable: `x`",
        "  --> src/foo.rs:10:5",
        "OUT",
        "exit 101",
      ].join("\n"),
    });
    const { context, recorded } = makeContext(
      dir,
      globals(cargo, { profiles: ["debug"] }),
    );

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    const findings = summary.findings as Array<Record<string, unknown>>;
    assertEquals(findings.length, 1);
    assertEquals(findings[0].file, "src/foo.rs");
    assertEquals(findings[0].line, 10);
    assertEquals(error !== null, true);
  });
});

Deno.test("refuses to run outside a Cargo workspace", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await writeScript(dir, "fake-cargo", "true");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    assertEquals(error?.message.includes("No Cargo.toml found"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("aborts a lint that outlives its timeout", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir, { cargo: "exec sleep 30" });
    const { context } = makeContext(
      dir,
      globals(cargo, { profiles: ["debug"] }),
    );

    const error = await captureError(() =>
      run.execute({ quiet: true, timeout: 200 }, context)
    );

    assertEquals(error?.message.includes("timed out"), true);
  });
});

Deno.test("points the lint build scripts at the build with BINDIR", async () => {
  // Linting runs the build scripts, and the ones binding C symbols panic when
  // the archive is missing rather than warning. Without BINDIR they look in the
  // release layout, so a lint after a debug build dies before reporting a
  // single finding.
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir, {
      cargo: 'echo "ENV BINDIR=${BINDIR:-unset}"',
    });
    const { context, recorded } = makeContext(
      dir,
      globals(cargo, { regenerateHeaders: false, profiles: ["debug"] }),
    );

    await run.execute({
      binDir: "/build/bin/linux-x64-debug/search-community",
      quiet: true,
    }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ENV BINDIR=/build/bin/linux-x64-debug/search-community",
      ),
      true,
    );
  });
});

Deno.test("leaves BINDIR alone when no binDir is given", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await repo(dir, {
      cargo: 'echo "ENV BINDIR=${BINDIR:-unset}"',
    });
    const { context, recorded } = makeContext(
      dir,
      globals(cargo, { regenerateHeaders: false, profiles: ["debug"] }),
    );

    await run.execute({ binDir: "", quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV BINDIR=unset"),
      true,
    );
  });
});
