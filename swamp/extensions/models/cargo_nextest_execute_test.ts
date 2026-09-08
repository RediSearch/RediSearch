/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the cargo nextest model's `execute`.
 *
 * The parser tests cover what the model makes of nextest's output. These cover
 * the surface around it: the argv cargo actually receives, the guard on a
 * filter that would be read as a flag, and what is recorded when a run is cut
 * short.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model } from "./cargo_nextest.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const run = model.methods.run;

/** A workspace whose cargo reports the arguments it was handed. */
async function workspace(
  dir: string,
  body = 'echo "ARGS: $@"',
): Promise<string> {
  await Deno.mkdir(`${dir}/ws`, { recursive: true });
  await Deno.writeTextFile(`${dir}/ws/Cargo.toml`, "[workspace]\n");
  return await writeScript(dir, "fake-cargo", body);
}

function globals(cargoBin: string, overrides: Record<string, unknown> = {}) {
  return { repoRoot: ".", workingDir: "ws", cargoBin, ...overrides };
}

Deno.test("invokes nextest with colour disabled", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ARGS: nextest run --color never",
      ),
      true,
    );
  });
});

Deno.test("works on the checkout when swamp lives in a subdirectory", async () => {
  await withTempRepo(async (dir) => {
    // The layout this repository uses: the swamp files sit in <checkout>/swamp,
    // so `repoDir` is that subdirectory and everything the model touches — the
    // workspace, the pinned nightly — is one level up from it.
    const cargo = await workspace(dir, 'echo "ARGS: $@"');
    await Deno.writeTextFile(
      `${dir}/${".rust-nightly"}`,
      "nightly-2026-01-01\n",
    );
    const swampDir = `${dir}/swamp`;
    await Deno.mkdir(swampDir, { recursive: true });
    const { context, recorded } = makeContext(
      swampDir,
      globals(cargo, { repoRoot: ".." }),
    );

    await run.execute({ miri: true, quiet: true }, context);

    // The pin was read from the checkout rather than from the swamp directory,
    // and cargo ran in the workspace rather than under swamp/.
    const log = recorded.files[0].lines.join("\n");
    assertEquals(
      log.includes("ARGS: +nightly-2026-01-01 miri nextest run"),
      true,
    );
    assertEquals(summaryOf(recorded).workingDir, `${dir}/ws`);
  });
});

Deno.test("scopes a run to a crate and a filter", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({
      crate: "varint",
      test: "test_u32",
      noFailFast: true,
      extraArgs: ["--retries", "2"],
      quiet: true,
    }, context);

    // The positional filter has to come last, after every flag.
    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ARGS: nextest run --color never -p varint --no-fail-fast --retries 2 test_u32",
      ),
      true,
    );
  });
});

Deno.test("an inherited CARGO_ENCODED_RUSTFLAGS does not decide the warnings", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      'echo "ENV ENCODED=[${CARGO_ENCODED_RUSTFLAGS-unset}]"',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));
    // cargo prefers the encoded form when both are set, so pinning RUSTFLAGS
    // alone left this deciding: a cap here allows every warning the run exists
    // to deny, and the suite passes what CI rejects.
    Deno.env.set("CARGO_ENCODED_RUSTFLAGS", "--cap-lints\u001fallow");

    try {
      await run.execute({ quiet: true }, context);
    } finally {
      Deno.env.delete("CARGO_ENCODED_RUSTFLAGS");
    }

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV ENCODED=[unset]"),
      true,
    );
  });
});

Deno.test("refuses a bare filter smuggled through extraArgs", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context } = makeContext(dir, globals(cargo));

    // nextest takes a filter as a positional, so this runs a handful of tests
    // while `testFilter` stays null — indistinguishable from the whole suite
    // to the gate that reads it.
    const bare = await captureError(() =>
      run.execute({ quiet: true, extraArgs: ["some_test"] }, context)
    );
    assertEquals(bare?.message.includes("positional entry"), true);

    // A value written apart from its own switch is still a value.
    await run.execute(
      { quiet: true, extraArgs: ["--retries", "2"] },
      context,
    );
  });
});

Deno.test("refuses a modelled switch smuggled through extraArgs", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    // The summary records `crate` and `testFilter` from the typed arguments, so
    // this would run one crate and record the run as the whole workspace — and
    // the hand-off reads exactly those two fields to decide a run was
    // unnarrowed.
    const scoped = await captureError(() =>
      run.execute({ quiet: true, extraArgs: ["-p", "qint"] }, context)
    );
    assertEquals(scoped?.message.includes("extraArgs sets -p"), true);

    // The joined form reaches the same nextest option.
    const joined = await captureError(() =>
      run.execute({ quiet: true, extraArgs: ["--package=qint"] }, context)
    );
    assertEquals(joined?.message.includes("extraArgs sets --package"), true);

    // And the narrowing switches with no field to redirect to.
    const expr = await captureError(() =>
      run.execute({ quiet: true, extraArgs: ["-E", "test(qint)"] }, context)
    );
    assertEquals(expr?.message.includes("extraArgs sets -E"), true);

    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("leaves an extraArgs entry the summary does not speak for alone", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true, extraArgs: ["--jobs", "2"] }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("--jobs 2"),
      true,
    );
  });
});

Deno.test("runs under miri on the toolchain the project pins", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-05-01\n");
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ miri: true, crate: "trie_rs", quiet: true }, context);

    // The toolchain has to lead, before the subcommand, and `miri` has to wrap
    // nextest rather than follow it.
    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ARGS: +nightly-2026-05-01 miri nextest run --color never -p trie_rs",
      ),
      true,
    );
    const summary = summaryOf(recorded);
    assertEquals(summary.miri, true);
    assertEquals(summary.toolchain, "nightly-2026-05-01");
  });
});

Deno.test("a native run pins no toolchain", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    // Present, and still not used: rust-toolchain.toml decides a native run, as
    // it does for every other cargo invocation.
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-05-01\n");
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ARGS: nextest run"),
      true,
    );
    assertEquals(summaryOf(recorded).toolchain, null);
    assertEquals(summaryOf(recorded).miri, false);
  });
});

Deno.test("an explicit toolchain overrides the pinned nightly", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-05-01\n");
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { miri: true, toolchain: "nightly", quiet: true },
      context,
    );

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ARGS: +nightly miri nextest",
      ),
      true,
    );
  });
});

Deno.test("falls back to nightly when nothing is pinned", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ miri: true, quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ARGS: +nightly miri nextest",
      ),
      true,
    );
    assertEquals(summaryOf(recorded).toolchain, "nightly");
  });
});

Deno.test("passes miriFlags through the environment", async () => {
  await withTempRepo(async (dir) => {
    // MIRIFLAGS reaches miri through the environment, not argv, so a test that
    // only inspected the arguments would pass while the flags went nowhere.
    const cargo = await workspace(
      dir,
      'echo "ENV MIRIFLAGS=${MIRIFLAGS:-unset}"',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({
      miri: true,
      miriFlags: ["-Zmiri-ignore-leaks", "-Zmiri-strict-provenance"],
      quiet: true,
    }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "ENV MIRIFLAGS=-Zmiri-ignore-leaks -Zmiri-strict-provenance",
      ),
      true,
    );
  });
});

Deno.test("compiles with the flags CI compiles with, miri or not", async () => {
  // Every job that compiles this workspace's tests denies warnings, and clippy
  // does not stand in for it: `cargo clippy` lints the default targets and the
  // lint model passes no `--all-targets`, so nothing else in the gate compiles
  // the test code this way. A warning in a test, or behind cfg(not(miri)) which
  // the miri run does not compile at all, reached CI unseen.
  for (const miri of [true, false]) {
    await withTempRepo(async (dir) => {
      const cargo = await workspace(
        dir,
        'echo "ENV RUSTFLAGS=[${RUSTFLAGS-unset}]"',
      );
      const { context, recorded } = makeContext(dir, globals(cargo));
      // Whatever the developer's shell says is not the CI configuration either.
      Deno.env.set("RUSTFLAGS", "-Awarnings");

      try {
        await run.execute({ miri, quiet: true }, context);
      } finally {
        Deno.env.delete("RUSTFLAGS");
      }

      assertEquals(
        recorded.files[0].lines.join("\n").includes(
          "ENV RUSTFLAGS=[-Dwarnings]",
        ),
        true,
        `miri: ${miri}`,
      );
    });
  }
});

Deno.test("does not let an inherited MIRIFLAGS decide what miri checks", async () => {
  // `${VAR-unset}` rather than `${VAR:-unset}`: the colon form cannot tell an
  // empty value from an absent one, which is the whole distinction here.
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      'echo "ENV MIRIFLAGS=[${MIRIFLAGS-unset}]"',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));
    Deno.env.set("MIRIFLAGS", "-Zmiri-ignore-leaks");

    try {
      await run.execute({ miri: true, quiet: true }, context);
    } finally {
      Deno.env.delete("MIRIFLAGS");
    }

    // A flag left in the caller's shell would otherwise turn the pre-PR gate
    // green on exactly what CI still fails on, with nothing in the summary to
    // say why.
    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV MIRIFLAGS=[]"),
      true,
    );
  });
});

Deno.test("leaves MIRIFLAGS alone for a run that is not under miri", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      'echo "ENV MIRIFLAGS=[${MIRIFLAGS-unset}]"',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ quiet: true }, context);

    // Nothing in a native run reads it, so there is nothing to own.
    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV MIRIFLAGS=[unset]"),
      true,
    );
  });
});

Deno.test("builds the test binaries with the profile it was given", async () => {
  await withTempRepo(async (dir) => {
    // Without it nextest uses its own default profile, which is not what a
    // release build just produced — so it would rebuild the workspace and test
    // artifacts other than the ones under verification.
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ cargoProfile: "optimised_test", quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes(
        "--cargo-profile=optimised_test",
      ),
      true,
    );
  });
});

Deno.test("leaves the profile to nextest when none is given", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ cargoProfile: "", quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("--cargo-profile"),
      false,
    );
  });
});

Deno.test("points the C-linking crates at the build with BINDIR", async () => {
  await withTempRepo(async (dir) => {
    // The crates that link against C read BINDIR from the environment to find
    // the static libraries. Without it they fall back to the conventional
    // release layout, so a debug run would link the wrong archive.
    const cargo = await workspace(dir, 'echo "ENV BINDIR=${BINDIR:-unset}"');
    const { context, recorded } = makeContext(dir, globals(cargo));

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

Deno.test("runs with BINDIR unset when no binDir is given", async () => {
  await withTempRepo(async (dir) => {
    // `${VAR-unset}` rather than `${VAR:-unset}`: the colon form cannot tell an
    // empty value from an absent one, and an empty BINDIR is a path to the
    // build scripts rather than an absent one.
    const cargo = await workspace(dir, 'echo "ENV BINDIR=[${BINDIR-unset}]"');
    const { context, recorded } = makeContext(dir, globals(cargo));

    // Empty stands for unset, so a caller can pass one through without
    // special-casing it.
    await run.execute({ binDir: "", quiet: true }, context);

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV BINDIR=[unset]"),
      true,
    );
  });
});

Deno.test("does not let an inherited BINDIR decide what is linked", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, 'echo "ENV BINDIR=[${BINDIR-unset}]"');
    const { context, recorded } = makeContext(dir, globals(cargo));
    Deno.env.set("BINDIR", "/stale/bin/linux-x64-debug-cov/search-community");

    try {
      await run.execute({ quiet: true }, context);
    } finally {
      Deno.env.delete("BINDIR");
    }

    // A directory left in the caller's shell by an earlier build would
    // otherwise link that build's archives into what the summary reports as an
    // ordinary default run.
    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV BINDIR=[unset]"),
      true,
    );
  });
});

Deno.test("refuses miriFlags without miri", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ miriFlags: ["-Zmiri-ignore-leaks"], quiet: true }, context)
    );

    assertEquals(error?.message.includes("without miri"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("explains a missing miri component", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      [
        "echo \"error: 'cargo-miri' is not installed for the toolchain 'nightly-2026-05-01-x86_64-unknown-linux-gnu'.\" >&2",
        "exit 1",
      ].join("\n"),
    );
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-05-01\n");
    const { context } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      // Nothing was interpreted, so this is an environment problem rather than
      // a test failure, and ignoreTestFailure must not bury it.
      run.execute({ miri: true, ignoreTestFailure: true, quiet: true }, context)
    );

    assertEquals(error?.message.includes("Miri is not installed"), true);
    assertEquals(
      error?.message.includes(
        "rustup component add --toolchain nightly-2026-05-01 miri",
      ),
      true,
    );
  });
});

Deno.test("treats an empty crate and filter as unset", async () => {
  await withTempRepo(async (dir) => {
    // A workflow passing an unset input through sends "", and an empty
    // positional filter is not the same as no filter: nextest would match it
    // against every test name and run nothing.
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { crate: "", test: "", toolchain: "", quiet: true },
      context,
    );

    // The whole line, not a prefix of it: nothing may follow --color never.
    assertEquals(
      recorded.files[0].lines.includes("ARGS: nextest run --color never"),
      true,
    );
    // Recorded as unset, rather than as an empty filter that never existed.
    const summary = summaryOf(recorded);
    assertEquals(summary.crate, null);
    assertEquals(summary.testFilter, null);
    // An empty toolchain leaves rust-toolchain.toml in charge, exactly as an
    // absent one does.
    assertEquals(summary.toolchain, null);
  });
});

Deno.test("refuses a filter that would be read as a flag", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ test: "--all-features", quiet: true }, context)
    );

    assertEquals(error?.message.includes("must not start with"), true);
    // Nothing ran, so there is nothing to record.
    assertEquals(recorded.resources.length, 0);
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

Deno.test("records the panic behind a failing test", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      [
        "cat <<'OUT'",
        "        FAIL [   0.003s] (1/1) build_utils tests::one",
        "    thread 'tests::one' panicked at build_utils/src/lib.rs:302:9:",
        "    assertion `left == right` failed",
        "      left: 1",
        "     right: 2",
        "    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
        "     Summary [   0.004s] 1 tests run: 0 passed, 1 failed",
        "OUT",
        "exit 1",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true }, context)
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.status, "failed");
    assertEquals(summary.failedTests, ["build_utils tests::one"]);
    const failures = summary.failures as Array<Record<string, unknown>>;
    assertEquals(failures.length, 1);
    // The path is rewritten from workspace-relative to repository-relative, so
    // it resolves from where the caller is working.
    assertEquals(failures[0].file, "ws/build_utils/src/lib.rs");
    assertEquals(failures[0].line, 302);
    // The thrown error leads with the assertion rather than just a name.
    assertEquals(
      error?.message.includes("assertion `left == right` failed"),
      true,
    );
  });
});

Deno.test("ignoreTestFailure records the failure without throwing", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(
      dir,
      [
        `echo "     Summary [   0.004s] 1 tests run: 0 passed, 1 failed"`,
        "exit 1",
      ].join("\n"),
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true, ignoreTestFailure: true }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
    assertEquals(summaryOf(recorded).testsRun, 1);
  });
});

Deno.test("ignoreTestFailure does not forgive a suite that never ran", async () => {
  await withTempRepo(async (dir) => {
    // A compile error exits before nextest prints a summary, so nothing parses.
    // Tolerating that would record `summaryParsed: false` beside a passing
    // method, which every gate downstream reads as a green suite.
    const cargo = await workspace(dir, 'echo "error[E0433]" >&2; exit 101');
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true, ignoreTestFailure: true }, context)
    );

    assertEquals(error?.message.includes("exited with code 101"), true);
    assertEquals(summaryOf(recorded).summaryParsed, false);
  });
});

Deno.test("aborts a run that outlives its timeout", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, "exec sleep 30");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ quiet: true, timeout: 200 }, context)
    );

    assertEquals(error?.message.includes("timed out after 200ms"), true);
    assertEquals(summaryOf(recorded).timedOut, true);
  });
});

Deno.test("times out even when a child outlives cargo", async () => {
  await withTempRepo(async (dir) => {
    // The background sleep inherits the pipes and is not signalled when the
    // script is, which is what a compiler or a test server does in a real run.
    // Waiting for the pipes to close would mean waiting for it, and the timeout
    // is only looked at after the readers finish.
    const cargo = await workspace(dir, "sleep 30 &\nexec sleep 30");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const startedAt = Date.now();
    const error = await captureError(() =>
      run.execute({ quiet: true, timeout: 200 }, context)
    );

    assertEquals(error?.message.includes("timed out after 200ms"), true);
    assertEquals(summaryOf(recorded).timedOut, true);
    // Generously above the timeout and far below the orphan's lifetime, so
    // this fails on a run that waited for the orphan without being flaky on a
    // loaded machine.
    assertEquals(Date.now() - startedAt < 10_000, true);
  });
});

Deno.test("a cancelled run records no summary", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, "exec sleep 30");
    const controller = new AbortController();
    const { context, recorded } = makeContext(
      dir,
      globals(cargo),
      controller.signal,
    );

    const started = run.execute({ quiet: true }, context);
    setTimeout(() => controller.abort(), 100);
    const error = await captureError(() => started);

    // Cancelling tested nothing, so a summary of zeroes would misrepresent it
    // as a passing run.
    assertEquals(error?.message.includes("was cancelled"), true);
    assertEquals(recorded.resources.length, 0);
    assertEquals(recorded.files.length, 1);
  });
});
