/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * End-to-end tests for the rust coverage model's `execute`.
 *
 * The parser tests cover what the model makes of llvm's export. These cover the
 * surface around it: that the export is read from stdout while progress goes to
 * the log, which files are kept, and what is recorded when the run dies before
 * measuring anything.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";
import { model, resolveToolchain } from "./rust_coverage.ts";
import {
  captureError,
  makeContext,
  summaryOf,
  withTempRepo,
  writeScript,
} from "./harness_test.ts";

const run = model.methods.run;

/** An export covering two files, one of them fully. */
function exportJson(): string {
  return JSON.stringify({
    data: [{
      files: [
        {
          filename: "/REPO/src/redisearch_rs/qint/src/lib.rs",
          summary: { lines: { count: 81, covered: 79, percent: 97.5 } },
          segments: [
            [134, 9, 0, true, true, false],
            [157, 13, 0, true, true, false],
            [158, 13, 0, true, true, false],
          ],
        },
        {
          filename: "/REPO/src/redisearch_rs/varint/src/lib.rs",
          summary: { lines: { count: 20, covered: 4, percent: 20 } },
          segments: [[7, 1, 0, true, true, false]],
        },
        {
          filename: "/REPO/src/redisearch_rs/fnv/src/lib.rs",
          summary: { lines: { count: 5, covered: 5, percent: 100 } },
          segments: [],
        },
      ],
    }],
  });
}

/**
 * A workspace whose cargo prints the given export on stdout and progress on
 * stderr, the way cargo llvm-cov does.
 */
async function workspace(
  dir: string,
  stdout = exportJson(),
  extra = "",
): Promise<string> {
  await Deno.mkdir(`${dir}/ws`, { recursive: true });
  await Deno.writeTextFile(`${dir}/ws/Cargo.toml`, "[workspace]\n");
  await Deno.writeTextFile(
    `${dir}/build.sh`,
    'EXCLUDE_RUST_BENCHING_CRATES_LINKING_C="--exclude varint_bencher"\n',
  );
  return await writeScript(
    dir,
    "fake-cargo",
    [
      'echo "ARGS: $@" >&2',
      'echo "   Compiling qint v0.1.0" >&2',
      `cat <<'JSON'`,
      stdout.replaceAll("/REPO", dir),
      "JSON",
      extra,
    ].join("\n"),
  );
}

function globals(cargoBin: string) {
  return { workingDir: "ws", cargoBin, excludeFrom: "build.sh" };
}

Deno.test("reads the export from stdout and the progress from stderr", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { crate: "qint", onlyIncomplete: true, quiet: true },
      context,
    );

    // Merging the two streams, as the other cargo models do, would corrupt the
    // JSON this run exists to read.
    const log = recorded.files[0].lines.join("\n");
    assertEquals(log.includes("Compiling qint"), true);
    assertEquals(log.includes('"data"'), false);

    const summary = summaryOf(recorded);
    assertEquals(summary.parsed, true);
    assertEquals(summary.filesMeasured, 3);
  });
});

Deno.test("keeps the worst covered files first", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { crate: "qint", onlyIncomplete: true, quiet: true },
      context,
    );

    const summary = summaryOf(recorded);
    const files = summary.files as Array<Record<string, unknown>>;
    // 20% before 97.5%, and the fully covered file dropped.
    assertEquals(files.map((f) => f.file), [
      "src/redisearch_rs/varint/src/lib.rs",
      "src/redisearch_rs/qint/src/lib.rs",
    ]);
    assertEquals(summary.filesIncomplete, 2);
    assertEquals(files[1].uncoveredRanges, [
      { start: 134, end: 134 },
      { start: 157, end: 158 },
    ]);
  });
});

Deno.test("drops files with no instrumented lines from the incomplete list", async () => {
  await withTempRepo(async (dir) => {
    const empty = JSON.stringify({
      data: [{
        files: [
          {
            filename: "/REPO/src/redisearch_rs/qint/src/lib.rs",
            summary: { lines: { count: 81, covered: 79, percent: 97.5 } },
            segments: [[134, 9, 0, true, true, false]],
          },
          {
            // A module that is nothing but `pub use` re-exports: no coverable
            // line, so llvm reports 0 of 0 and, with it, 0%.
            filename: "/REPO/src/redisearch_rs/qint/src/reexport.rs",
            summary: { lines: { count: 0, covered: 0, percent: 0 } },
            segments: [],
          },
        ],
      }],
    });
    const cargo = await workspace(dir, empty);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { crate: "qint", onlyIncomplete: true, quiet: true },
      context,
    );

    const summary = summaryOf(recorded);
    const files = summary.files as Array<Record<string, unknown>>;
    // Sorting on percentage alone would put the 0% file first and push files
    // that do have gaps past maxFiles, hiding the only actionable one here.
    assertEquals(summary.filesIncomplete, 1);
    assertEquals(files.map((f) => f.file), [
      "src/redisearch_rs/qint/src/lib.rs",
    ]);
  });
});

Deno.test("records a tolerated test failure as a failure", async () => {
  await withTempRepo(async (dir) => {
    // --ignore-run-fail makes llvm-cov exit 0 for having written the report,
    // however the tests went. Reading the exit code alone would present a floor
    // measurement as clean coverage, and the digest would say nothing.
    const cargo = await workspace(
      dir,
      exportJson(),
      'echo "test result: FAILED. 3 passed; 1 failed; 0 ignored" >&2',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      {
        crate: "qint",
        ignoreTestFailure: true,
        onlyIncomplete: true,
        quiet: true,
      },
      context,
    );

    const summary = summaryOf(recorded);
    assertEquals(summary.exitCode, 0);
    assertEquals(summary.status, "failed");
    // The measurement is still recorded: it is a floor, which is what the
    // caller asked for by tolerating the failure.
    assertEquals(summary.parsed, true);
  });
});

Deno.test("still fails a tolerated run that measured nothing", async () => {
  await withTempRepo(async (dir) => {
    // ignoreTestFailure promises a floor, and a floor needs a measurement. A
    // compile error exports nothing at all, so returning success here would let
    // a caller read an absence as a completed run.
    const cargo = await writeScript(
      dir,
      "fake-cargo",
      'echo "error[E0433]: failed to resolve" >&2; exit 101',
    );
    await Deno.mkdir(`${dir}/ws`, { recursive: true });
    await Deno.writeTextFile(`${dir}/ws/Cargo.toml`, "[workspace]\n");
    await Deno.writeTextFile(`${dir}/build.sh`, "\n");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute(
        {
          crate: "qint",
          ignoreTestFailure: true,
          onlyIncomplete: true,
          quiet: true,
        },
        context,
      )
    );

    assertEquals(
      error?.message.includes("before exporting any coverage"),
      true,
    );
    assertEquals(summaryOf(recorded).parsed, false);
  });
});

Deno.test("keeps every file when asked", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { crate: "qint", onlyIncomplete: false, quiet: true },
      context,
    );

    assertEquals(summaryOf(recorded).filesKept, 3);
  });
});

Deno.test("caps the kept files without changing the totals", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({
      crate: "qint",
      maxFiles: 1,
      onlyIncomplete: true,
      quiet: true,
    }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.filesKept, 1);
    // The cap is a display limit: everything measured is still counted.
    assertEquals(summary.filesMeasured, 3);
    const overall = summary.overall as Record<string, number>;
    assertEquals(overall.totalLines, 106);
    assertEquals(overall.coveredLines, 88);
  });
});

Deno.test("excludes the bencher crates on a workspace run", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ onlyIncomplete: true, quiet: true }, context);

    const log = recorded.files[0].lines.join("\n");
    assertEquals(
      log.includes(
        "ARGS: llvm-cov test --quiet --json --workspace --exclude varint_bencher",
      ),
      true,
    );
    assertEquals(summaryOf(recorded).excluded, ["varint_bencher"]);
    assertEquals(summaryOf(recorded).scope, "workspace");
  });
});

Deno.test("does not consult the exclude list for a single crate", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    // No build.sh to read: a crate-scoped run must not need one.
    await Deno.remove(`${dir}/build.sh`);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ crate: "qint", onlyIncomplete: true, quiet: true }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).excluded, []);
  });
});

Deno.test("fails a workspace run when the exclude list cannot be read", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    await Deno.remove(`${dir}/build.sh`);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ onlyIncomplete: true, quiet: true }, context)
    );

    // Guessing the excludes would fail at link time, deep in a long build.
    assertEquals(error?.message.includes("exclude list"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("refuses a crate and a manifest together", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({
        crate: "qint",
        manifestPath: "ws/Cargo.toml",
        onlyIncomplete: true,
        quiet: true,
      }, context)
    );

    assertEquals(error?.message.includes("pass one"), true);
    assertEquals(recorded.resources.length, 0);
  });
});

Deno.test("refuses a manifest that is not there", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir);
    const { context } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({
        manifestPath: "absent/Cargo.toml",
        onlyIncomplete: true,
        quiet: true,
      }, context)
    );

    assertEquals(error?.message.includes("No Cargo.toml found"), true);
  });
});

Deno.test("refuses to run outside a Cargo workspace", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await writeScript(dir, "fake-cargo", "true");
    const { context } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ crate: "qint", onlyIncomplete: true, quiet: true }, context)
    );

    assertEquals(error?.message.includes("No Cargo.toml found"), true);
  });
});

Deno.test("records no coverage when the run produced no export", async () => {
  await withTempRepo(async (dir) => {
    // A compile error: diagnostics on stderr, nothing on stdout.
    const cargo = await workspace(dir, "", 'echo "error[E0433]" >&2; exit 101');
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ crate: "qint", onlyIncomplete: true, quiet: true }, context)
    );

    assertEquals(
      error?.message.includes("before exporting any coverage"),
      true,
    );
    const summary = summaryOf(recorded);
    assertEquals(summary.parsed, false);
    // Null rather than zero: nothing was measured, which is not the same as
    // nothing being covered.
    assertEquals(summary.overall, null);
    assertEquals(summary.status, "failed");
  });
});

Deno.test("calls the coverage a floor when the tests failed", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, exportJson(), "exit 1");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({ crate: "qint", onlyIncomplete: true, quiet: true }, context)
    );

    assertEquals(error?.message.includes("floor"), true);
    // The measurement still describes what ran, so it is recorded.
    assertEquals(summaryOf(recorded).parsed, true);
  });
});

Deno.test("ignoreTestFailure records the coverage without throwing", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, exportJson(), "exit 1");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({
        crate: "qint",
        ignoreTestFailure: true,
        onlyIncomplete: true,
        quiet: true,
      }, context)
    );

    assertEquals(error, null);
    assertEquals(summaryOf(recorded).status, "failed");
    assertEquals(summaryOf(recorded).parsed, true);
  });
});

Deno.test("aborts a run that outlives its timeout", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, exportJson(), "exec sleep 30");
    const { context, recorded } = makeContext(dir, globals(cargo));

    const error = await captureError(() =>
      run.execute({
        crate: "qint",
        timeout: 300,
        onlyIncomplete: true,
        quiet: true,
      }, context)
    );

    assertEquals(error?.message.includes("timed out after 300ms"), true);
    assertEquals(summaryOf(recorded).timedOut, true);
  });
});

Deno.test("a cancelled run records no summary", async () => {
  await withTempRepo(async (dir) => {
    const cargo = await workspace(dir, exportJson(), "exec sleep 30");
    const controller = new AbortController();
    const { context, recorded } = makeContext(
      dir,
      globals(cargo),
      controller.signal,
    );

    const started = run.execute({
      crate: "qint",
      onlyIncomplete: true,
      quiet: true,
    }, context);
    setTimeout(() => controller.abort(), 100);
    const error = await captureError(() => started);

    assertEquals(error?.message.includes("was cancelled"), true);
    assertEquals(recorded.resources.length, 0);
    assertEquals(recorded.files.length, 1);
  });
});

Deno.test("points the C-linking crates at the build with BINDIR", async () => {
  await withTempRepo(async (dir) => {
    // The crates whose build scripts bind C symbols read BINDIR to find the
    // archive. Without it they fall back to the conventional release layout,
    // so an instrumented run measures against stale C artifacts or fails to
    // link at all on a checkout that was never built release.
    const cargo = await workspace(
      dir,
      exportJson(),
      'echo "ENV BINDIR=${BINDIR:-unset}" >&2',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({
      binDir: "/build/bin/linux-x64-debug/search-community",
      onlyIncomplete: true,
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
    const cargo = await workspace(
      dir,
      exportJson(),
      'echo "ENV BINDIR=${BINDIR:-unset}" >&2',
    );
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute(
      { binDir: "", onlyIncomplete: true, quiet: true },
      context,
    );

    assertEquals(
      recorded.files[0].lines.join("\n").includes("ENV BINDIR=unset"),
      true,
    );
  });
});

Deno.test("keeps a fully covered file that still holds a gap", async () => {
  await withTempRepo(async (dir) => {
    // Every line was hit, so the summary says 100%, but one region on a covered
    // line was never entered — the unexecuted arm of a one-line `if`. Filtering
    // on the percentage alone would drop exactly what the parser exists to find.
    const stdout = JSON.stringify({
      data: [{
        files: [{
          filename: "/REPO/src/redisearch_rs/qint/src/lib.rs",
          summary: { lines: { count: 10, covered: 10, percent: 100 } },
          segments: [[42, 9, 0, true, true, false]],
        }],
      }],
    });
    const cargo = await workspace(dir, stdout);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ onlyIncomplete: true, quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.filesIncomplete, 1);
    assertEquals(summary.filesKept, 1);
    assertEquals(
      (summary.files as Array<{ uncoveredRanges: unknown }>)[0]
        .uncoveredRanges,
      [{ start: 42, end: 42 }],
    );
  });
});

Deno.test("drops a file with neither missing lines nor gaps", async () => {
  await withTempRepo(async (dir) => {
    const stdout = JSON.stringify({
      data: [{
        files: [{
          filename: "/REPO/src/redisearch_rs/fnv/src/lib.rs",
          summary: { lines: { count: 5, covered: 5, percent: 100 } },
          segments: [[7, 1, 3, true, true, false]],
        }],
      }],
    });
    const cargo = await workspace(dir, stdout);
    const { context, recorded } = makeContext(dir, globals(cargo));

    await run.execute({ onlyIncomplete: true, quiet: true }, context);

    const summary = summaryOf(recorded);
    assertEquals(summary.filesIncomplete, 0);
    assertEquals(summary.filesKept, 0);
  });
});

const quiet = { info: () => {} };

Deno.test("takes the pinned nightly for a doctest run", async () => {
  await withTempRepo(async (dir) => {
    // Doctest coverage needs nightly-only features, and the repository pins
    // stable — so without this the advertised mode fails before measuring
    // anything. The pin is the same file the miri runs read.
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-01-01\n");

    const toolchain = await resolveToolchain(
      { doctests: true, onlyIncomplete: true },
      dir,
      quiet,
    );

    assertEquals(toolchain, "nightly-2026-01-01");
  });
});

Deno.test("falls back to plain nightly when nothing is pinned", async () => {
  await withTempRepo(async (dir) => {
    // Whatever nightly is installed still measures doctests, so the run is
    // worth making rather than refusing outright.
    const toolchain = await resolveToolchain(
      { doctests: true, onlyIncomplete: true },
      dir,
      quiet,
    );

    assertEquals(toolchain, "nightly");
  });
});

Deno.test("leaves an ordinary run on the default toolchain", async () => {
  await withTempRepo(async (dir) => {
    await Deno.writeTextFile(`${dir}/.rust-nightly`, "nightly-2026-01-01\n");

    const toolchain = await resolveToolchain(
      { crate: "qint", onlyIncomplete: true },
      dir,
      quiet,
    );

    // Only doctests need nightly; forcing it on every run would measure
    // something other than what the repository builds.
    assertEquals(toolchain, null);
  });
});
