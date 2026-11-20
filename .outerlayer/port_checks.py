#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#
"""The deterministic gates of a C-to-Rust port, run by the build's gate command.

Ported from the Amp rust-port plugin (.amp/plugins/rust-port): its claim gates
(core/gates.ts, core/diff.ts, c-to-rust/rust.ts, redisearch/profile.ts) and its
check judges (c-to-rust/cargo.ts). Each subcommand exits non-zero with one line
per problem, so an agent reads why and a later run shows it is fixed.

    port_checks.py diff --base origin/master --paths src/redisearch_rs/geo/ tests/pytests/test_geo.py
    port_checks.py miri -- cargo +nightly miri nextest run -p geo
    port_checks.py mutants -- cargo mutants -p geo --in-diff <(git diff origin/master)
    port_checks.py bench -- cargo bench --manifest-path src/redisearch_rs/Cargo.toml -p geo_bencher
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

WORKSPACE = "src/redisearch_rs/"
HEADERS = WORKSPACE + "headers/"
FFI = WORKSPACE + "c_entrypoint/"
# How much slower than C, in percent, Rust may be in any benchmark group.
SLOWDOWN_PERCENT = 5
# Lints a new crate enables for itself until the workspace does (.skills/port-c-module).
CRATE_LINTS = [
    "clippy::unwrap_used", "clippy::dbg_macro", "clippy::todo", "clippy::unimplemented",
    "clippy::print_stdout", "clippy::panic", "clippy::unnecessary_safety_comment",
    "clippy::ptr_as_ptr", "clippy::borrow_as_ptr", "clippy::ref_as_ptr",
    "clippy::indexing_slicing",
]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def show(rev: str, path: str) -> str | None:
    result = subprocess.run(["git", "show", f"{rev}:{path}"], capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else None


# --- Classification -----------------------------------------------------------------------------

def is_test(path: str) -> bool:
    return re.search(r"(^|/)(tests|benches)/", path) is not None


def is_generated(path: str) -> bool:
    return path.startswith(HEADERS) or path.endswith("Cargo.lock")


def is_rust(path: str) -> bool:
    return path.startswith(WORKSPACE) and path.endswith(".rs")


def is_pure(path: str) -> bool:
    return is_rust(path) and not re.match(
        r"^src/redisearch_rs/(c_entrypoint|ffi|c_wrappers|redis_mock)/", path
    )


# Strings, char literals and comments are skipped whole, so a gate or brace in them does not count.
LITERAL = re.compile(r"""b?r(#*)"[\s\S]*?"\1|"(\\.|[^"\\])*"|'(\\.|[^'\\])'|//[^\n]*|/\*[\s\S]*?\*/""")
TEST_GATE = "#[cfg(test)]"


def without_tests(text: str) -> str:
    """The text with every `#[cfg(test)]` item removed."""
    kept, start, gate, depth, i = [], 0, -1, 0, 0
    while i < len(text):
        m = LITERAL.match(text, i)
        if m:
            i = m.end()
            continue
        if gate < 0:
            if text.startswith(TEST_GATE, i):
                kept.append(text[start:i])
                gate, depth = i, 0
                i += len(TEST_GATE)
                continue
        elif text[i] == "{":
            depth += 1
        elif (text[i] == "}" and depth == 1) or (text[i] == ";" and depth == 0):
            gate, start = -1, i + 1
        elif text[i] == "}":
            depth -= 1
        i += 1
    if gate < 0:
        kept.append(text[start:])
    return "".join(kept)


# --- House rules --------------------------------------------------------------------------------

def named_for_artifact(name: str) -> bool:
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    snake = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", snake).lower()
    return any(w in ("contract", "boundary", "abi", "compat") for w in snake.split("_"))


def artifact_named_tests(text: str) -> int:
    names = [m["name"] for m in re.finditer(
        r"#\[(?:\w+::)*(?:test|rstest)\b[^\]]*\]\s*(?:#\[[^\]]*\]\s*)*(?:async\s+)?fn\s+(?P<name>\w+)", text)]
    names += re.findall(r"\bdef\s+(test\w*)\s*\(", text)
    names += [m[1] for m in re.findall(r"\bTEST(_F)?\s*\(\s*\w+\s*,\s*(\w+)\s*\)", text)]
    return sum(named_for_artifact(n) for n in names)


def bare_asserts(text: str) -> int:
    helper, count = False, 0
    for line in text.split("\n"):
        fn = re.search(r"\bfn\s+(\w+)", line)
        if fn:
            helper = re.match(r"^(debug_)?assert_", fn[1]) is not None
        if re.search(r"\bdebug_assert(_eq|_ne)?!\s*\(", line) and not helper:
            count += 1
    return count


def unnumbered_safety_sections(text: str) -> int:
    count = 0
    for rest in re.split(r"^\s*///\s*# Safety\s*$", text, flags=re.M)[1:]:
        docs = [line for line in rest.split("\n") if line]
        end = next((i for i, line in enumerate(docs) if not line.strip().startswith("///")), len(docs))
        if not any(re.match(r"^\s*///\s*\d+\.", line) for line in docs[:end]):
            count += 1
    return count


def production(p: str) -> bool:
    return is_rust(p) and not is_test(p)


def ffi(p: str) -> bool:
    return production(p) and p.startswith(FFI)


def count(pattern: str):
    return lambda text: len(re.findall(pattern, text))


# (applies to path, count violations in text, message), each a ratchet against the base.
RULES = [
    (production, lambda t: count(r"\.unwrap\(\)")(without_tests(t)),
     'adds `unwrap()` outside tests; use `expect("<the invariant it relies on>")`'),
    (ffi, count(r"unsafe\s*\{\s*(&\s*(mut\s+)?\*|\(\s*\*)"),
     "dereferences a raw pointer with `*`; use `as_ref()`, `as_mut()` or `write()`"),
    (ffi, lambda t: bare_asserts(without_tests(t)),
     "adds a bare `debug_assert!`; check FFI pre-conditions with a named assertion helper"),
    (production, lambda t: unnumbered_safety_sections(without_tests(t)),
     "adds a `# Safety` section whose pre-conditions are not numbered"),
    (production, count(r"Result<[^;{]*,\s*&'static str\s*>"),
     "returns a `&'static str` error; use an error enum deriving `thiserror::Error`"),
    (lambda p: is_pure(p) and not is_test(p), count(r'\bextern\s+"C"|#\[(unsafe\()?no_mangle\b'),
     "adds C ABI items to a pure crate; they belong in its FFI crate"),
    (lambda p: is_pure(p) and is_test(p), count(r'\bextern\s+"C"\s*\{'),
     "declares C functions in a pure crate's tests; test the C boundary in the FFI crate"),
    (lambda p: is_rust(p) and not is_test(p), count(r"RedisModule_(Alloc|Calloc|Realloc)\b"),
     "calls the Redis allocator directly; allocate with `Box`, the global allocator"),
    (lambda p: is_rust(p) and p != f"{FFI}redisearch_rs/src/lib.rs", count(r"#\[global_allocator\]"),
     "adds an allocator; Miri already catches leaks and double frees"),
    (lambda p: is_rust(p) or re.match(r"^tests/(pytests|cpptests|ctests)/", p) is not None,
     artifact_named_tests,
     "adds a test named for its artifact (contract, ABI, boundary); name the behavior"),
    # An existing crate rolls the lints out on its own, but none may drop one.
    (lambda p: re.match(r"^src/redisearch_rs/(.+/)?[^/]+/src/lib\.rs$", p) is not None,
     lambda t: sum(lint not in t for lint in CRATE_LINTS),
     "lacks lints of the crate lint block: " + ", ".join(CRATE_LINTS)),
]


# --- diff ---------------------------------------------------------------------------------------

def changed_files(base: str) -> list[tuple[str, list[str], list[str]]]:
    """(path, added lines, removed lines) for each file changed between `base` and the work tree."""
    files: list[tuple[str, list[str], list[str]]] = []
    old = None
    in_header = False
    for line in git("diff", "--no-renames", "--no-color", "-U0", base, "--").split("\n"):
        if line.startswith("diff --git "):
            in_header = True
        elif in_header and line.startswith("--- "):
            old = line[6:] if line != "--- /dev/null" else None
        elif in_header and line.startswith("+++ "):
            # A deleted file names only its old path.
            files.append((line[6:] if line != "+++ /dev/null" else old, [], []))
            in_header = False
        elif not in_header and files and line.startswith("+"):
            files[-1][1].append(line[1:])
        elif not in_header and files and line.startswith("-"):
            files[-1][2].append(line[1:])
    # A new file not yet added would otherwise escape the scope and the size class.
    for path in git("ls-files", "--others", "--exclude-standard").split("\n"):
        if path and Path(path).is_file():
            files.append((path, Path(path).read_text(errors="replace").split("\n"), []))
    return files


def outside(path: str, scopes: list[str]) -> bool:
    return not any(path == s or path.startswith(s.rstrip("/") + "/") for s in scopes)


def check_diff(args) -> list[str]:
    problems, notes = [], []
    files = changed_files(args.base)
    if args.paths:
        stray = [p for p, _, _ in files if not is_generated(p) and outside(p, args.paths)]
        if stray:
            problems.append(f"changes {', '.join(stray)}, outside the stage's paths "
                            f"({', '.join(args.paths)}); revert them, or amend the issue's paths")
        tracked = git("ls-files").split("\n")
        for scope in args.paths:
            prefix = scope.rstrip("/") + "/"
            crates = sum(f.startswith(prefix) and f.endswith("/Cargo.toml") for f in tracked)
            if crates > 1:
                problems.append(f"{scope} holds {crates} crates; list each one the stage changes")

    lines = 0
    for path, added, removed in files:
        if not is_generated(path) and all(not l.strip() for l in added + removed):
            problems.append(f"{path} changes only blank lines; restore it")
        if not (path.endswith(".md") or path.endswith(".gitignore")):
            lines += len(added)
        if not path.endswith((".rs", ".py", ".c", ".h", ".cpp", ".cc", ".hpp")) or \
                not Path(path).exists():
            continue
        before, after = show(args.base, path), Path(path).read_text(errors="replace")
        for applies, counter, message in RULES:
            if applies(path) and counter(after) > (counter(before) if before is not None else 0):
                problems.append(f"{path} {message}")

    if lines > 1000:
        problems.append(f"adds {lines} lines (XL, over 1000): it cannot be reviewed; split or cut it")
    elif lines > 500:
        notes.append(f"adds {lines} lines (L, up to 1000): reviewers will push back. "
                     "Stop adding: cut first, and question every file the stage touches.")
    elif lines > 250:
        notes.append(f"adds {lines} lines (M, up to 500): delete what the port does not need.")
    for note in notes:
        print(f"note: {note}")
    return problems


# --- Check judges -------------------------------------------------------------------------------

def run(command: list[str]) -> tuple[int, str]:
    """Runs `command` through bash, streaming its output, and returns its status and output."""
    proc = subprocess.Popen(["bash", "-o", "pipefail", "-c", " ".join(command)],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = []
    for line in proc.stdout:
        sys.stdout.write(line)
        out.append(line)
    return proc.wait(), "".join(out)


def judge_miri(log: str) -> list[str]:
    ran = re.search(r"Summary \[[^\]]*\] (\d+) tests? run", log)
    if not ran or not int(ran[1]):
        return ["ran no tests under Miri: nextest reported no summary of tests run"]
    slow = sorted({m.strip() for m in re.findall(r" SLOW \[[^\]]*\] \([^)]*\) (.+)$", log, re.M)})
    if slow:
        return [f"has tests nextest marked slow, which CI's slower runners time out: "
                f"{', '.join(slow)}. Shrink their inputs; never raise the timeout."]
    return []


def judge_mutants(log: str) -> list[str]:
    if not re.search(r"Found [1-9]\d* mutants? to test", log):
        return ["found no mutants in the stage diff, so it shows no test coverage. Point `-p` and "
                "`--in-diff` at the Rust the stage adds; a swap that changes only `unsafe extern` "
                "exports, which cargo mutants skips, needs no mutants check."]
    return []


def bench_filter(command: str) -> str | None:
    """What in a `cargo bench` command selects only some benchmarks."""
    m = re.search(r"cargo bench\b([^;&|]*)", command)
    args = m[1].split() if m else []
    valued = {"-p", "--package", "--manifest-path", "--features", "-F", "--profile", "--target-dir"}
    i = 0
    while i < len(args):
        if args[i] in ("--", "--bench") or args[i].startswith("--bench="):
            return args[i]
        if args[i] in valued:
            i += 1
        elif not args[i].startswith("-"):
            return args[i]
        i += 1
    return None


def criterion_dirs() -> list[Path]:
    """Where Criterion may write; Cargo run from the root and from the workspace disagree on it."""
    dirs = []
    manifest = Path(WORKSPACE, "Cargo.toml").resolve()
    for cwd in (Path.cwd(), Path(WORKSPACE).resolve()):
        meta = subprocess.run(["cargo", "metadata", "--format-version", "1", "--no-deps",
                               "--manifest-path", str(manifest)],
                              cwd=cwd, check=True, capture_output=True, text=True).stdout
        target = Path(json.loads(meta)["target_directory"], "criterion")
        if target not in dirs:
            dirs.append(target)
    return dirs


def judge_bench(since: float) -> list[str]:
    ratios, halves, unreadable = [], [], []
    for root in criterion_dirs():
        groups = {p.parent.parent.parent for p in root.glob("**/new/estimates.json")
                  if p.parent.parent.name in ("c", "rust")}
        for group in sorted(groups):
            medians = {}
            for impl in ("c", "rust"):
                f = group / impl / "new" / "estimates.json"
                if f.exists() and f.stat().st_mtime >= since:
                    try:
                        medians[impl] = float(json.loads(f.read_text())["median"]["point_estimate"])
                    except (ValueError, KeyError):
                        medians[impl] = float("nan")
            name = str(group.relative_to(root))
            if len(medians) == 2:
                if not all(v > 0 for v in medians.values()):
                    unreadable.append(name)
                else:
                    ratios.append((name, medians["rust"] / medians["c"]))
            elif medians:
                halves.append(name)
    if halves:
        return [f"measured only one of `c` and `rust` for: {', '.join(sorted(halves))}"]
    if unreadable:
        return [f"left unreadable Criterion estimates for: {', '.join(unreadable)}"]
    if not ratios:
        return ["measured no Criterion group with `c` and `rust` functions"]
    limit = 1 + SLOWDOWN_PERCENT / 100
    slow = [f"{n} {r:.2f}x" for n, r in ratios if r > limit]
    for n, r in ratios:
        print(f"bench: {n} rust/c = {r:.3f}")
    return [f"is slower than C beyond {limit:.2f}x: {', '.join(slow)}"] if slow else []


def check_command(kind: str, command: list[str]) -> list[str]:
    if not command:
        return [f"{kind} needs a command after --"]
    if kind == "bench":
        f = bench_filter(" ".join(command))
        if f:
            return [f"the bench command filters the benchmarks ({f}); run them all"]
    started = time.time()
    status, log = run(command)
    if status != 0:
        return [f"`{' '.join(command)}` exited with {status}"]
    return {"miri": judge_miri, "mutants": judge_mutants}.get(kind, lambda _: [])(log) \
        if kind != "bench" else judge_bench(started)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="kind", required=True)
    diff = sub.add_parser("diff", help="scope, blank-only files, size class and house rules")
    diff.add_argument("--base", required=True, help="the revision the stage branches from")
    diff.add_argument("--paths", nargs="*", default=[], help="the files and directories the stage may change")
    for kind in ("miri", "mutants", "bench"):
        p = sub.add_parser(kind, help=f"run a {kind} command and judge its output")
        p.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.kind == "diff":
        problems = check_diff(args)
    else:
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        problems = check_command(args.kind, command)
    for problem in problems:
        print(f"{args.kind}: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
