#!/usr/bin/env python3
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
"""Fail when a workflow's expressions cannot be resolved.

`swamp workflow evaluate` reports an expression it could not resolve as a
warning and still exits 0, so on its own it cannot gate anything: a guard
referring to `inputs.coverageFilse` evaluates "successfully" and fails the first
time somebody runs the workflow.

The warnings cannot simply be treated as failures either. Evaluating without
inputs warns about every declared input that was not given a value, which is
most of them and entirely benign. What distinguishes a typo is that the key it
names is not an input the workflow declares at all — so that is what this
checks.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

NO_SUCH_KEY = re.compile(r"No such key: (\w+)")
# Colour, which swamp leaves out when its output is not a terminal — as it is
# not here, being a pipe. Stripped anyway so that the record split below does
# not depend on that: an escape ahead of the timestamp defeats the `^\d{4}`
# anchor, every warning collapses into one record, and a record holding one
# undeclared key alongside a genuine type error is reported for the key and the
# error is dropped with it. One substitution costs nothing next to that.
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# Log records start with a timestamp. A warning about a multi-line expression
# spans several lines and its *reason* is on the last of them, so warnings have
# to be split on the record boundary rather than by line — matching one line
# would classify every multi-line expression by its opening bracket.
RECORD = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z ", re.M)

# Two things are unresolvable at evaluation time by design, and warning about
# them says nothing about whether the workflow is correct.
#
# `run` does not exist until there is a run — every `workflowRunId == run.id`
# scoping produces this, and those are the expressions that most need writing.
#
# Async data lookups are left for the run in some positions. Verified rather
# than assumed: a `manual_approval` prompt containing `data.latest(...)` warns
# here and still renders the resolved value to the operator at runtime.
BENIGN = (
    "Unknown variable: run",
    "Async CEL functions",
)


# The workflow's own name, which is all that has to be read out of the file.
NAME = re.compile(r"^name:\s*(\S+)\s*$", re.M)

# `[0]` closing a `data.query(...)`, which is the whole of what the check below
# looks for; the predicate it belongs to is recovered by scanning backwards.
QUERY_FIRST = re.compile(r"\)\[0\]")


def oldest_record_reads(path: Path) -> list[str]:
    """Return the history-mode queries that read the oldest record, not the newest.

    `version >= 0` switches `data.query` from returning just the latest version
    of a record to returning every version, oldest first. So `[0]` on such a
    query is the first version ever written, where every caller here wants the
    one this run produced — a distinction that only shows up once a run writes a
    record twice, which is what resuming a failed step does. The gate that
    asserts a run built a module then reads the failed build's summary and
    passes.

    Left as a text check rather than something `swamp workflow evaluate` could
    catch: both forms evaluate, and they differ only in which record comes back.
    """
    problems = []
    for number, line in enumerate(path.read_text().split("\n"), 1):
        for match in QUERY_FIRST.finditer(line):
            start = line.rfind("data.query(", 0, match.start())
            if start == -1:
                continue
            if "version >= 0" in line[start:match.start()]:
                problems.append(
                    f"{path.name}:{number}: data.query(...)[0] on a `version >= 0` "
                    "query reads the oldest record; index the newest with "
                    "[<the same query>.size() - 1]"
                )
    return problems


def declared_inputs(name: str, root: Path) -> set[str]:
    """Input names the workflow declares, which may legitimately be unset.

    Asked of swamp rather than parsed out of the YAML. It is the authority on
    what a workflow declares, and reading it this way keeps the gate free of a
    YAML library — this runs in a CI job that installs deno and swamp and no
    Python packages, so an import here is a job that fails before it checks
    anything.
    """
    result = subprocess.run(
        ["swamp", "workflow", "get", name, "--repo-dir", str(root), "--json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return set()
    body = result.stdout[result.stdout.index("{"):]
    document = json.loads(body)
    return set(((document.get("inputs") or {}).get("properties") or {}).keys())


def check(path: Path, root: Path) -> list[str]:
    """Return the problems found in one workflow, empty when it is sound."""
    match = NAME.search(path.read_text())
    if not match:
        return [f"{path}: no name"]
    name = match.group(1)

    problems = oldest_record_reads(path)

    result = subprocess.run(
        ["swamp", "workflow", "evaluate", name, "--repo-dir", str(root)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        problems.append(
            f"{name}: evaluate exited {result.returncode}\n{result.stderr.strip()}"
        )
        return problems

    known = declared_inputs(name, root)
    records = [
        r for r in RECORD.split(ANSI.sub("", result.stderr)) if "Warning:" in r
    ]
    for warning in records:
        keys = NO_SUCH_KEY.findall(warning)
        undeclared = [key for key in keys if key not in known]
        # An undeclared key is reported whatever else the record says. One
        # record covers one expression, and an expression is free to mention
        # both `run.id` and a misspelled input — most of them scope a query by
        # run and then read an input. Letting the benign half of that record
        # excuse the other half is how a typo reaches the only gate that would
        # have caught it and passes.
        if undeclared:
            problems.append(f"{name}: {' '.join(warning.split())[:300]}")
            continue
        # A warning about a declared input is just an input without a value,
        # which is what evaluating with none looks like. Anything else — a type
        # error, a missing overload — is the workflow being wrong.
        if keys:
            continue
        if any(benign in warning for benign in BENIGN):
            continue
        problems.append(f"{name}: {' '.join(warning.split())[:300]}")
    return problems


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    problems = []
    for path in sorted((root / "workflows").glob("*.yaml")):
        problems.extend(check(path, root))

    for problem in problems:
        print(problem, file=sys.stderr)
    if problems:
        print(
            "\nAn expression that cannot be resolved is reported by swamp as a "
            "warning and\nstill exits 0, so this gate reads the warnings. Fix "
            "the expression, or add the\ninput it refers to.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
