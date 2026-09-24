# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import argparse
import os
import re
import subprocess
import xml.etree.ElementTree as ElementTree
from pathlib import Path


def lcov_branch_counts(path: Path) -> tuple[int, int]:
    result = subprocess.run(
        [
            "lcov",
            "--summary",
            path,
            "--branch-coverage",
            "--ignore-errors",
            "inconsistent,corrupt,mismatch,negative",
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if result.returncode != 0:
        raise ValueError(f"lcov could not summarize {path}:\n{result.stdout.rstrip()}")

    match = re.search(
        r"branches\.*:\s*[0-9.]+%\s*\((\d+)\s+of\s+(\d+)\s+branches\)",
        result.stdout,
    )
    if match is None:
        raise ValueError(f"{path} does not contain LCOV branch data")
    return int(match.group(1)), int(match.group(2))


def cobertura_branch_counts(path: Path) -> tuple[int, int]:
    root = ElementTree.parse(path).getroot()
    try:
        covered = int(root.attrib["branches-covered"])
        total = int(root.attrib["branches-valid"])
    except KeyError as error:
        raise ValueError(f"{path} does not contain Cobertura branch totals") from error
    return covered, total


def validate_branch_counts(path: Path, covered: int, total: int) -> None:
    if total <= 0:
        raise ValueError(f"{path} reports no branches")
    if covered < 0 or covered > total:
        raise ValueError(f"{path} reports invalid branch counts: {covered}/{total}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish branch totals from LCOV and Cobertura reports."
    )
    parser.add_argument(
        "--lcov",
        action="append",
        default=[],
        nargs=2,
        metavar=("LABEL", "PATH"),
    )
    parser.add_argument(
        "--cobertura",
        action="append",
        default=[],
        nargs=2,
        metavar=("LABEL", "PATH"),
    )
    args = parser.parse_args()

    reports = [
        (label, Path(path), lcov_branch_counts) for label, path in args.lcov
    ] + [
        (label, Path(path), cobertura_branch_counts)
        for label, path in args.cobertura
    ]
    if not reports:
        parser.error("at least one coverage report is required")

    rows: list[tuple[str, int, int]] = []
    try:
        for label, path, reader in reports:
            covered, total = reader(path)
            validate_branch_counts(path, covered, total)
            rows.append((label, covered, total))
    except (OSError, ElementTree.ParseError, ValueError) as error:
        parser.error(str(error))

    lines = [
        "### Branch coverage",
        "",
        "| Suite | Covered | Total | Coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    for label, covered, total in rows:
        percentage = covered / total * 100
        lines.append(f"| {label} | {covered} | {total} | {percentage:.2f}% |")
    summary = "\n".join(lines) + "\n"

    print(summary)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary_path).open("a", encoding="utf-8") as output:
            output.write(summary)


if __name__ == "__main__":
    main()
