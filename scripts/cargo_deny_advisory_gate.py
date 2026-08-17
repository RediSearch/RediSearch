#!/usr/bin/env python3
"""
Gate cargo-deny advisories.

By default, this preserves cargo-deny's exit code. With
--compare-to-base, it compares the current checkout to --base-ref and
fails only for advisory findings that are new.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def cargo_deny(repo: Path, output: Path, manifest: str) -> int:
    cmd = [
        "cargo",
        "deny",
        "--format",
        "json",
        "--manifest-path",
        manifest,
        "--all-features",
        "check",
        "--audit-compatible-output",
        "advisories",
    ]
    with output.open("w", encoding="utf-8") as fh:
        return subprocess.run(cmd, cwd=repo, stdout=fh, stderr=subprocess.STDOUT, text=True).returncode


def finding_from_object(value: dict[str, Any]) -> tuple[str, str, str] | None:
    package = value.get("package") if isinstance(value.get("package"), dict) else None
    advisory = value.get("advisory") if isinstance(value.get("advisory"), dict) else None
    if not package or not advisory or not advisory.get("id"):
        return None

    return (
        str(advisory["id"]).upper(),
        str(package.get("name") or ""),
        str(package.get("version") or ""),
    )


def collect_findings(value: Any, findings: set[tuple[str, str, str]]) -> None:
    if isinstance(value, list):
        for item in value:
            collect_findings(item, findings)
        return

    if isinstance(value, dict):
        if finding := finding_from_object(value):
            findings.add(finding)
        for item in value.values():
            collect_findings(item, findings)


def parse_findings(path: Path) -> set[tuple[str, str, str]]:
    findings: set[tuple[str, str, str]] = set()

    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.lstrip().startswith("{"):
                try:
                    collect_findings(json.loads(line), findings)
                except json.JSONDecodeError:
                    pass
    return findings


def print_findings(title: str, findings: set[tuple[str, str, str]]) -> None:
    print(title)
    if not findings:
        print("  <none>")
        return
    for advisory_id, package, version in sorted(findings):
        crate = package or "<unknown package>"
        print(f"  - {advisory_id}: {crate}{f' {version}' if version else ''}")


def fail_if_unparsed(rc: int, findings: set[tuple[str, str, str]], label: str) -> None:
    if rc != 0 and not findings:
        print(f"cargo deny failed for {label}, but no advisory findings could be parsed.")
        print("Failing conservatively so CI does not hide a tool or configuration error.")
        sys.exit(rc)


def add_base_worktree(base_ref: str, out_dir: Path) -> Path:
    worktree = Path(tempfile.mkdtemp(prefix="cargo-deny-base-", dir=out_dir))
    shutil.rmtree(worktree)
    try:
        subprocess.run(["git", "fetch", "--no-tags", "--depth=1", "origin", base_ref], check=True)
        subprocess.run(["git", "worktree", "add", "--detach", str(worktree), "FETCH_HEAD"], check=True)
    except Exception:
        shutil.rmtree(worktree, ignore_errors=True)
        raise
    return worktree


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compare-to-base", action="store_true")
    parser.add_argument("--base-ref", default="")
    parser.add_argument("--manifest-path", required=True)
    args = parser.parse_args()

    out_dir = Path(os.getenv("RUNNER_TEMP", tempfile.gettempdir()))
    head_out = out_dir / "cargo-deny-advisories-head.jsonl"

    head_rc = cargo_deny(Path.cwd(), head_out, args.manifest_path)
    head_findings = parse_findings(head_out)
    fail_if_unparsed(head_rc, head_findings, "current checkout")

    if not args.compare_to_base:
        print_findings("Current advisory findings:", head_findings)
        return head_rc

    if head_rc == 0:
        print_findings("Current advisory findings:", head_findings)
        print("No failing advisory findings in the current checkout.")
        return 0

    if not args.base_ref:
        print("--base-ref is required when --compare-to-base is used.")
        return 2

    worktree = add_base_worktree(args.base_ref, out_dir)
    try:
        base_out = out_dir / "cargo-deny-advisories-base.jsonl"
        base_rc = cargo_deny(worktree, base_out, args.manifest_path)
        base_findings = parse_findings(base_out)
        fail_if_unparsed(base_rc, base_findings, "base checkout")
    finally:
        subprocess.run(["git", "worktree", "remove", "--force", str(worktree)], check=False)
        shutil.rmtree(worktree, ignore_errors=True)

    new_findings = head_findings - base_findings
    print_findings("Base advisory findings:", base_findings)
    print_findings("Current advisory findings:", head_findings)
    print_findings("New advisory findings:", new_findings)
    return 1 if new_findings else 0


if __name__ == "__main__":
    sys.exit(main())
