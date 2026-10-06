"""Flaky-test DB CLI — single entry point for all flaky-test operations.

Subcommands:
  fetch    Pull active marks for $REPO/$BRANCH into local files.
  filter   Filter a test list against a marks file (exact or variant-suffix match).
  mark     Add a flaky mark.
  unmark   Remove a flaky mark.
  record   Record failed/passed test ids for the current run.

Context inputs (env):
  REDIS_URL                 connection string (rediss://...)
  REPO                      e.g. RediSearch/RediSearch
  BRANCH                    e.g. master, 2.10, or *
  ACTOR                     github.actor — who triggered the action
  COMMIT                    git sha (record only)
  WORKFLOW_RUN_ID           gh run id (record only)
  RUN_ATTEMPT               gh run attempt number (record only)
  JOB_NAME                  short label for the run/shard (record only)
  TEST_ID, REASON, JIRA_KEY, DAYS  (mark)
  TEST_ID, REASON                  (unmark)
  FAILED_FILE, PASSED_FILE         (record)

Any operation that talks to Redis is a no-op when REDIS_URL is empty — keeps
fork-PR CI green when secrets aren't available.
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query


INDEX_NAME = "idx:marks"
INDEX_PREFIX = "mark:"
RUN_TTL_SECONDS = 60 * 86400
AUDIT_MAXLEN = 100_000
FAILURES_MAXLEN = 50_000

_TAG_ESCAPE_CHARS = set(',.<>{}[]"\':;!@#$%^&*()-+=~ \\/?')
# RLTest LIST=1 emits "<test_file>:<test_name>" (no .py suffix, no variant
# suffix). Class-based tests appear as "<test_file>:<Class>.<method>" — RLTest
# joins them with a dot — so the name half must accept `.`. The variant bracket
# is left in the parse regex in case a future RLTest version starts emitting
# them; prefix matching handles both shapes.
_TEST_ID_RE = re.compile(r"^[A-Za-z0-9_-]+:[A-Za-z_][\w.]*(?:\[[^\]]+\])?\s*$")
# Marks must be variant-less: cmd_filter matches `test == mark` or
# `test.startswith(mark + "[")`, so a mark with a `[variant]` suffix could
# never match anything in current LIST=1 output and would silently no-op.
_MARK_TEST_ID_RE = re.compile(r"^[A-Za-z0-9_-]+:[A-Za-z_][\w.]*\s*$")


def _connect() -> redis.Redis | None:
    """Connect using REDIS_URL. Returns None when the URL is unset (fork PR)."""
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        print("REDIS_URL not set — skipping DB operation")
        return None
    return redis.from_url(url, socket_timeout=10, decode_responses=True)


def _escape_tag(value: str) -> str:
    return "".join(("\\" + c) if c in _TAG_ESCAPE_CHARS else c for c in value)


def _ensure_index(client: redis.Redis) -> None:
    try:
        client.ft(INDEX_NAME).info()
        return
    except redis.ResponseError:
        pass

    schema = (
        TagField("repo", sortable=True),
        TagField("branch", sortable=True),
        TagField("test_id", sortable=True),
        TagField("jira_key", sortable=True),
        TagField("marked_by"),
        NumericField("created_at", sortable=True),
        NumericField("expires_at", sortable=True),
        TextField("reason"),
    )
    definition = IndexDefinition(prefix=[INDEX_PREFIX], index_type=IndexType.HASH)
    try:
        client.ft(INDEX_NAME).create_index(schema, definition=definition)
        print(f"Created {INDEX_NAME}")
    except redis.ResponseError as exc:
        if "already exists" not in str(exc).lower():
            raise


def _summary(lines: list[str]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------- fetch

def cmd_fetch(args: argparse.Namespace) -> int:
    repo = os.environ["REPO"]
    branch = os.environ["BRANCH"]
    now = int(time.time())

    result = {"generated_at": now, "repo": repo, "branch": branch, "skips": [], "status": "ok"}
    client = _connect()

    if client is None:
        result["status"] = "no-url"
    else:
        try:
            _ensure_index(client)
            query_str = (
                f"@repo:{{{_escape_tag(repo)}}} "
                f"(@branch:{{{_escape_tag(branch)}}} | @branch:{{{_escape_tag('*')}}}) "
                f"@expires_at:[({now} +inf]"
            )
            q = (
                Query(query_str)
                .return_fields("test_id", "branch", "reason", "jira_key", "marked_by", "expires_at")
                .paging(0, 10000)
            )
            res = client.ft(INDEX_NAME).search(q)
            # Prefer branch-specific rows over wildcard rows for the same test_id.
            docs = sorted(res.docs, key=lambda d: 0 if getattr(d, "branch", "") == branch else 1)
            seen: set[str] = set()
            for doc in docs:
                tid = getattr(doc, "test_id", "")
                if not tid or tid in seen:
                    continue
                seen.add(tid)
                result["skips"].append({
                    "test_id": tid,
                    "reason": getattr(doc, "reason", ""),
                    "jira_key": getattr(doc, "jira_key", ""),
                    "marked_by": getattr(doc, "marked_by", ""),
                    "expires_at": int(getattr(doc, "expires_at", 0) or 0),
                    "applied_via": "branch" if getattr(doc, "branch", "") == branch else "wildcard",
                })
        except Exception as exc:  # noqa: BLE001
            result["status"] = "error"
            result["error"] = str(exc)
            print(f"::warning::Could not fetch flaky list: {exc}")

    Path(args.output_json).write_text(json.dumps(result, indent=2))
    Path(args.output_list).write_text(
        "\n".join(s["test_id"] for s in result["skips"]) + ("\n" if result["skips"] else "")
    )

    print(f"Wrote {len(result['skips'])} skip(s) to {args.output_json} / {args.output_list}")
    for skip in result["skips"]:
        jira = f" [{skip['jira_key']}]" if skip["jira_key"] else ""
        scope = "" if skip["applied_via"] == "branch" else " (wildcard)"
        print(f"  - {skip['test_id']}{jira}{scope}")

    if result["skips"]:
        _summary([
            f"### Flaky tests skipped on `{branch}` ({len(result['skips'])})",
            "",
            "| Test | Jira | Reason | Scope |",
            "|---|---|---|---|",
            *(
                f"| `{s['test_id']}` | {s['jira_key']} | {s['reason']} | {s['applied_via']} |"
                for s in result["skips"]
            ),
        ])
    return 0


# ---------------------------------------------------------------- filter

def cmd_filter(args: argparse.Namespace) -> int:
    raw = Path(args.tests).read_text().splitlines()
    tests = [l.strip() for l in raw if _TEST_ID_RE.match(l.strip())]
    marks = [l.strip() for l in Path(args.marks).read_text().splitlines() if l.strip()]

    print(f"Input: {len(raw)} lines, {len(tests)} look like test ids, {len(marks)} mark(s)")

    if not tests:
        print("::warning::No recognisable test ids; writing empty TESTFILE")
        Path(args.output).write_text("")
        return 0
    if not marks:
        Path(args.output).write_text("\n".join(tests) + "\n")
        return 0

    kept: list[str] = []
    skipped: list[tuple[str, str]] = []
    for t in tests:
        m = next((mark for mark in marks if t == mark or t.startswith(mark + "[")), None)
        if m:
            skipped.append((t, m))
        else:
            kept.append(t)

    Path(args.output).write_text("\n".join(kept) + ("\n" if kept else ""))
    print(f"Filter: {len(tests)} -> {len(kept)} kept, {len(skipped)} skipped")
    for tid, mark in skipped[:50]:
        print(f"  - {tid}  (matched mark {mark!r})")
    if len(skipped) > 50:
        print(f"  ... and {len(skipped) - 50} more")

    if skipped:
        _summary([
            "",
            f"**Flaky-skipped from this run** ({len(skipped)}):",
            *(f"- `{tid}` (mark `{mark}`)" for tid, mark in skipped[:50]),
        ])
    return 0


# ---------------------------------------------------------------- mark / unmark

def cmd_mark(args: argparse.Namespace) -> int:
    repo, branch, actor = os.environ["REPO"], os.environ["BRANCH"], os.environ["ACTOR"]
    test_id = os.environ["TEST_ID"].strip()
    reason = os.environ["REASON"].strip()
    jira_key = os.environ["JIRA_KEY"].strip()

    for name, value in (("test_id", test_id), ("reason", reason), ("jira_key", jira_key)):
        if not value:
            print(f"::error::{name} is empty")
            return 1

    if not _MARK_TEST_ID_RE.match(test_id):
        print(f"::error::test_id {test_id!r} doesn't match the RLTest format "
              "'<test_file>:<test_name>' (no .py, no [variant])")
        return 1

    try:
        days = int(os.environ["DAYS"])
    except ValueError:
        print(f"::error::expires_in_days must be an integer, got: {os.environ['DAYS']!r}")
        return 1
    if not 1 <= days <= 365:
        print(f"::error::expires_in_days must be between 1 and 365, got {days}")
        return 1

    client = _connect()
    if client is None:
        print("::error::REDIS_URL is required for mark")
        return 1

    _ensure_index(client)

    now = int(time.time())
    expires = now + days * 86400
    key = f"mark:{repo}:{branch}:{test_id}"

    with client.pipeline() as pipe:
        pipe.hset(key, mapping={
            "repo": repo, "branch": branch, "test_id": test_id,
            "reason": reason, "jira_key": jira_key, "marked_by": actor,
            "created_at": now, "expires_at": expires,
        })
        pipe.expireat(key, expires)
        pipe.xadd("marks:audit", {
            "action": "mark", "repo": repo, "branch": branch, "test_id": test_id,
            "by": actor, "reason": reason, "jira_key": jira_key, "expires_at": str(expires),
        }, maxlen=AUDIT_MAXLEN, approximate=True)
        pipe.execute()

    print(f"Marked '{test_id}' as flaky on {repo}@{branch}")
    print(f"  Reason:  {reason}")
    print(f"  Jira:    {jira_key}")
    print(f"  Expires: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(expires))} ({days}d)")
    return 0


def cmd_unmark(args: argparse.Namespace) -> int:
    repo, branch, actor = os.environ["REPO"], os.environ["BRANCH"], os.environ["ACTOR"]
    test_id = os.environ["TEST_ID"].strip()
    reason = os.environ["REASON"].strip()

    if not test_id or not reason:
        print("::error::test_id and reason are required")
        return 1

    if not _MARK_TEST_ID_RE.match(test_id):
        print(f"::error::test_id {test_id!r} doesn't match the RLTest format "
              "'<test_file>:<test_name>' (no .py, no [variant])")
        return 1

    client = _connect()
    if client is None:
        print("::error::REDIS_URL is required for unmark")
        return 1

    key = f"mark:{repo}:{branch}:{test_id}"
    prev = client.hgetall(key)
    existed = bool(prev)

    with client.pipeline() as pipe:
        pipe.delete(key)
        pipe.xadd("marks:audit", {
            "action": "unmark", "repo": repo, "branch": branch, "test_id": test_id,
            "by": actor, "reason": reason,
            "prev_jira_key": prev.get("jira_key", ""),
            "prev_marked_by": prev.get("marked_by", ""),
        }, maxlen=AUDIT_MAXLEN, approximate=True)
        pipe.execute()

    if not existed:
        print(f"::warning::'{test_id}' was not marked on {repo}@{branch} — nothing to remove")
        print("Audit entry was still recorded.")
        return 0

    print(f"Unmarked '{test_id}' on {repo}@{branch}")
    print(f"  Reason:        {reason}")
    if prev.get("marked_by"):
        print(f"  Was marked by: {prev['marked_by']}")
    if prev.get("jira_key"):
        print(f"  Original jira: {prev['jira_key']}")
    return 0


# ---------------------------------------------------------------- record

def _read_lines(path: str) -> list[str]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    return [line.strip() for line in p.read_text().splitlines() if line.strip()]


def cmd_record(args: argparse.Namespace) -> int:
    failed = _read_lines(os.environ.get("FAILED_FILE", ""))
    passed = _read_lines(os.environ.get("PASSED_FILE", ""))
    if not failed and not passed:
        print("No results to record (FAILED_FILE/PASSED_FILE empty or unset)")
        return 0

    client = _connect()
    if client is None:
        return 0  # fork PR or DB unset; recording is best-effort

    repo = os.environ["REPO"]
    branch = os.environ["BRANCH"]
    commit = os.environ.get("COMMIT", "")
    workflow_run_id = os.environ.get("WORKFLOW_RUN_ID", "0")
    run_attempt = os.environ.get("RUN_ATTEMPT", "1")
    job = os.environ.get("JOB_NAME", "default")

    print(f"Recording {len(failed)} failure(s), {len(passed)} pass(es)")

    now = int(time.time())
    expires_at = now + RUN_TTL_SECONDS

    try:
        with client.pipeline(transaction=False) as pipe:
            for status, test_ids in (("failed", failed), ("passed", passed)):
                for test_id in test_ids:
                    key = f"run:{workflow_run_id}:{run_attempt}:{job}:{test_id}"
                    pipe.hset(key, mapping={
                        "repo": repo, "branch": branch, "test_id": test_id, "status": status,
                        "commit": commit, "workflow_run_id": workflow_run_id,
                        "run_attempt": run_attempt, "job": job, "recorded_at": now,
                    })
                    pipe.expireat(key, expires_at)
                    if status == "failed":
                        pipe.xadd(f"failures:{repo}:{branch}", {
                            "test_id": test_id, "commit": commit,
                            "workflow_run_id": workflow_run_id, "run_attempt": run_attempt,
                            "job": job, "recorded_at": str(now),
                        }, maxlen=FAILURES_MAXLEN, approximate=True)
            pipe.execute()
    except Exception as exc:  # noqa: BLE001
        print(f"::warning::Failed writing results to flaky DB: {exc}")
        return 0

    _summary([
        "### Flaky DB updated",
        "",
        "| | Count |",
        "|---|---|",
        f"| Failures recorded | {len(failed)} |",
        f"| Passes recorded | {len(passed)} |",
        f"| Job | `{job}` |",
        f"| Branch | `{branch}` |",
    ])
    if failed:
        _summary(["", "**Failed:**", *(f"- `{tid}`" for tid in failed[:50])])
        if len(failed) > 50:
            _summary([f"- … and {len(failed) - 50} more"])
    return 0


# ---------------------------------------------------------------- main

def main() -> int:
    parser = argparse.ArgumentParser(prog="flaky_db.py")
    # Context flags: override the matching env var when present. Lets local
    # users run the script without exporting a pile of envs.
    parser.add_argument("--redis-url", help="Overrides REDIS_URL")
    parser.add_argument("--repo", help="Overrides REPO")
    parser.add_argument("--branch", help="Overrides BRANCH")
    parser.add_argument("--actor", help="Overrides ACTOR")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("fetch", help="Fetch active marks into local files")
    p.add_argument("--output-json", default=".flaky_skip.json")
    p.add_argument("--output-list", default=".flaky_skip.txt")
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("filter", help="Filter a test list against marks (exact or variant-suffix match)")
    p.add_argument("--tests", required=True)
    p.add_argument("--marks", required=True)
    p.add_argument("--output", required=True)
    p.set_defaults(func=cmd_filter)

    p = sub.add_parser("mark", help="Add a flaky mark")
    p.add_argument("--test-id", help="Overrides TEST_ID")
    p.add_argument("--reason", help="Overrides REASON")
    p.add_argument("--jira-key", help="Overrides JIRA_KEY")
    p.add_argument("--expires-in-days", type=int, help="Overrides DAYS")
    p.set_defaults(func=cmd_mark)

    p = sub.add_parser("unmark", help="Remove a flaky mark")
    p.add_argument("--test-id", help="Overrides TEST_ID")
    p.add_argument("--reason", help="Overrides REASON")
    p.set_defaults(func=cmd_unmark)

    p = sub.add_parser("record", help="Record failed/passed test ids")
    p.add_argument("--failed-file", help="Overrides FAILED_FILE")
    p.add_argument("--passed-file", help="Overrides PASSED_FILE")
    p.add_argument("--commit", help="Overrides COMMIT")
    p.add_argument("--workflow-run-id", help="Overrides WORKFLOW_RUN_ID")
    p.add_argument("--run-attempt", help="Overrides RUN_ATTEMPT")
    p.add_argument("--job-name", help="Overrides JOB_NAME")
    p.set_defaults(func=cmd_record)

    args = parser.parse_args()

    # Mirror provided CLI flags into env vars so the cmd_* functions can keep
    # reading from os.environ (and CI workflows that set env vars keep working).
    _arg_to_env = {
        "redis_url": "REDIS_URL", "repo": "REPO", "branch": "BRANCH", "actor": "ACTOR",
        "test_id": "TEST_ID", "reason": "REASON", "jira_key": "JIRA_KEY",
        "expires_in_days": "DAYS",
        "failed_file": "FAILED_FILE", "passed_file": "PASSED_FILE",
        "commit": "COMMIT", "workflow_run_id": "WORKFLOW_RUN_ID",
        "run_attempt": "RUN_ATTEMPT", "job_name": "JOB_NAME",
    }
    for attr, env_name in _arg_to_env.items():
        value = getattr(args, attr, None)
        if value is not None:
            os.environ[env_name] = str(value)

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
