# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Build a GitHub PR review payload from an AI review's structured output.

The review session cannot post to GitHub itself, so it returns
`{"summary": str, "comments": [{"path", "line", "body"}]}` and this script
turns that into one `COMMENT` review. GitHub rejects the whole review if any
inline comment names a line outside the diff, so such comments are moved into
the review body instead of being dropped.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")

# GitHub rejects a review body over 65536 characters, which loses the whole
# review; stay well under it.
MAX_BODY = 60000
TRUNCATED = "\n\n*Truncated: the review exceeded GitHub's size limit.*"


def commentable_lines(diff: str) -> set[tuple[str, int]]:
    """Return the `(path, line)` pairs a RIGHT-side review comment may target."""
    lines: set[tuple[str, int]] = set()
    path: str | None = None
    line = 0
    in_hunk = False
    for text in diff.splitlines():
        if text.startswith("diff --git "):
            path, in_hunk = None, False
        elif not in_hunk and text.startswith("+++ "):
            target = text[4:]
            path = target[2:] if target.startswith("b/") else None
        elif m := HUNK.match(text):
            line, in_hunk = int(m.group(1)), True
        elif in_hunk and path is not None:
            if text.startswith((" ", "+")) or text == "":
                lines.add((path, line))
                line += 1
            elif text.startswith("-") or text.startswith("\\"):
                pass
            else:
                in_hunk = False
    return lines


def build_review(output: dict, diff: str, commit_id: str) -> dict:
    allowed = commentable_lines(diff)
    inline, outside = [], []
    for c in output.get("comments", []):
        key = (c["path"], int(c["line"]))
        if key in allowed:
            inline.append(
                {"path": c["path"], "line": key[1], "side": "RIGHT", "body": c["body"]}
            )
        else:
            outside.append(c)

    body = output["summary"].strip()
    if outside:
        body += "\n\n### Findings outside the diff\n"
        for c in outside:
            body += f"\n**`{c['path']}:{c['line']}`**\n\n{c['body'].strip()}\n"
    if len(body) > MAX_BODY:
        body = body[: MAX_BODY - len(TRUNCATED)] + TRUNCATED
    return {"commit_id": commit_id, "event": "COMMENT", "body": body, "comments": inline}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", required=True, help="structured output JSON file")
    ap.add_argument("--diff", required=True, help="the PR diff")
    ap.add_argument("--commit", required=True, help="head commit the review targets")
    args = ap.parse_args()

    output = json.loads(Path(args.output).read_text())
    review = build_review(output, Path(args.diff).read_text(), args.commit)
    json.dump(review, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
