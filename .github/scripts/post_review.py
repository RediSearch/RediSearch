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

HUNK = re.compile(r"^@@ -\d+(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")

# GitHub rejects a review body or comment over 65536 characters, which loses
# the whole review; stay well under it.
MAX_BODY = 60000
TRUNCATED = "\n\n*Truncated: the review exceeded GitHub's size limit.*"


def commentable_lines(diff: str) -> set[tuple[str, int]]:
    """Return the `(path, line)` pairs a RIGHT-side review comment may target.

    Each hunk is walked by the line counts in its `@@` header, so hunk content
    is never mistaken for a file header. Paths git quotes (non-ASCII names) are
    not decoded; comments on them land in the review body.
    """
    lines: set[tuple[str, int]] = set()
    path: str | None = None
    line = old_left = new_left = 0
    # Only "\n" ends a diff line; str.splitlines() also splits on form feeds
    # and other separators that can appear inside a line's content.
    for text in diff.split("\n"):
        if old_left > 0 or new_left > 0:
            if text.startswith("\\"):
                continue
            if text.startswith(("+", " ")):
                if path is not None:
                    lines.add((path, line))
                line, new_left = line + 1, new_left - 1
            if text.startswith(("-", " ")):
                old_left -= 1
        elif text.startswith("diff --git "):
            path = None
        elif text.startswith("+++ "):
            # Git appends a tab to a name containing a space.
            target = text[4:].rstrip("\t")
            path = target[2:] if target.startswith("b/") else None
        elif m := HUNK.match(text):
            old_count, start, new_count = m.groups()
            old_left = 1 if old_count is None else int(old_count)
            new_left = 1 if new_count is None else int(new_count)
            line = int(start)
    return lines


def truncate(text: str, limit: int = MAX_BODY) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - len(TRUNCATED))] + TRUNCATED


def build_review(output: dict, diff: str, commit_id: str, inline: bool = True) -> dict:
    """Build the review; with `inline=False`, every comment goes in the body.

    The `inline=False` form is the fallback for a review GitHub rejected, so it
    leaves out `commit_id` (GitHub then uses the PR's current head, which still
    exists after a force-push) and names the reviewed commit in the body.
    """
    allowed = commentable_lines(diff) if inline else set()
    comments, outside = [], []
    for c in output["comments"]:
        if (c["path"], c["line"]) in allowed:
            comments.append(
                {"path": c["path"], "line": c["line"], "side": "RIGHT", "body": truncate(c["body"])}
            )
        else:
            outside.append(c)

    section = ""
    if outside:
        heading = "Findings outside the diff" if inline else "Findings (inline posting was rejected)"
        section = f"\n\n### {heading}\n"
        for c in outside:
            section += f"\n**`{c['path']}:{c['line']}`**\n\n{c['body'].strip()}\n"
    if not inline:
        section += f"\n*Reviewed commit `{commit_id}`.*\n"
    # Cut the summary before the findings: they exist nowhere else.
    summary = truncate(output["summary"].strip(), MAX_BODY - len(section))
    review = {"event": "COMMENT", "body": truncate(summary + section), "comments": comments}
    if inline:
        review["commit_id"] = commit_id
    return review


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output", required=True, help="structured output JSON file")
    ap.add_argument("--diff", required=True, help="the PR diff")
    ap.add_argument("--commit", required=True, help="head commit the review targets")
    ap.add_argument(
        "--no-inline",
        action="store_true",
        help="put every comment in the body, for when GitHub rejects an anchor",
    )
    args = ap.parse_args()

    output = json.loads(Path(args.output).read_text())
    review = build_review(
        output, Path(args.diff).read_text(), args.commit, inline=not args.no_inline
    )
    json.dump(review, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
