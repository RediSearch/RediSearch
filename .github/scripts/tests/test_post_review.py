# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import post_review  # noqa: E402

DIFF = """\
diff --git a/src/a.c b/src/a.c
index 1111111..2222222 100644
--- a/src/a.c
+++ b/src/a.c
@@ -10,3 +10,4 @@ int f(void) {
 int x = 1;
-int y = 2;
+int y = 3;
+int z = 4;
 return x;
diff --git a/old.txt b/old.txt
deleted file mode 100644
--- a/old.txt
+++ /dev/null
@@ -1 +0,0 @@
-gone
diff --git a/new.txt b/new.txt
new file mode 100644
--- /dev/null
+++ b/new.txt
@@ -0,0 +1,2 @@
+one
+two
\\ No newline at end of file
"""


class PostReviewTests(unittest.TestCase):
    def test_commentable_lines_follow_the_right_side(self) -> None:
        self.assertEqual(
            post_review.commentable_lines(DIFF),
            {
                ("src/a.c", 10),
                ("src/a.c", 11),
                ("src/a.c", 12),
                ("src/a.c", 13),
                ("new.txt", 1),
                ("new.txt", 2),
            },
        )

    def test_hunk_content_that_looks_like_headers(self) -> None:
        # Two hunks; an added line whose content starts with "++ ", a removed
        # line whose content starts with "-- a/", and a form feed in content.
        diff = (
            "diff --git a/x.txt b/x.txt\n"
            "--- a/x.txt\n"
            "+++ b/x.txt\n"
            "@@ -1,2 +1,3 @@\n"
            " one\n"
            "+++ b/not-a-header\n"
            "--- a/not-a-header\n"
            "+two\x0cstill two\n"
            "@@ -20 +21 @@\n"
            "-old\n"
            "+new\n"
        )
        self.assertEqual(
            post_review.commentable_lines(diff),
            {("x.txt", 1), ("x.txt", 2), ("x.txt", 3), ("x.txt", 21)},
        )

    def test_renamed_file_uses_the_new_path(self) -> None:
        diff = (
            "diff --git a/old name.c b/new name.c\n"
            "similarity index 90%\n"
            "rename from old name.c\n"
            "rename to new name.c\n"
            "--- a/old name.c\t\n"
            "+++ b/new name.c\t\n"
            "@@ -1,2 +1,2 @@\n"
            " keep\n"
            "-a\n"
            "+b\n"
        )
        self.assertEqual(
            post_review.commentable_lines(diff),
            {("new name.c", 1), ("new name.c", 2)},
        )

    def test_oversized_inline_comment_is_truncated(self) -> None:
        output = {
            "summary": "s",
            "comments": [{"path": "src/a.c", "line": 12, "body": "y" * 100000}],
        }
        comment = post_review.build_review(output, DIFF, "c")["comments"][0]
        self.assertEqual(len(comment["body"]), post_review.MAX_BODY)
        self.assertTrue(comment["body"].endswith(post_review.TRUNCATED))

    def test_truncation_keeps_the_findings_outside_the_diff(self) -> None:
        output = {
            "summary": "x" * post_review.MAX_BODY,
            "comments": [{"path": "src/a.c", "line": 40, "body": "far away"}],
        }
        body = post_review.build_review(output, DIFF, "c")["body"]
        self.assertLessEqual(len(body), post_review.MAX_BODY)
        self.assertIn("`src/a.c:40`", body)
        self.assertIn("far away", body)
        self.assertIn(post_review.TRUNCATED, body)

    def test_no_inline_moves_every_comment_into_the_body(self) -> None:
        output = {
            "summary": "s",
            "comments": [{"path": "src/a.c", "line": 12, "body": "z is unused"}],
        }
        review = post_review.build_review(output, DIFF, "abc123", inline=False)
        self.assertEqual(review["comments"], [])
        self.assertNotIn("commit_id", review)
        self.assertIn("`src/a.c:12`", review["body"])
        self.assertIn("inline posting was rejected", review["body"])
        self.assertIn("`abc123`", review["body"])

    def test_findings_filling_the_body_drop_the_summary_first(self) -> None:
        output = {
            "summary": "SUMMARY" * 10000,
            "comments": [
                {"path": "src/a.c", "line": 40 + i, "body": "f" * 1000} for i in range(59)
            ],
        }
        body = post_review.build_review(output, DIFF, "c")["body"]
        self.assertLessEqual(len(body), post_review.MAX_BODY)
        self.assertNotIn("SUMMARY", body)
        self.assertIn("`src/a.c:40`", body)

    def test_comments_outside_the_diff_move_into_the_body(self) -> None:
        output = {
            "summary": "Verdict: fine.",
            "comments": [
                {"path": "src/a.c", "line": 12, "body": "z is unused"},
                {"path": "src/a.c", "line": 40, "body": "far away"},
                {"path": "old.txt", "line": 1, "body": "deleted file"},
            ],
        }
        review = post_review.build_review(output, DIFF, "abc123")

        self.assertEqual(review["event"], "COMMENT")
        self.assertEqual(review["commit_id"], "abc123")
        self.assertEqual(
            review["comments"],
            [{"path": "src/a.c", "line": 12, "side": "RIGHT", "body": "z is unused"}],
        )
        self.assertTrue(review["body"].startswith("Verdict: fine."))
        self.assertIn("`src/a.c:40`", review["body"])
        self.assertIn("`old.txt:1`", review["body"])

    def test_oversized_body_is_truncated(self) -> None:
        output = {"summary": "x" * 100000, "comments": []}
        body = post_review.build_review(output, DIFF, "c")["body"]
        self.assertEqual(len(body), post_review.MAX_BODY)
        self.assertTrue(body.endswith(post_review.TRUNCATED))

    def test_no_comments(self) -> None:
        review = post_review.build_review({"summary": "ok", "comments": []}, DIFF, "c")
        self.assertEqual(review["comments"], [])
        self.assertEqual(review["body"], "ok")


if __name__ == "__main__":
    unittest.main()
