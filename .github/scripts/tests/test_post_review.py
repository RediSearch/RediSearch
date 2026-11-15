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

    def test_no_comments(self) -> None:
        review = post_review.build_review({"summary": "ok", "comments": []}, DIFF, "c")
        self.assertEqual(review["comments"], [])
        self.assertEqual(review["body"], "ok")


if __name__ == "__main__":
    unittest.main()
