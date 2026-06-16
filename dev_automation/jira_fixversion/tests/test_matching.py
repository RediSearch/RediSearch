# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Offline unit tests for the pure matching engine (design §8).

Run with: ``python3 -m unittest discover -s dev_automation/jira_fixversion``
"""

import unittest

from dev_automation.jira_fixversion.matching import (
    PullRequest,
    ReleaseTemplates,
    find_release_exact,
    handle_redisearch,
    handlers_for_repo,
    is_version_branch,
    open_source_highest_version,
    open_source_minor_unreleased,
)
from dev_automation.jira_fixversion.version_parse import (
    VersionParseError,
    parse_version_h,
)


def vh(x, y, z):
    """Build a minimal src/version.h body declaring version (x, y, z)."""
    return (
        "#pragma once\n"
        f"#define REDISEARCH_VERSION_MAJOR {x}\n"
        f"#define REDISEARCH_VERSION_MINOR {y}\n"
        f"#define REDISEARCH_VERSION_PATCH {z}\n"
    )


# Project-versions fixture. Open Source releases are two-part (major.minor) only.
# Covers released/unreleased filtering and numeric-vs-lexical ordering (8.10 > 8.8).
VERSIONS = [
    {"id": "1", "name": "RediSearch v6.6.0", "released": False},
    {"id": "2", "name": "Open Source 6.6", "released": False},    # the X.Y match for 6.6.z
    {"id": "5", "name": "Open Source 8.8", "released": False},
    {"id": "6", "name": "Open Source 8.10", "released": False},   # highest two-part
    {"id": "7", "name": "Open Source 9.0", "released": True},     # highest but released -> ignored
    {"id": "9", "name": "Open Source 6.4", "released": True},     # released -> excluded
    {"id": "10", "name": "Open Source 7.2", "released": False, "archived": True},  # archived -> excluded
]


class TestVersionParse(unittest.TestCase):
    def test_concrete(self):
        self.assertEqual(parse_version_h(vh(6, 6, 0)), (6, 6, 0))

    def test_sentinel(self):
        self.assertEqual(parse_version_h(vh(99, 99, 99)), (99, 99, 99))

    def test_ignores_module_version_macro(self):
        text = vh(2, 8, 5) + "#define REDISEARCH_MODULE_VERSION 20805\n"
        self.assertEqual(parse_version_h(text), (2, 8, 5))

    def test_missing_macro_raises(self):
        with self.assertRaises(VersionParseError):
            parse_version_h("#define REDISEARCH_VERSION_MAJOR 6\n")


class TestSelectionHelpers(unittest.TestCase):
    def test_is_version_branch(self):
        self.assertTrue(is_version_branch("2.8"))
        self.assertTrue(is_version_branch("6.6"))
        self.assertFalse(is_version_branch("master"))
        self.assertFalse(is_version_branch("2.8.1"))
        self.assertFalse(is_version_branch("feature/2.8"))

    def test_find_release_exact(self):
        self.assertEqual(find_release_exact(VERSIONS, "RediSearch v6.6.0")["id"], "1")
        self.assertIsNone(find_release_exact(VERSIONS, "RediSearch v6.6.9"))

    def test_open_source_minor_unreleased(self):
        # 6.6.z maps to the two-part "Open Source 6.6"
        self.assertEqual(open_source_minor_unreleased(VERSIONS, 6, 6)["id"], "2")

    def test_open_source_minor_none(self):
        self.assertIsNone(open_source_minor_unreleased(VERSIONS, 7, 7))

    def test_open_source_minor_released_excluded(self):
        # "Open Source 6.4" exists but is released -> not a candidate
        self.assertIsNone(open_source_minor_unreleased(VERSIONS, 6, 4))

    def test_open_source_minor_archived_excluded(self):
        # "Open Source 7.2" is unreleased but archived -> not a candidate
        self.assertIsNone(open_source_minor_unreleased(VERSIONS, 7, 2))

    def test_open_source_highest_numeric_order(self):
        # 8.10 > 8.8 numerically; 9.0 is released and excluded
        self.assertEqual(open_source_highest_version(VERSIONS)["id"], "6")


class TestRediSearchHandler(unittest.TestCase):
    def test_master_sentinel_picks_highest_unreleased(self):
        pr = PullRequest(repo="RediSearch", head_branch="feat", base_branch="master", pr_number=1)
        lookups = handle_redisearch(pr, VERSIONS, vh(99, 99, 99))
        self.assertEqual(len(lookups), 1)
        self.assertEqual(lookups[0].release["id"], "6")  # Open Source 8.10

    def test_sentinel_on_version_branch_is_mismatch_alert(self):
        # 99.99.99 on a version branch (un-rebased backport) -> alert, not master rule.
        pr = PullRequest(repo="RediSearch", head_branch="bp", base_branch="6.6", pr_number=9)
        lookups = handle_redisearch(pr, VERSIONS, vh(99, 99, 99))
        self.assertEqual(len(lookups), 1)
        self.assertIsNone(lookups[0].release)  # mismatch -> alert
        self.assertIn("99.99.99", lookups[0].searched_name)

    def test_version_branch_both_lookups_found(self):
        pr = PullRequest(repo="RediSearch", head_branch="fix", base_branch="6.6", pr_number=2)
        lookups = handle_redisearch(pr, VERSIONS, vh(6, 6, 0))
        self.assertEqual(len(lookups), 2)
        self.assertEqual(lookups[0].release["id"], "1")  # RediSearch v6.6.0
        self.assertEqual(lookups[1].release["id"], "2")  # Open Source 6.6

    def test_version_branch_missing_exact_alerts(self):
        pr = PullRequest(repo="RediSearch", head_branch="fix", base_branch="6.6", pr_number=3)
        lookups = handle_redisearch(pr, VERSIONS, vh(6, 6, 9))
        self.assertIsNone(lookups[0].release)  # RediSearch v6.6.9 missing -> alert
        self.assertEqual(lookups[0].searched_name, "RediSearch v6.6.9")
        self.assertEqual(lookups[1].release["id"], "2")  # OSS still found independently

    def test_non_version_base_ignored(self):
        pr = PullRequest(repo="RediSearch", head_branch="x", base_branch="feature/foo", pr_number=4)
        self.assertEqual(handle_redisearch(pr, VERSIONS, vh(6, 6, 0)), [])

    def test_oss_released_emits_no_oss_lookup(self):
        # "Open Source 9.0" exists but is released -> shipped; no OSS lookup/alert.
        pr = PullRequest(repo="RediSearch", head_branch="x", base_branch="9.0", pr_number=7)
        lookups = handle_redisearch(pr, VERSIONS, vh(9, 0, 0))
        self.assertEqual(len(lookups), 1)  # only RediSearch v9.0.0
        self.assertEqual(lookups[0].searched_name, "RediSearch v9.0.0")

    def test_oss_genuinely_missing_alerts(self):
        # No "Open Source 7.7" at all -> the OSS lookup is emitted (release None).
        pr = PullRequest(repo="RediSearch", head_branch="x", base_branch="7.7", pr_number=8)
        lookups = handle_redisearch(pr, VERSIONS, vh(7, 7, 0))
        self.assertEqual(len(lookups), 2)
        self.assertIsNone(lookups[1].release)
        self.assertEqual(lookups[1].searched_name, "Open Source 7.7")


class TestConfigurableTemplates(unittest.TestCase):
    """The exact-name templates are overridable so a test can target the live
    ``RediSearch v6.6.0 dummy`` release without weakening prod semantics."""

    # Mirrors the real test release created in MOD for the e2e run.
    DUMMY_VERSIONS = [
        {"id": "100", "name": "RediSearch v6.6.0 dummy", "released": False},
    ]

    def test_default_template_misses_dummy_release(self):
        # Production default searches "RediSearch v6.6.0" -> not found -> alert.
        pr = PullRequest(repo="RediSearch", head_branch="x", base_branch="6.6", pr_number=10)
        lookups = handle_redisearch(pr, self.DUMMY_VERSIONS, vh(6, 6, 0))
        self.assertIsNone(lookups[0].release)
        self.assertEqual(lookups[0].searched_name, "RediSearch v6.6.0")

    def test_override_template_matches_dummy_release(self):
        templates = ReleaseTemplates(redisearch="RediSearch v{X}.{Y}.{Z} dummy")
        pr = PullRequest(repo="RediSearch", head_branch="x", base_branch="6.6", pr_number=10)
        lookups = handle_redisearch(pr, self.DUMMY_VERSIONS, vh(6, 6, 0), templates)
        self.assertEqual(lookups[0].release["id"], "100")  # found the dummy release
        # The Open Source 6.6.x lookup is still missing -> alert (exercised e2e).
        self.assertIsNone(lookups[1].release)


class TestRepoDispatch(unittest.TestCase):
    def test_redisearch(self):
        self.assertIs(handlers_for_repo("RediSearch"), handle_redisearch)

    def test_unruled_repos(self):
        # RediSearchEnterprise is handled in its own repo, not here.
        self.assertIsNone(handlers_for_repo("RediSearchEnterprise"))
        self.assertIsNone(handlers_for_repo("RedisJSON"))


if __name__ == "__main__":
    unittest.main()
