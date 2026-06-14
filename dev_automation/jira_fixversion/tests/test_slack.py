# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Offline unit tests for Slack alerting (design §9, §12)."""

import unittest
from unittest import mock

import requests

from dev_automation.jira_fixversion import slack_client
from dev_automation.jira_fixversion.slack_client import MissingReleaseAlert, SlackAlerter


def make_alert(searched="Open Source 6.6"):
    return MissingReleaseAlert(
        ticket_key="MOD-16249",
        ticket_url="https://redislabs.atlassian.net/browse/MOD-16249",
        repo="RediSearch",
        pr_number=10092,
        pr_url="https://github.com/RediSearch/RediSearch/pull/10092",
        base_branch="master",
        searched_name=searched,
        rule="RediSearch/version-branch -> unreleased Open Source X.Y",
    )


class FakeResp:
    def __init__(self, status_code=200, headers=None, text="ok"):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text


class TestWebhookAlert(unittest.TestCase):
    def test_posts_payload_variable(self):
        a = SlackAlerter(webhook_url="https://hooks.slack.com/wf/XYZ")
        self.assertFalse(a.stub)
        with mock.patch.object(slack_client.requests, "post",
                               return_value=FakeResp(200)) as post:
            a.alert(make_alert())
        post.assert_called_once()
        _, kwargs = post.call_args
        self.assertEqual(kwargs["json"].get("payload"), make_alert().to_text())
        self.assertIn("Missing Jira release", kwargs["json"]["payload"])

    def test_dedup_within_run(self):
        a = SlackAlerter(webhook_url="https://hooks.slack.com/wf/XYZ")
        with mock.patch.object(slack_client.requests, "post",
                               return_value=FakeResp(200)) as post:
            a.alert(make_alert())
            a.alert(make_alert())  # same dedup key -> suppressed
        self.assertEqual(post.call_count, 1)

    def test_non_200_does_not_raise(self):
        a = SlackAlerter(webhook_url="https://hooks.slack.com/wf/XYZ")
        with mock.patch.object(slack_client.requests, "post",
                               return_value=FakeResp(404, text="no_service")):
            a.alert(make_alert())  # must not raise

    def test_network_error_does_not_raise(self):
        a = SlackAlerter(webhook_url="https://hooks.slack.com/wf/XYZ", max_retries=2)
        with mock.patch.object(slack_client.requests, "post",
                               side_effect=requests.ConnectionError("boom")), \
             mock.patch.object(slack_client.time, "sleep"):
            a.alert(make_alert())  # retries then gives up, no raise

    def test_429_then_success_retries(self):
        a = SlackAlerter(webhook_url="https://hooks.slack.com/wf/XYZ", max_retries=3)
        responses = [FakeResp(429, headers={"Retry-After": "0"}), FakeResp(200)]
        with mock.patch.object(slack_client.requests, "post",
                               side_effect=responses) as post, \
             mock.patch.object(slack_client.time, "sleep"):
            a.alert(make_alert())
        self.assertEqual(post.call_count, 2)


class TestStubMode(unittest.TestCase):
    def test_stub_when_unconfigured(self):
        a = SlackAlerter()
        self.assertTrue(a.stub)
        with mock.patch.object(slack_client.requests, "post") as post:
            a.alert(make_alert())
        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
