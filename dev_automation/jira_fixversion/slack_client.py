# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Slack alerting for missing releases (design §9).

The only condition that triggers an alert is a release the agent expected to
find — based on an eligible ticket's linked PR — not existing in MOD.

Delivery is via a Slack Workflow Builder **incoming webhook** (configured as the
``SLACK_WEBHOOK_URL_FIX_VERSION`` secret). The workflow declares a single
``payload`` variable, so the request body is ``{"payload": "<all alert info>"}``.

If no webhook (and no bot token) is configured the alerter runs in *stub mode*:
alerts are logged but not sent. Sending is best-effort: per design §12 a Slack
failure is logged and the run continues — it never crashes the agent.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests

log = logging.getLogger("jira_fixversion.slack")


@dataclass
class MissingReleaseAlert:
    ticket_key: str
    ticket_url: str
    repo: str
    pr_number: int
    pr_url: str
    base_branch: str
    searched_name: str
    rule: str

    def dedup_key(self) -> str:
        # De-dup per ticket + missing release so reconciliation does not spam
        # the channel (design §9).
        return f"{self.ticket_key}|{self.searched_name}"

    def to_text(self) -> str:
        """Plain text for a Workflow Builder ``payload`` variable.

        URLs are bare on their own line (no surrounding punctuation) so Slack's
        auto-linker does not swallow a trailing ``)`` or ``,`` and break them.
        """
        return (
            ":warning: Missing Jira release — expected release not found in MOD\n"
            f"Ticket: {self.ticket_key}\n"
            f"{self.ticket_url}\n"
            f"Repo / PR: {self.repo} #{self.pr_number} (base {self.base_branch})\n"
            f"{self.pr_url}\n"
            f"Searched for: {self.searched_name}\n"
            f"Rule: {self.rule}"
        )


class SlackAlerter:
    def __init__(self, webhook_url: str = "", bot_token: str = "", channel_id: str = "",
                 *, timeout: int = 30, max_retries: int = 4):
        self.webhook_url = webhook_url
        self.bot_token = bot_token
        self.channel_id = channel_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.stub = not (webhook_url or (bot_token and channel_id))
        self._sent: set[str] = set()  # in-process de-dup for one run

    def alert(self, alert: MissingReleaseAlert) -> None:
        """Send one missing-release alert. Best-effort: never raises."""
        key = alert.dedup_key()
        if key in self._sent:
            log.debug("Suppressing duplicate alert: %s", key)
            return
        self._sent.add(key)

        if self.stub:
            log.warning("[SLACK STUB] would alert:\n%s", alert.to_text())
            return

        try:
            if self.webhook_url:
                self._post_webhook(alert)
            else:
                self._post_bot(alert)
        except Exception as exc:  # alerting must never crash the run (design §12)
            log.error("Slack alert failed (non-fatal): %s", exc)

    # -- delivery backends ---------------------------------------------------

    def _post_webhook(self, alert: MissingReleaseAlert) -> None:
        # Workflow Builder webhook: keys must match the workflow's variables.
        resp = self._post_with_retry(self.webhook_url, json={"payload": alert.to_text()})
        if resp is None or resp.status_code != 200:
            status = getattr(resp, "status_code", None)
            body = (getattr(resp, "text", "") or "")[:300]
            log.error("Slack webhook returned %s: %s", status, body)
        else:
            log.info("Slack alert sent for %s (%s)", alert.ticket_key, alert.searched_name)

    def _post_bot(self, alert: MissingReleaseAlert) -> None:
        resp = self._post_with_retry(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {self.bot_token}"},
            json={"channel": self.channel_id, "text": alert.to_text()},
        )
        body = resp.json() if (resp is not None and resp.content) else {}
        if not body.get("ok"):
            log.error("Slack chat.postMessage failed: %s",
                      body.get("error", getattr(resp, "status_code", None)))

    def _post_with_retry(self, url: str, **kwargs) -> Optional[requests.Response]:
        """POST with backoff on 429 / 5xx (design §12). Returns the last response
        or ``None`` if every attempt raised a network error."""
        resp: Optional[requests.Response] = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(url, timeout=self.timeout, **kwargs)
            except requests.RequestException as exc:
                log.warning("Slack POST error (attempt %d): %s", attempt + 1, exc)
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                log.warning("Slack rate limited; backing off %ss", retry_after)
                time.sleep(retry_after)
                continue
            if 500 <= resp.status_code < 600:
                log.warning("Slack %s; retrying", resp.status_code)
                time.sleep(2 ** attempt)
                continue
            return resp
        return resp
