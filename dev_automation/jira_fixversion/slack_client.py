# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Slack alerting for missing releases (design §9).

Posts to a Slack Workflow Builder incoming webhook whose body is
``{"payload": <text>}``. With no webhook configured it logs instead (stub mode).
Delivery is best-effort: failures are logged, never raised.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

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
        return f"{self.ticket_key}|{self.searched_name}"

    def to_text(self) -> str:
        # URLs go on their own line so Slack auto-links them without swallowing
        # trailing punctuation.
        return (
            ":warning: Missing Jira release — expected release not found in MOD\n"
            f"Ticket: {self.ticket_key}\n{self.ticket_url}\n"
            f"Repo / PR: {self.repo} #{self.pr_number} (base {self.base_branch})\n{self.pr_url}\n"
            f"Searched for: {self.searched_name}\n"
            f"Rule: {self.rule}"
        )


class SlackAlerter:
    def __init__(self, webhook_url: str = "", *, timeout: int = 30, max_retries: int = 4):
        self.webhook_url = webhook_url
        self.timeout = timeout
        self.max_retries = max_retries
        self._sent: set[str] = set()  # de-dup within a run

    def alert(self, alert: MissingReleaseAlert) -> None:
        key = alert.dedup_key()
        if key in self._sent:
            return
        self._sent.add(key)

        if not self.webhook_url:
            log.warning("[SLACK STUB] would alert:\n%s", alert.to_text())
            return

        try:
            resp = self._post({"payload": alert.to_text()})
            if resp is None or resp.status_code != 200:
                log.error("Slack webhook returned %s: %s",
                          getattr(resp, "status_code", None), getattr(resp, "text", "")[:300])
            else:
                log.info("Slack alert sent for %s (%s)", alert.ticket_key, alert.searched_name)
        except Exception as exc:  # alerting must never crash the run (design §12)
            log.error("Slack alert failed (non-fatal): %s", exc)

    def _post(self, body: dict):
        """POST with backoff on 429/5xx; returns the last response or None."""
        resp = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(self.webhook_url, json=body, timeout=self.timeout)
            except requests.RequestException as exc:
                log.warning("Slack POST error (attempt %d): %s", attempt + 1, exc)
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2 ** attempt)))
                continue
            if 500 <= resp.status_code < 600:
                time.sleep(2 ** attempt)
                continue
            return resp
        return resp
