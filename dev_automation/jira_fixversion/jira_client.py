# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Thin Jira Cloud REST client used by the agent (design §6, §8.4)."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

log = logging.getLogger("jira_fixversion.jira")


@dataclass
class Ticket:
    key: str
    issue_id: str  # numeric id, needed for the dev-status endpoint
    components: set = field(default_factory=set)
    fix_version_ids: set = field(default_factory=set)


@dataclass
class PRLink:
    """A PR association discovered from the Jira development panel."""

    repo: str
    number: int
    state: str = ""
    url: str = ""


class JiraClient:
    def __init__(self, base_url: str, email: str, token: str, *, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (email, token)
        self.session.headers.update({"Accept": "application/json"})

    # -- low-level with simple rate-limit backoff (design §12) ---------------

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        for attempt in range(5):
            resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                log.warning("Jira 429; backing off %ss", retry_after)
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp
        resp.raise_for_status()
        return resp

    # -- project versions (design §8.4) --------------------------------------

    def list_project_versions(self, project_key: str) -> list[dict]:
        """All releases for the project: ``[{id, name, released, archived}, ...]``."""
        resp = self._request("GET", f"/rest/api/3/project/{project_key}/versions")
        return resp.json()

    # -- eligible-ticket discovery (design §7) -------------------------------

    def search_eligible_tickets(self, project_key: str, components: set) -> list[Ticket]:
        """Find MOD tickets whose components intersect the eligible set.

        Uses the enhanced ``/search/jql`` endpoint with token pagination.
        """
        comp_list = ", ".join(f'"{c}"' for c in sorted(components))
        jql = f'project = "{project_key}" AND component in ({comp_list})'
        tickets: list[Ticket] = []
        next_token: Optional[str] = None
        while True:
            payload = {
                "jql": jql,
                "fields": ["components", "fixVersions"],
                "maxResults": 100,
            }
            if next_token:
                payload["nextPageToken"] = next_token
            resp = self._request("POST", "/rest/api/3/search/jql", json=payload)
            data = resp.json()
            for issue in data.get("issues", []):
                f = issue.get("fields", {})
                tickets.append(
                    Ticket(
                        key=issue["key"],
                        issue_id=issue["id"],
                        components={c["name"] for c in (f.get("components") or [])},
                        fix_version_ids={v["id"] for v in (f.get("fixVersions") or [])},
                    )
                )
            next_token = data.get("nextPageToken")
            if data.get("isLast", True) or not next_token:
                break
        return tickets

    def get_ticket(self, key: str) -> Ticket:
        """Fetch a single ticket's components and current fixVersions."""
        resp = self._request(
            "GET", f"/rest/api/3/issue/{key}",
            params={"fields": "components,fixVersions"},
        )
        issue = resp.json()
        f = issue.get("fields", {})
        return Ticket(
            key=issue["key"],
            issue_id=issue["id"],
            components={c["name"] for c in (f.get("components") or [])},
            fix_version_ids={v["id"] for v in (f.get("fixVersions") or [])},
        )

    # -- PR <-> ticket links via the development panel (design §6) -----------

    def get_linked_prs(self, issue_id: str) -> list[PRLink]:
        """Read GitHub PRs linked to a ticket from the Jira development panel."""
        resp = self._request(
            "GET", "/rest/dev-status/latest/issue/detail",
            params={
                "issueId": issue_id,
                "applicationType": "GitHub",
                "dataType": "pullrequest",
            },
        )
        data = resp.json()
        links: list[PRLink] = []
        for detail in data.get("detail", []):
            for pr in detail.get("pullRequests", []):
                links.append(
                    PRLink(
                        repo=_repo_name(pr.get("repositoryName") or pr.get("repositoryUrl", "")),
                        number=_pr_number(pr.get("id", "")),
                        state=(pr.get("status") or "").upper(),
                        url=pr.get("url", ""),
                    )
                )
        return links

    # -- write path (design §8.4): idempotent fixVersions append -------------

    def add_fix_version(self, issue_key: str, version_id: str) -> None:
        body = {"update": {"fixVersions": [{"add": {"id": version_id}}]}}
        self._request("PUT", f"/rest/api/3/issue/{issue_key}", json=body)


def _repo_name(repo_field: str) -> str:
    """Normalize a dev-status repository field to a bare repo name."""
    if not repo_field:
        return ""
    return repo_field.rstrip("/").rsplit("/", 1)[-1]


def _pr_number(pr_id: str) -> int:
    """dev-status reports PR id like ``#123``; extract the integer."""
    digits = "".join(ch for ch in str(pr_id) if ch.isdigit())
    return int(digits) if digits else 0
