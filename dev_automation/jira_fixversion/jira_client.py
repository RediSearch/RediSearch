# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Thin Jira Cloud REST client (design §6, §8.4)."""

from __future__ import annotations

import logging
import re
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
    repo: str
    number: int
    state: str = ""
    url: str = ""
    owner: str = ""  # repo owner from the dev-panel URL (may differ from our org for forks)


class JiraClient:
    def __init__(self, base_url: str, email: str, token: str, *, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (email, token)
        self.session.headers.update({"Accept": "application/json"})

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        for attempt in range(5):  # retry on rate limit (design §12)
            resp = self.session.request(method, f"{self.base_url}{path}", timeout=self.timeout, **kwargs)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 2 ** attempt)))
                continue
            resp.raise_for_status()
            return resp
        resp.raise_for_status()
        return resp

    def list_project_versions(self, project_key: str) -> list[dict]:
        """All releases: [{id, name, released, archived}, ...]."""
        return self._request("GET", f"/rest/api/3/project/{project_key}/versions").json()

    def search_eligible_tickets(self, project_key: str, components: set) -> list[Ticket]:
        """MOD tickets whose components intersect the eligible set (token-paginated)."""
        comp_list = ", ".join(f'"{c}"' for c in sorted(components))
        jql = f'project = "{project_key}" AND component in ({comp_list})'
        tickets, next_token = [], None
        while True:
            payload = {"jql": jql, "fields": ["components", "fixVersions"], "maxResults": 100}
            if next_token:
                payload["nextPageToken"] = next_token
            data = self._request("POST", "/rest/api/3/search/jql", json=payload).json()
            tickets.extend(_ticket(i) for i in data.get("issues", []))
            next_token = data.get("nextPageToken")
            if data.get("isLast", True) or not next_token:
                return tickets

    def get_ticket(self, key: str) -> Ticket:
        resp = self._request("GET", f"/rest/api/3/issue/{key}",
                             params={"fields": "components,fixVersions"})
        return _ticket(resp.json())

    def get_linked_prs(self, issue_id: str) -> list[PRLink]:
        """GitHub PRs linked to a ticket via the Jira dev panel."""
        data = self._request("GET", "/rest/dev-status/latest/issue/detail",
                             params={"issueId": issue_id, "applicationType": "GitHub",
                                     "dataType": "pullrequest"}).json()
        links = []
        for detail in data.get("detail", []):
            for pr in detail.get("pullRequests", []):
                # Prefer the stable `url` (keeps the owner, which matters for forks);
                # `id` is a provider entity id and repositoryName may be absent.
                url = pr.get("url", "")
                owner, repo, number = _parse_pr_url(url)
                repo = repo or _repo_name(pr.get("repositoryName") or pr.get("repositoryUrl", ""))
                number = number or _pr_number(pr.get("id", ""))
                links.append(PRLink(repo=repo, number=number, owner=owner,
                                    state=(pr.get("status") or "").upper(), url=url))
        return links

    def add_fix_version(self, issue_key: str, version_id: str) -> None:
        self._request("PUT", f"/rest/api/3/issue/{issue_key}",
                      json={"update": {"fixVersions": [{"add": {"id": version_id}}]}})

    def remove_fix_version(self, issue_key: str, version_id: str) -> None:
        self._request("PUT", f"/rest/api/3/issue/{issue_key}",
                      json={"update": {"fixVersions": [{"remove": {"id": version_id}}]}})


def _ticket(issue: dict) -> Ticket:
    f = issue.get("fields", {})
    return Ticket(key=issue["key"], issue_id=issue["id"],
                  components={c["name"] for c in (f.get("components") or [])},
                  fix_version_ids={v["id"] for v in (f.get("fixVersions") or [])})


_PR_URL_RE = re.compile(r"github\.com/([^/]+)/([^/]+)/pull/(\d+)")


def _parse_pr_url(url: str) -> tuple[str, str, int]:
    """Extract (owner, repo, number) from a GitHub PR URL; ('', '', 0) if no match."""
    m = _PR_URL_RE.search(url or "")
    return (m.group(1), m.group(2), int(m.group(3))) if m else ("", "", 0)


def _repo_name(repo_field: str) -> str:
    return repo_field.rstrip("/").rsplit("/", 1)[-1] if repo_field else ""


def _pr_number(pr_id: str) -> int:
    digits = "".join(ch for ch in str(pr_id) if ch.isdigit())
    return int(digits) if digits else 0
