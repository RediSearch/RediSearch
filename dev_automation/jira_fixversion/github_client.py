# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Thin GitHub REST client: read PR metadata and src/version.h (design §8.1)."""

from __future__ import annotations

import base64
import logging
import time
from dataclasses import dataclass

import requests

log = logging.getLogger("jira_fixversion.github")


class VersionFileNotFound(Exception):
    """src/version.h does not exist at the requested ref."""


def is_fork(head_repo: str, base_repo: str) -> bool:
    """Whether a PR comes from a fork. Fails closed: a PR is treated as a fork
    unless its head repo is present and equals the base repo. GitHub returns a
    null head repo for forks whose source was deleted/made inaccessible."""
    return not (head_repo and head_repo == base_repo)


@dataclass
class PRMeta:
    base_branch: str
    head_branch: str
    head_sha: str
    state: str
    url: str
    is_fork: bool = False  # head repo differs from base repo


class GitHubClient:
    def __init__(self, token: str, org: str, *, timeout: int = 30):
        self.org = org
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.github+json"})
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"https://api.github.com{path}"
        for attempt in range(5):
            resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
            wait = self._rate_limit_wait(resp, attempt)  # design §12
            if wait is None:
                return resp
            log.warning("GitHub rate limited; sleeping %ss", wait)
            time.sleep(wait)
        return resp

    @staticmethod
    def _rate_limit_wait(resp: requests.Response, attempt: int):
        """Seconds to back off for a rate-limited 403/429, or None to proceed.

        Covers both the primary limit (`X-RateLimit-Remaining: 0` + reset time)
        and secondary limits (a `Retry-After` header with remaining still > 0).
        """
        if resp.status_code not in (403, 429):
            return None
        retry_after = resp.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return min(int(retry_after), 60)
        if resp.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(resp.headers.get("X-RateLimit-Reset", "0"))
            return min(max(1, reset - int(time.time())) if reset else 2 ** attempt, 60)
        return None

    def get_pull_request(self, repo: str, number: int, owner: str = "") -> PRMeta:
        """Authoritative PR base branch + head SHA (robust for merged PRs).

        `owner` defaults to the configured org; pass the dev-panel URL's owner so a
        fork PR is fetched from its real repo (the fork guard then skips it) instead
        of querying the same number under our org.
        """
        resp = self._request("GET", f"/repos/{owner or self.org}/{repo}/pulls/{number}")
        resp.raise_for_status()
        pr = resp.json()
        head_repo = ((pr.get("head") or {}).get("repo") or {}).get("full_name", "")
        base_repo = ((pr.get("base") or {}).get("repo") or {}).get("full_name", "")
        return PRMeta(
            base_branch=pr["base"]["ref"],
            head_branch=pr["head"]["ref"],
            head_sha=pr["head"]["sha"],
            state=("MERGED" if pr.get("merged_at") else pr.get("state", "").upper()),
            url=pr.get("html_url", ""),
            is_fork=is_fork(head_repo, base_repo),
        )

    def read_version_h(self, repo: str, ref: str, path: str = "src/version.h",
                       owner: str = "") -> str:
        """Fetch a file at ``ref`` via the Contents API and return its text."""
        resp = self._request(
            "GET", f"/repos/{owner or self.org}/{repo}/contents/{path}",
            params={"ref": ref},
        )
        if resp.status_code == 404:
            raise VersionFileNotFound(f"{path} not found in {self.org}/{repo}@{ref}")
        resp.raise_for_status()
        data = resp.json()
        content = data.get("content", "")
        encoding = data.get("encoding", "base64")
        if encoding == "base64":
            return base64.b64decode(content).decode("utf-8", errors="replace")
        return content
