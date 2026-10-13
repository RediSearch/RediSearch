# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Jira eligibility and GitHub approval, independent of the request trigger."""

import argparse
import base64
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid


LABEL = "ai-fast-track"
JIRA_URL = "https://redislabs.atlassian.net"
MARKER = "<!-- redisearch-ai-fast-track-v1 -->"


class Rejected(Exception):
    """An eligibility failure or an unavailable source of authority."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward service credentials to a redirected endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise Rejected("API redirect refused")


class API:
    """Small JSON client with bounded requests and secret-free errors."""

    def __init__(self, base, authorization):
        self.base = base
        self.authorization = authorization

    def request(self, path, method="GET", body=None):
        request = urllib.request.Request(
            self.base + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={
                "Authorization": self.authorization,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "redisearch-fast-track",
            },
        )
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise Rejected(f"API request failed (HTTP {error.code})") from None
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise Rejected("API unavailable or returned invalid JSON") from None


@dataclass(frozen=True)
class Decision:
    eligible: bool
    reason: str
    repository: str
    pr_number: int
    requester: str
    ticket: str = ""
    head_sha: str = ""
    base_ref: str = ""
    title: str = ""


def ticket_from_title(title):
    """Only the canonical title prefix establishes the ticket association."""
    match = re.match(r"^\[(MOD-[1-9][0-9]*)\]\s+\S", title)
    keys = set(re.findall(r"\b[A-Z][A-Z0-9]*-[0-9]+\b", title))
    if not match or keys != {match[1]}:
        raise Rejected("Title must start with [MOD-123] and reference exactly one ticket")
    return match[1]


def pr_snapshot(pr):
    """Fields whose mutation invalidates an eligibility decision."""
    return (
        pr["head"]["sha"], pr["base"]["ref"], pr["base"]["repo"]["full_name"],
        pr["title"], pr["user"]["login"], pr["state"], pr["draft"],
    )


def validate(github, jira, repository, pr_number, requester):
    """Fail closed; a trigger cannot assert ticket eligibility or a head SHA."""
    decision = dict(repository=repository, pr_number=pr_number, requester=requester)
    try:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise Rejected("Invalid repository")
        if isinstance(pr_number, bool) or pr_number <= 0:
            raise Rejected("PR number must be positive")
        path = f"/repos/{repository}"
        pr = github.request(f"{path}/pulls/{pr_number}")
        if pr["state"] != "open" or pr["draft"]:
            raise Rejected("PR must be open and ready for review")
        if pr["base"]["repo"]["full_name"] != repository or pr["base"]["ref"] != "master":
            raise Rejected("Only PRs targeting this repository's master are eligible")
        if requester.casefold() != pr["user"]["login"].casefold():
            raise Rejected("Only the PR author may request fast-track approval")
        permission = github.request(
            f"{path}/collaborators/{urllib.parse.quote(requester, safe='')}/permission"
        )
        if permission["permission"] not in {"write", "maintain", "admin"}:
            raise Rejected("Requester must have repository write access")
        ticket = ticket_from_title(pr["title"])
        decision.update(ticket=ticket, head_sha=pr["head"]["sha"],
                        base_ref=pr["base"]["ref"], title=pr["title"])
        if not re.fullmatch(r"[0-9a-f]{40}", decision["head_sha"]):
            raise Rejected("Invalid PR head SHA")
        issue = jira.request(f"/rest/api/3/issue/{ticket}?fields=labels")
        if issue["key"] != ticket:
            raise Rejected("Jira resolved the title to a different ticket")
        labels = issue["fields"]["labels"]
        if not isinstance(labels, list) or LABEL not in labels:
            raise Rejected(f"Jira ticket does not carry {LABEL}")
        if pr_snapshot(github.request(f"{path}/pulls/{pr_number}")) != pr_snapshot(pr):
            raise Rejected("PR changed during validation; request approval again")
        return Decision(True, f"Jira ticket carries {LABEL}", **decision)
    except Rejected as error:
        return Decision(False, str(error), **decision)
    except (KeyError, TypeError, ValueError):
        return Decision(False, "Malformed API response or input", **decision)


def reviews_for_pr(github, path):
    """Iterate every review, including entries outside the first API page."""
    page = 1
    while True:
        reviews = github.request(f"{path}/reviews?per_page=100&page={page}")
        yield from reviews
        if len(reviews) < 100:
            return
        page += 1


def reconcile_submission(github, path, bot_login, head_sha, submission_marker):
    """Best-effort revocation when the POST response cannot establish its outcome."""
    found = False
    try:
        for review in reviews_for_pr(github, path):
            if (review["user"]["login"] == bot_login
                    and review["commit_id"] == head_sha
                    and submission_marker in (review["body"] or "")):
                found = True
                if review["state"] == "APPROVED":
                    github.request(f"{path}/reviews/{review['id']}/dismissals", "PUT", {
                        "message": "Fast-track submission outcome was ambiguous; revoking this attempt."
                    })
    except Exception:
        raise Rejected("Approval submission and reconciliation failed; inspect PR reviews manually") from None
    if not found:
        raise Rejected("Approval submission uncertain; no matching review visible yet; inspect PR reviews manually")
    raise Rejected("Approval submission failed; matching review reconciled; request approval again")


def approve(github, jira, decision, bot_login, run_url):
    """Revalidate authority and approve only the recorded revision."""
    if not decision.eligible:
        raise Rejected(decision.reason)
    if not re.fullmatch(r"[A-Za-z0-9_-]+\[bot\]", bot_login):
        raise Rejected("Expected a dedicated GitHub App bot identity")
    if bot_login.casefold() == decision.requester.casefold():
        raise Rejected("Approval App must not be the PR author")
    path = f"/repos/{decision.repository}/pulls/{decision.pr_number}"
    latest = None
    for review in reviews_for_pr(github, path):
        if review["user"]["login"] == bot_login and review["state"] != "COMMENTED":
            if latest is None or review["id"] > latest["id"]:
                latest = review
    # Pagination may take time; refresh external authority after it completes.
    fresh = validate(github, jira, decision.repository, decision.pr_number, decision.requester)
    if not fresh.eligible:
        raise Rejected(fresh.reason)
    if fresh != decision:
        raise Rejected("Validated PR changed; request approval again")
    if latest:
        latest = github.request(f"{path}/reviews/{latest['id']}")
    if (latest and latest["state"] == "APPROVED"
            and latest["commit_id"] == decision.head_sha
            and MARKER in (latest["body"] or "")
            and f"Ticket: {decision.ticket}\n" in latest["body"]):
        return "Already approved for this ticket and head"

    pr = github.request(path)
    expected = (decision.head_sha, decision.base_ref, decision.repository,
                decision.title, decision.requester.casefold(), "open", False)
    snapshot = pr_snapshot(pr)
    snapshot = snapshot[:4] + (snapshot[4].casefold(),) + snapshot[5:]
    if snapshot != expected:
        raise Rejected("PR changed before approval; request approval again")
    submission_marker = f"<!-- fast-track-submission:{uuid.uuid4()} -->"
    payload = {
        "event": "APPROVE",
        "commit_id": decision.head_sha,
        "body": (
            f"{MARKER}\n{submission_marker}\nPolicy approval: Jira `{LABEL}` (not a code review).\n\n"
            f"Ticket: {decision.ticket}\n"
            f"{JIRA_URL}/browse/{decision.ticket}\n\n"
            f"Requested by: @{decision.requester}\n"
            f"Head: `{decision.head_sha}`\n"
            f"Workflow: {run_url}\n\n"
            "CI and merge-queue requirements remain in force."
        ),
    }
    try:
        review = github.request(path + "/reviews", "POST", payload)
        if not isinstance(review.get("id"), int):
            raise Rejected("Malformed approval response")
    except Exception:
        reconcile_submission(github, path, bot_login, decision.head_sha, submission_marker)
    # The reviews API has no compare-and-swap. Compensate if the PR changes
    # during submission; branch rules also require approval of the latest push.
    try:
        if pr_snapshot(github.request(path)) != pr_snapshot(pr):
            raise Rejected("PR changed during approval; request approval again")
    except Exception:
        github.request(f"{path}/reviews/{review['id']}/dismissals", "PUT", {
            "message": "Fast-track approval invalidated: PR changed or could not be rechecked."
        })
        raise
    return "Approved validated head"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["validate", "approve"])
    parser.add_argument("--decision", type=Path, required=True)
    args = parser.parse_args()
    github = API("https://api.github.com", f"Bearer {os.environ['GH_TOKEN']}")
    credentials = base64.b64encode(
        f"{os.environ['JIRA_EMAIL']}:{os.environ['JIRA_API_TOKEN']}".encode()
    ).decode()
    jira = API(JIRA_URL, f"Basic {credentials}")
    try:
        if args.phase == "validate":
            decision = validate(github, jira, os.environ["GITHUB_REPOSITORY"],
                                int(os.environ["PR_NUMBER"]), os.environ["REQUESTER"])
            args.decision.write_text(json.dumps(asdict(decision)), encoding="utf-8")
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                output.write(f"eligible={str(decision.eligible).lower()}\n")
            message = decision.reason
        else:
            decision = Decision(**json.loads(args.decision.read_text(encoding="utf-8")))
            message = approve(github, jira, decision, os.environ["APP_BOT_LOGIN"],
                              os.environ["RUN_URL"])
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
            summary.write(f"Fast-track: {message}\n")
        print(message)
        return 0 if decision.eligible else 1
    except (Rejected, KeyError, TypeError, ValueError) as error:
        # API error bodies and malformed remote data must not enter logs.
        print(str(error) if isinstance(error, Rejected) else "Invalid configuration or API response")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
