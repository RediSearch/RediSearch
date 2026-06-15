# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The agent and its scheduled-reconciliation entrypoint (design §5.2).

The scheduled path walks every eligible MOD ticket, resolves its linked PRs via
the Jira dev panel, and for each PR in a ruled repo appends the matching
fixVersions (idempotently) or alerts on a missing release. The event path
(handle_event.py) drives the same per-PR logic. Every write is idempotent, so
the two overlap harmlessly.

    python -m dev_automation.jira_fixversion.reconcile [--dry-run] [--ticket MOD-123]
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from typing import Optional

import requests

from .config import RULED_REPOS, VERSION_FILE_PATH, Config
from .events import PrEvent, extract_issue_keys
from .github_client import GitHubClient, VersionFileNotFound
from .jira_client import JiraClient, PRLink, Ticket
from .matching import PullRequest, ReleaseTemplates, handlers_for_repo
from .slack_client import MissingReleaseAlert, SlackAlerter
from .version_parse import VersionParseError

log = logging.getLogger("jira_fixversion.reconcile")


class Agent:
    def __init__(self, cfg: Config, *, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.jira = JiraClient(cfg.jira_base_url, cfg.jira_email, cfg.jira_token)
        self.github = GitHubClient(cfg.github_token, cfg.github_org)
        self.slack = SlackAlerter(cfg.slack_webhook_url)
        self.templates = ReleaseTemplates(redisearch=cfg.redisearch_template)
        self.versions: list[dict] = []
        self.stats = {"tickets": 0, "prs": 0, "added": 0, "alerts": 0, "errors": 0}

    # -- entrypoints ---------------------------------------------------------

    def run_reconciliation(self) -> None:
        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        tickets = self.jira.search_eligible_tickets(self.cfg.project_key,
                                                    self.cfg.eligible_components)
        log.info("%d versions, %d eligible tickets", len(self.versions), len(tickets))
        for ticket in tickets:
            self.process_ticket(ticket)
        self._log_summary()

    def run_single(self, key: str) -> None:
        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        ticket = self._eligible(key)
        if ticket:
            self.process_ticket(ticket)
        self._log_summary()

    def run_event(self, event: PrEvent) -> None:
        """Event path (design §5.1): act on the ticket(s) a PR references.

        Tickets are resolved from the PR branch and title only (the fields
        GitHub-for-Jira uses) — not the body, so a "depends on MOD-123" mention
        doesn't trigger a write. Fork PRs are skipped (see _process_pr).
        """
        if event.repo not in RULED_REPOS:
            return
        if self.cfg.skip_fork_prs and event.is_fork:
            log.info("Skipping fork PR %s#%s; reconciliation will cover it",
                     event.repo, event.number)
            return
        keys = extract_issue_keys(event.head_ref, event.title)
        if not keys:
            log.info("No MOD keys on %s#%s; no-op", event.repo, event.number)
            return

        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        link = PRLink(repo=event.repo, number=event.number, state=event.state, url=event.html_url)
        for key in keys:
            ticket = self._eligible(key)
            if ticket:
                self.stats["tickets"] += 1
                self._process_pr_safe(ticket, link)
        self._log_summary()

    # -- core ----------------------------------------------------------------

    def process_ticket(self, ticket: Ticket) -> None:
        self.stats["tickets"] += 1
        for link in self.jira.get_linked_prs(ticket.issue_id):
            self._process_pr_safe(ticket, link)

    def _eligible(self, key: str) -> Optional[Ticket]:
        """Fetch a ticket, returning None (with a log line) if absent or ineligible."""
        try:
            ticket = self.jira.get_ticket(key)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                log.info("%s not found; skipping", key)
                return None
            raise
        if not (ticket.components & self.cfg.eligible_components):
            log.info("%s has no eligible component; skipping", key)
            return None
        return ticket

    def _process_pr_safe(self, ticket: Ticket, link: PRLink) -> None:
        if link.repo not in RULED_REPOS:
            return
        try:
            self._process_pr(ticket, link)
        except Exception as exc:  # one PR must not abort the run
            self.stats["errors"] += 1
            log.exception("Error on %s PR %s/#%s: %s", ticket.key, link.repo, link.number, exc)

    def _process_pr(self, ticket: Ticket, link: PRLink) -> None:
        self.stats["prs"] += 1
        meta = self.github.get_pull_request(link.repo, link.number)

        # A fork PR (linked via a MOD key in its branch/title) could write
        # fixVersions with our credentials; a PR closed without merging never
        # landed. Skip both.
        if self.cfg.skip_fork_prs and meta.is_fork:
            log.info("%s: skipping fork PR %s/#%s", ticket.key, link.repo, link.number)
            return
        if meta.state == "CLOSED":
            log.info("%s: PR %s/#%s closed without merge; skipping",
                     ticket.key, link.repo, link.number)
            return

        pr = PullRequest(repo=link.repo, head_branch=meta.head_branch,
                         base_branch=meta.base_branch, pr_number=link.number,
                         state=meta.state, head_sha=meta.head_sha, url=meta.url or link.url)

        # Read version.h at the head SHA (works even after a merged branch is gone).
        try:
            version_text = self.github.read_version_h(link.repo, meta.head_sha, VERSION_FILE_PATH)
        except VersionFileNotFound:
            self._alert(ticket, pr, f"<{VERSION_FILE_PATH} missing>", "version.h not found")
            return
        try:
            lookups = handlers_for_repo(link.repo)(pr, self.versions, version_text, self.templates)
        except VersionParseError as exc:
            self._alert(ticket, pr, f"<{VERSION_FILE_PATH} unparseable>", str(exc))
            return

        for lk in lookups:
            if lk.release is None:
                self._alert(ticket, pr, lk.searched_name, lk.rule)
            else:
                self._apply(ticket, lk.release)

    def _apply(self, ticket: Ticket, release: dict) -> None:
        if release["id"] in ticket.fix_version_ids:
            return  # idempotent
        if self.dry_run:
            log.info("[DRY-RUN] would add fixVersion %s to %s", release["name"], ticket.key)
            return
        self.jira.add_fix_version(ticket.key, release["id"])
        ticket.fix_version_ids.add(release["id"])
        self.stats["added"] += 1
        log.info("Added fixVersion %s to %s", release["name"], ticket.key)

    def _alert(self, ticket: Ticket, pr: PullRequest, searched_name: str, rule: str) -> None:
        self.stats["alerts"] += 1
        self.slack.alert(MissingReleaseAlert(
            ticket_key=ticket.key, ticket_url=f"{self.cfg.jira_base_url}/browse/{ticket.key}",
            repo=pr.repo, pr_number=pr.pr_number, pr_url=pr.url, base_branch=pr.base_branch,
            searched_name=searched_name, rule=rule,
        ))

    def _log_summary(self) -> None:
        s = self.stats
        log.info("Done: %(tickets)d tickets, %(prs)d PRs, %(added)d added, "
                 "%(alerts)d alerts, %(errors)d errors", s)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Jira Fix-Version reconciliation")
    parser.add_argument("--dry-run", action="store_true", help="log writes without applying")
    parser.add_argument("--ticket", help="process a single ticket key (e.g. MOD-123)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    agent = Agent(Config.from_env(), dry_run=args.dry_run)
    if args.ticket:
        if not re.fullmatch(r"MOD-\d+", args.ticket):  # may come from a workflow input
            parser.error(f"--ticket must be a MOD issue key (got {args.ticket!r})")
        agent.run_single(args.ticket)
    else:
        agent.run_reconciliation()
    return 0


if __name__ == "__main__":
    sys.exit(main())
