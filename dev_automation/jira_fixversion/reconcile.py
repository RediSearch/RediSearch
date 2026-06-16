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
        self.stats = {"tickets": 0, "prs": 0, "added": 0, "removed": 0, "alerts": 0, "errors": 0}

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

        On an ``edited`` event, MOD keys that were dropped from the title since the
        previous version are rolled back on their (now-unreferenced) tickets.
        """
        if event.repo not in RULED_REPOS:
            return
        if self.cfg.skip_fork_prs and event.is_fork:
            log.info("Skipping fork PR %s#%s; reconciliation will cover it",
                     event.repo, event.number)
            return
        current = extract_issue_keys(event.head_ref, event.title)
        removed = []
        if event.action == "edited" and event.prev_title:
            removed = [k for k in extract_issue_keys(event.prev_title) if k not in current]
        if not current and not removed:
            log.info("No MOD keys on %s#%s; no-op", event.repo, event.number)
            return

        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        link = PRLink(repo=event.repo, number=event.number, state=event.state, url=event.html_url)
        for key in current:
            ticket = self._eligible(key)
            if ticket:
                self.stats["tickets"] += 1
                self._process_pr_safe(ticket, link)
        for key in removed:  # key no longer referenced -> roll back what this PR added
            ticket = self._eligible(key)
            if ticket:
                self.stats["tickets"] += 1
                log.info("%s no longer referenced by %s#%s; rolling back", key,
                         event.repo, event.number)
                self._process_pr_safe(ticket, link, force_remove=True)
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

    def _process_pr_safe(self, ticket: Ticket, link: PRLink, *, force_remove: bool = False) -> None:
        if link.repo not in RULED_REPOS:
            return
        # A dev-panel link whose owner is not our org points at an external repo;
        # acting on it would use the bot's credentials on an untrusted PR. Skip it.
        if link.owner and link.owner != self.cfg.github_org:
            log.info("%s: skipping PR %s/%s#%s outside org %s",
                     ticket.key, link.owner, link.repo, link.number, self.cfg.github_org)
            return
        try:
            self._process_pr(ticket, link, force_remove=force_remove)
        except Exception as exc:  # one PR must not abort the run
            self.stats["errors"] += 1
            log.exception("Error on %s PR %s/#%s: %s", ticket.key, link.repo, link.number, exc)

    def _lookups_for(self, link: PRLink):
        """(meta, lookups) for a linked PR, or (meta, None) if version.h is
        missing/unparsable. Raises on transport errors."""
        meta = self.github.get_pull_request(link.repo, link.number, owner=link.owner)
        try:
            text = self.github.read_version_h(link.repo, meta.head_sha,
                                              VERSION_FILE_PATH, owner=link.owner)
        except VersionFileNotFound:
            return meta, None
        pr = PullRequest(repo=link.repo, head_branch=meta.head_branch,
                         base_branch=meta.base_branch, pr_number=link.number,
                         state=meta.state, head_sha=meta.head_sha, url=meta.url or link.url)
        try:
            return meta, handlers_for_repo(link.repo)(pr, self.versions, text, self.templates)
        except VersionParseError:
            return meta, None

    def _process_pr(self, ticket: Ticket, link: PRLink, *, force_remove: bool = False) -> None:
        self.stats["prs"] += 1
        meta = self.github.get_pull_request(link.repo, link.number, owner=link.owner)

        # A fork PR (linked via a MOD key in its branch/title, or via a fork dev-panel
        # URL) could write fixVersions with our credentials — never touch it.
        if self.cfg.skip_fork_prs and meta.is_fork:
            log.info("%s: skipping fork PR %s/#%s", ticket.key, link.repo, link.number)
            return

        # Roll back when the PR closed without merging, or when the ticket is no
        # longer referenced (force_remove, e.g. a MOD key edited out of the title).
        remove_mode = force_remove or meta.state == "CLOSED"
        pr = PullRequest(repo=link.repo, head_branch=meta.head_branch,
                         base_branch=meta.base_branch, pr_number=link.number,
                         state=meta.state, head_sha=meta.head_sha, url=meta.url or link.url)

        # Read version.h at the head SHA (works even after a merged branch is gone).
        try:
            version_text = self.github.read_version_h(link.repo, meta.head_sha,
                                                      VERSION_FILE_PATH, owner=link.owner)
        except VersionFileNotFound:
            if not remove_mode:
                self._alert(ticket, pr, f"<{VERSION_FILE_PATH} missing>", "version.h not found")
            return
        try:
            lookups = handlers_for_repo(link.repo)(pr, self.versions, version_text, self.templates)
        except VersionParseError as exc:
            if not remove_mode:
                self._alert(ticket, pr, f"<{VERSION_FILE_PATH} unparsable>", str(exc))
            return

        # In remove mode, only drop releases that no other live (open/merged) linked
        # PR still maps to — otherwise an abandoned PR could delete a release another
        # PR justifies.
        still_needed = self._releases_needed_by_others(ticket, link) if remove_mode else set()
        for lk in lookups:
            if lk.release is None:
                if not remove_mode:  # nothing to roll back for a missing release
                    self._alert(ticket, pr, lk.searched_name, lk.rule)
            elif remove_mode:
                if lk.release["id"] in still_needed:
                    log.info("%s: keeping %s (still mapped by another linked PR)",
                             ticket.key, lk.release["name"])
                else:
                    self._remove(ticket, lk.release)
            else:
                self._apply(ticket, lk.release)

    def _releases_needed_by_others(self, ticket: Ticket, closing: PRLink) -> set:
        """Release ids still mapped by the ticket's other live (open/merged) PRs."""
        needed: set = set()
        for link in self.jira.get_linked_prs(ticket.issue_id):
            if link.repo not in RULED_REPOS:
                continue
            if link.owner and link.owner != self.cfg.github_org:
                continue  # external repo — not a trusted justification
            if link.number == closing.number and (link.owner or "") == (closing.owner or ""):
                continue  # the PR being closed
            try:
                meta, lookups = self._lookups_for(link)
            except Exception:
                continue  # ignore siblings we can't read; conservative on the remove side
            if meta.state == "CLOSED" or (self.cfg.skip_fork_prs and meta.is_fork) or not lookups:
                continue
            needed |= {lk.release["id"] for lk in lookups if lk.release}
        return needed

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

    def _remove(self, ticket: Ticket, release: dict) -> None:
        if release["id"] not in ticket.fix_version_ids:
            return  # nothing to roll back
        if self.dry_run:
            log.info("[DRY-RUN] would remove fixVersion %s from %s", release["name"], ticket.key)
            return
        self.jira.remove_fix_version(ticket.key, release["id"])
        ticket.fix_version_ids.discard(release["id"])
        self.stats["removed"] += 1
        log.info("Removed fixVersion %s from %s (PR closed unmerged)", release["name"], ticket.key)

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
                 "%(removed)d removed, %(alerts)d alerts, %(errors)d errors", s)


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
