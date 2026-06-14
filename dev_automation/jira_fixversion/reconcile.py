# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Scheduled reconciliation entrypoint for the Fix-Version agent (design §5.2).

Queries every eligible MOD ticket, re-resolves the PRs linked to it via the
Jira development panel, runs the matching engine for each linked PR in a ruled
repository, and either appends the matching fixVersions (idempotently) or
raises a Slack alert for a missing release.

Both the event path and this scheduled path feed the same per-ticket logic, so
the two overlap harmlessly — every write is idempotent (design §12).

Usage::

    python -m dev_automation.jira_fixversion.reconcile [--dry-run] [--ticket MOD-123]
"""

from __future__ import annotations

import argparse
import logging
import re
import sys

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
        self.slack = SlackAlerter(cfg.slack_webhook_url, cfg.slack_bot_token, cfg.slack_channel_id)
        self.templates = ReleaseTemplates(
            redisearch=cfg.redisearch_template,
            enterprise=cfg.enterprise_template,
        )
        self.versions: list[dict] = []
        self.stats = {"tickets": 0, "prs": 0, "added": 0, "alerts": 0, "errors": 0}

    def run_reconciliation(self) -> None:
        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        log.info("Loaded %d project versions for %s", len(self.versions), self.cfg.project_key)
        tickets = self.jira.search_eligible_tickets(
            self.cfg.project_key, self.cfg.eligible_components
        )
        log.info("Found %d eligible tickets", len(tickets))
        for ticket in tickets:
            self.process_ticket(ticket)
        self._log_summary()

    def run_single(self, key: str) -> None:
        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        ticket = self.jira.get_ticket(key)
        if not (ticket.components & self.cfg.eligible_components):
            log.info("%s has no eligible component; skipping", key)
            return
        self.process_ticket(ticket)
        self._log_summary()

    def run_event(self, event: PrEvent) -> None:
        """Event-driven path (design §5.1): act on a single PR's linked tickets.

        Entry point is the MOD issue key(s) the PR references in the **branch name
        and PR title** — the fields the GitHub-for-Jira app uses to associate a PR
        with a ticket. The PR *body* is intentionally not scanned: a free-form
        mention like "depends on MOD-123" must not cause a write to MOD-123.

        For each eligible ticket we run the same per-PR matching as the scheduled
        path, against authoritative GitHub data. Unlinked / ineligible / out-of-repo
        PRs are silent no-ops.
        """
        if event.repo not in RULED_REPOS:
            log.info("PR repo %r has no rule; no-op", event.repo)
            return
        if self.cfg.skip_fork_prs and event.is_fork:
            log.info("Skipping fork PR %s#%s in event path; the scheduled "
                     "reconciliation will cover it", event.repo, event.number)
            return

        keys = extract_issue_keys(event.head_ref, event.title)
        if not keys:
            log.info("No MOD issue keys referenced by %s#%s; no-op", event.repo, event.number)
            return

        self.versions = self.jira.list_project_versions(self.cfg.project_key)
        link = PRLink(repo=event.repo, number=event.number, state=event.state, url=event.html_url)
        for key in keys:
            try:
                ticket = self.jira.get_ticket(key)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    log.info("%s referenced by PR but not found; skipping", key)
                    continue
                raise
            if not (ticket.components & self.cfg.eligible_components):
                log.info("%s has no eligible component; skipping", key)
                continue
            self.stats["tickets"] += 1
            try:
                self._process_pr(ticket, link)
            except Exception as exc:
                self.stats["errors"] += 1
                log.exception("Error processing %s PR %s/#%s: %s",
                              ticket.key, link.repo, link.number, exc)
        self._log_summary()

    def process_ticket(self, ticket: Ticket) -> None:
        self.stats["tickets"] += 1
        links = self.jira.get_linked_prs(ticket.issue_id)
        for link in links:
            if link.repo not in RULED_REPOS:
                continue  # PR in a repo without a rule -> silent no-op (design §2)
            try:
                self._process_pr(ticket, link)
            except Exception as exc:  # one PR failing must not abort the run
                self.stats["errors"] += 1
                log.exception("Error processing %s PR %s/#%s: %s",
                              ticket.key, link.repo, link.number, exc)

    def _process_pr(self, ticket: Ticket, link: PRLink) -> None:
        self.stats["prs"] += 1
        meta = self.github.get_pull_request(link.repo, link.number)
        handler = handlers_for_repo(link.repo)
        if handler is None:
            return

        # Fork guard for BOTH paths: GitHub-for-Jira links a PR from an issue key
        # in its branch/title, so a fork PR can reach the scheduled path too. Skip
        # it unless forks are explicitly allowed, so an external contributor cannot
        # write fixVersions with the bot's credentials.
        if self.cfg.skip_fork_prs and meta.is_fork:
            log.info("%s: skipping fork PR %s/#%s", ticket.key, link.repo, link.number)
            return

        # A PR closed without merging (declined/abandoned) never landed, so it must
        # not mark the ticket as fixed. Open and merged PRs are still processed.
        if meta.state == "CLOSED":
            log.info("%s: PR %s/#%s closed without merge; skipping",
                     ticket.key, link.repo, link.number)
            return

        pr = PullRequest(
            repo=link.repo,
            head_branch=meta.head_branch,
            base_branch=meta.base_branch,
            pr_number=link.number,
            state=meta.state,
            head_sha=meta.head_sha,
            url=meta.url or link.url,
        )

        # Read src/version.h at the PR head SHA (authoritative even after the
        # head branch is deleted on merge). A missing/unparseable file alerts
        # rather than crashing the run (design §12).
        try:
            version_text = self.github.read_version_h(link.repo, meta.head_sha, VERSION_FILE_PATH)
        except VersionFileNotFound:
            log.warning("%s: %s missing in %s@%s", ticket.key, VERSION_FILE_PATH,
                        link.repo, meta.head_sha[:8])
            self._alert(ticket, pr, f"<{VERSION_FILE_PATH} missing>",
                        "version.h not found at PR head")
            return

        try:
            lookups = handler(pr, self.versions, version_text, self.templates)
        except VersionParseError as exc:
            log.warning("%s: unparseable version.h: %s", ticket.key, exc)
            self._alert(ticket, pr, f"<{VERSION_FILE_PATH} unparseable>", str(exc))
            return

        for lk in lookups:
            if lk.release is None:
                self._alert(ticket, pr, lk.searched_name, lk.rule)
            else:
                self._apply(ticket, lk.release)

    def _apply(self, ticket: Ticket, release: dict) -> None:
        if release["id"] in ticket.fix_version_ids:
            log.info("%s already has fixVersion %s", ticket.key, release["name"])
            return
        if self.dry_run:
            log.info("[DRY-RUN] would add fixVersion %s to %s", release["name"], ticket.key)
            return
        self.jira.add_fix_version(ticket.key, release["id"])
        ticket.fix_version_ids.add(release["id"])  # keep idempotent within the run
        self.stats["added"] += 1
        log.info("Added fixVersion %s to %s", release["name"], ticket.key)

    def _alert(self, ticket: Ticket, pr: PullRequest, searched_name: str, rule: str) -> None:
        self.stats["alerts"] += 1
        alert = MissingReleaseAlert(
            ticket_key=ticket.key,
            ticket_url=f"{self.cfg.jira_base_url}/browse/{ticket.key}",
            repo=pr.repo,
            pr_number=pr.pr_number,
            pr_url=pr.url,
            base_branch=pr.base_branch,
            searched_name=searched_name,
            rule=rule,
        )
        self.slack.alert(alert)

    def _log_summary(self) -> None:
        s = self.stats
        log.info(
            "Reconciliation done: %d tickets, %d ruled PRs, %d fixVersions added, "
            "%d alerts, %d errors",
            s["tickets"], s["prs"], s["added"], s["alerts"], s["errors"],
        )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Jira Fix-Version reconciliation")
    parser.add_argument("--dry-run", action="store_true",
                        help="log intended writes without modifying Jira")
    parser.add_argument("--ticket", help="process a single ticket key (e.g. MOD-123)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = Config.from_env()
    agent = Agent(cfg, dry_run=args.dry_run)
    if args.ticket:
        # Defense in depth: --ticket may originate from a workflow_dispatch input.
        if not re.fullmatch(r"MOD-\d+", args.ticket):
            parser.error(f"--ticket must be a MOD issue key (got {args.ticket!r})")
        agent.run_single(args.ticket)
    else:
        agent.run_reconciliation()
    return 0


if __name__ == "__main__":
    sys.exit(main())
