# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Event-driven entrypoint for the Fix-Version agent (design §5.1).

Reads a GitHub ``pull_request`` event payload (by default from the path in
``$GITHUB_EVENT_PATH``, as set by GitHub Actions) and processes the affected PR.

Usage::

    python -m dev_automation.jira_fixversion.handle_event [--event-path FILE]
                                                    [--dry-run] [--include-forks]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from .config import Config
from .events import parse_pr_event
from .reconcile import Agent

log = logging.getLogger("jira_fixversion.handle_event")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Jira Fix-Version PR-event handler")
    parser.add_argument("--event-path", default=os.environ.get("GITHUB_EVENT_PATH"),
                        help="GitHub event payload JSON (default: $GITHUB_EVENT_PATH)")
    parser.add_argument("--dry-run", action="store_true",
                        help="log intended writes without modifying Jira")
    parser.add_argument("--include-forks", action="store_true",
                        help="also act on fork PRs (default: skip in the event path)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.event_path or not os.path.exists(args.event_path):
        log.error("Event payload not found (path=%r). Set $GITHUB_EVENT_PATH or "
                  "pass --event-path.", args.event_path)
        return 1

    with open(args.event_path, encoding="utf-8") as f:
        payload = json.load(f)

    event = parse_pr_event(payload)
    if event is None:
        log.info("Payload is not a pull_request event; nothing to do")
        return 0

    log.info("Handling PR event: %s#%s action=%s base=%s fork=%s",
             event.repo, event.number, event.action, event.base_ref, event.is_fork)

    cfg = Config.from_env()
    if args.include_forks:
        cfg.skip_fork_prs = False
    agent = Agent(cfg, dry_run=args.dry_run)
    agent.run_event(event)
    return 0


if __name__ == "__main__":
    sys.exit(main())
