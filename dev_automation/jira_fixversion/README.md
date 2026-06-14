# Jira Fix-Version Automation Agent

Automatically populates the **Fix Version** field on **MOD** Jira tickets from
the GitHub PRs linked to each ticket, by reading the version in the repo's
`src/version.h` and mapping it to the matching Jira release(s). Missing releases
raise a Slack alert.

Design doc:
<https://redislabs.atlassian.net/wiki/spaces/DX/pages/6346178605>

## Layout

| File | Responsibility |
| --- | --- |
| `version_parse.py` | Parse `src/version.h` → `(X, Y, Z)` (design §8.1) |
| `matching.py` | Pure matching engine: selection helpers + per-repo handlers (design §8) |
| `jira_client.py` | Jira REST: eligible tickets, dev-panel PR links, project versions, idempotent `fixVersions` append |
| `github_client.py` | GitHub REST: PR metadata + read `src/version.h` at a ref |
| `slack_client.py` | Missing-release alerts (design §9) via a Workflow Builder webhook; **stub mode** when unconfigured |
| `reconcile.py` | `Agent` + scheduled reconciliation entrypoint (design §5.2) |
| `events.py` | Parse `pull_request` payload + extract MOD issue keys (design §5.1) |
| `handle_event.py` | Event-driven entrypoint (reads `$GITHUB_EVENT_PATH`) |
| `config.py` | Env-driven configuration (design §11.3) |
| `tests/` | Offline unit tests for the matching engine and event parsing |

Both triggers feed the same per-PR matching:

- **Scheduled** (`reconcile.py`, `event-periodic-jira-fixversion.yml`): walks every
  eligible MOD ticket, resolves PRs via the Jira dev panel (the association of
  record, design §6).
- **Event-driven** (`handle_event.py`, `event-pr-jira-fixversion.yml`): on a
  `pull_request` event, extracts the MOD key(s) the PR references (branch / title /
  body) and applies matching to that PR. Lower latency; the scheduled path is the
  safety net for anything it misses.

The matching engine is side-effect-free and fully unit-tested offline.

### Event-path security

`event-pr-jira-fixversion.yml` uses `pull_request_target` (secrets available, runs
the trusted base-branch workflow; PR head code is never checked out or run). To
stop an external contributor writing fixVersions to MOD tickets they merely
reference:

- **Fork PRs are skipped in both paths by default** (`JFV_SKIP_FORK_PRS`, default
  `true`) — a fork PR can otherwise be linked by GitHub-for-Jira (issue key in the
  branch/title) and reach the scheduled path too.
- The event path resolves the ticket only from the **branch name and PR title**
  (not the PR body), so a "depends on MOD-123" mention does not trigger a write.
- A PR **closed without merging** is skipped (never marks a ticket as fixed).
- `workflow_dispatch` inputs are passed via env vars, never interpolated into the
  shell, and `--ticket` is validated against `MOD-\d+` (no script injection).

## Configuration (environment variables)

| Var | Purpose |
| --- | --- |
| `JIRA_BASE_URL`, `JIRA_USER_EMAIL`, `JIRA_API_TOKEN` | Jira Cloud auth |
| `GITHUB_TOKEN` / `GH_TOKEN` | Read `src/version.h` + PR metadata (needs both repos) |
| `GITHUB_ORG` | Owner org for the repos (default `RediSearch`) |
| `SLACK_WEBHOOK_URL_FIX_VERSION` | Slack Workflow Builder webhook; alert text is sent as the `payload` variable. Absent → stub mode (logs only) |
| `SLACK_BOT_TOKEN`, `SLACK_RELEASES_CHANNEL_ID` | Fallback `chat.postMessage` alerting if no webhook is set |
| `JFV_SKIP_FORK_PRS` | Skip fork PRs in both paths (default `true`) |
| `JFV_REDISEARCH_RELEASE_TEMPLATE`, `JFV_ENTERPRISE_RELEASE_TEMPLATE` | Override exact release-name templates (testing) |

## Running

```bash
# Offline unit tests (no network):
python3 -m unittest discover -s dev_automation/jira_fixversion

# Full reconciliation (needs live credentials):
python3 -m dev_automation.jira_fixversion.reconcile

# Safe preview / single ticket:
python3 -m dev_automation.jira_fixversion.reconcile --dry-run
python3 -m dev_automation.jira_fixversion.reconcile --ticket MOD-123 --dry-run
```

Scheduled run: `.github/workflows/event-periodic-jira-fixversion.yml`
(every 6h + manual `workflow_dispatch`).

## Known caveat — release-name matching

The matching engine uses these release-name forms:

- `RediSearch v{X}.{Y}.{Z}` — exact (version-branch case).
- `Open Source {X}.{Y}` — **two-part** (major.minor); Open Source releases carry
  no patch, so a concrete `X.Y.Z` maps to `Open Source X.Y` (deviation from the
  design doc §8.2 step 5, which wrote `Open Source X.Y.Z`; the doc's master case
  §8.2 step 4 already used the two-part form).
- `RediSearchEnterprise v{X}.{Y}.{Z}` — exact.

Real MOD releases also use variants the strict match will *miss* and therefore
alert on, e.g. `RediSearch v8.2.10-priv`, `Redis Open Source 8.4.1 - maint.`,
`OSS 8.8`. Normalizing/loosening the match is a likely follow-up requiring design
sign-off.
