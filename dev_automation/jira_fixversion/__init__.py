# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Jira Fix-Version Automation Agent for the MOD project.

See the design document:
https://redislabs.atlassian.net/wiki/spaces/DX/pages/6346178605

This package automatically populates the ``fixVersions`` field on MOD Jira
tickets by inspecting the GitHub pull requests linked to each ticket, reading
the version declared in the repository's ``src/version.h`` and mapping it to
the matching Jira release(s). Missing releases raise a Slack alert.
"""
