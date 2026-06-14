# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Parsing of ``src/version.h`` (design §8.1)."""

from __future__ import annotations

import re

# Both RediSearch and RediSearchEnterprise declare the version through these
# three macros in src/version.h.
_MACROS = (
    "REDISEARCH_VERSION_MAJOR",
    "REDISEARCH_VERSION_MINOR",
    "REDISEARCH_VERSION_PATCH",
)

# The deliberate sentinel committed on the `master` development branch.
MASTER_SENTINEL = (99, 99, 99)


class VersionParseError(ValueError):
    """Raised when src/version.h cannot be parsed into a (X, Y, Z) tuple."""


def parse_version_h(text: str) -> tuple[int, int, int]:
    """Parse the three ``REDISEARCH_VERSION_*`` macros from ``src/version.h``.

    Returns ``(major, minor, patch)``. Raises :class:`VersionParseError` if any
    macro is missing.
    """
    nums: list[int] = []
    for macro in _MACROS:
        m = re.search(rf"^\s*#define\s+{macro}\s+(\d+)", text, re.MULTILINE)
        if not m:
            raise VersionParseError(f"{macro} not found in src/version.h")
        nums.append(int(m.group(1)))
    return (nums[0], nums[1], nums[2])
