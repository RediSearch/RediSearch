#!/usr/bin/env python3
"""Guard the SIMD tier objects against sharing externally visible symbols.

Every file under spaces/functions/ is compiled for one instruction-set tier, each with its
own -march/-m flags, and the running CPU's feature bits decide which tier's Choose_* entry
point is called. The kernels themselves are templates at namespace scope, so if two tiers
instantiate the same template they emit the same mangled symbol holding bodies built for
different architectures. The linker then keeps whichever object it saw first and nothing in
the source decides which. That either silently downgrades the faster tier or, once a body
contains an instruction the weaker tier's CPU lacks, faults in the query path.

Tiers stay distinct by naming: a kernel reachable from two tiers must carry the tier in its
name, as NEON_HP/NEON_FHM and SVE/SVE2 do. This test checks that invariant on the built
archive, so a new tier or a newly shared kernel header cannot reintroduce the collision
unnoticed.

Usage: check_tier_linkage.py <path to libVectorSimilaritySpaces.a>
"""

import re
import subprocess
import sys
from itertools import combinations

# Object files under spaces/functions/, i.e. the per-tier translation units. Anything else in
# the archive (the dispatchers, the preprocessor container) is tier-neutral and shared on
# purpose, so it is not part of this invariant.
TIER_PREFIXES = ("NEON", "SVE", "AVX", "SSE", "F16C")

# Tier-neutral helpers from shared type headers rather than from a kernel header. These are
# scalar bit manipulation with no instruction-set dependency, so every tier compiles them to
# the same bytes and link order cannot pick a wrong body. They surface only at -O0, where
# nothing is inlined away. The invariant this test enforces is about kernel code, so they are
# excluded by name rather than by weakening the check.
TIER_NEUTRAL = ("vecsim_types",)

# Toolchain bookkeeping that can be emitted into every translation unit. These are not
# executable kernel code and cannot select a body built for the wrong ISA.
TOOLCHAIN_SYMBOLS = {
    "___asan_globals_registered",
    "DW.ref.__gxx_personality_v0",
}


def tier_symbols(archive):
    """Map each tier object in the archive to its set of defined external symbols."""
    out = subprocess.run(["nm", "--defined-only", "--extern-only", archive],
                         check=True, capture_output=True, text=True).stdout
    tiers, current = {}, None
    for line in out.splitlines():
        member = re.fullmatch(r"(\S+\.o):", line.strip())
        if member:
            name = member.group(1)
            current = name if name.startswith(TIER_PREFIXES) else None
            if current:
                tiers.setdefault(current, set())
            continue
        if current and len(line.split()) == 3:
            symbol = line.split()[2]
            if (symbol not in TOOLCHAIN_SYMBOLS and
                    not any(ns in symbol for ns in TIER_NEUTRAL)):
                tiers[current].add(symbol)
    return tiers


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    archive = sys.argv[1]
    tiers = tier_symbols(archive)
    if not tiers:
        sys.exit("no tier objects found in %s, so nothing was checked" % archive)

    failures = []
    for a, b in combinations(sorted(tiers), 2):
        shared = tiers[a] & tiers[b]
        if shared:
            failures.append((a, b, sorted(shared)))

    print("checked %d tier objects, %d pairs" % (len(tiers), len(tiers) * (len(tiers) - 1) // 2))
    for name in sorted(tiers):
        print("  %-28s %4d external symbols" % (name, len(tiers[name])))

    if not failures:
        print("PASS: no tier pair shares an externally visible symbol")
        return

    for a, b, shared in failures:
        print("\nFAIL: %s and %s both define %d symbol(s), so link order picks the body:"
              % (a, b, len(shared)))
        for sym in shared[:10]:
            print("    %s" % sym)
        if len(shared) > 10:
            print("    ... and %d more" % (len(shared) - 10))
    sys.exit("\n%d tier pair(s) share symbols. Give the kernel a name carrying its tier, as "
             "NEON_HP/NEON_FHM and SVE/SVE2 do." % len(failures))


if __name__ == "__main__":
    main()
