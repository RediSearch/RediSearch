# libnu casemap regeneration pipeline

This directory contains everything needed to regenerate the six MPH-encoded
data files under `../gen/` (`_tofold.c`, `_tolower.c`, `_toupper.c`,
`_tounaccent.c`, `_ducet.c`, `_ducet_switch.c`) from raw UCD inputs.

```
regen/
├── tools/             # Python pipeline from upstream nunicode
│                      # (natively Python 3 since 1.11.1).
├── unicode.org/       # UCD inputs (Unicode 17.0, fetched 2025-2026).
├── CMakeLists.txt     # Regen recipe (adapted from upstream).
├── LICENSE.upstream   # Original nunicode license (MIT, covers tools/).
└── README.md          # This file.
```

## Running

```sh
cmake -B build -S .
cmake --build build --target gen
```

This rewrites `../gen/_*.c` in place. Re-runs are deterministic.

Per-table targets are also available: `_tofold`, `_tolower`, `_toupper`,
`_tounaccent`, `_ducet`. The `_ducet` target also generates
`_ducet_switch.c` and runs `tools/fixup-ducet-switch-include.sh` over
it to rewrite `#include "udb.h"` → `#include "../udb.h"` so the file
can be included from `deps/libnu/ducet.c` (one dir up from `gen/`).

## Upgrading Unicode

1. Replace files in `unicode.org/` from <https://www.unicode.org/Public/>:
   * `UnicodeData.txt`, `CaseFolding.txt`, `SpecialCasing.txt` from
     `ucd/` of the target Unicode version.
   * `allkeys.txt`, `decomps.txt` from `UCA/<version>/` (or
     `UCA/latest/`).
2. Run the pipeline as above.
3. Review the resulting `LIBNU_FOLD_GAPS` set in
   `tests/pytests/test_multibyte_char_terms.py` — codepoints added
   between the previous and new Unicode versions that gained
   uppercase/lowercase pairs must be removed from the gap set. Run
   `testToLowerConversionExactMatch` to surface stale entries.

## Tool provenance

`tools/` tracks upstream nunicode, which ported its pipeline to
Python 3 and made generation order deterministic itself (the earlier
RediSearch-local py3 port is superseded). The only local addition is
`tools/fixup-ducet-switch-include.sh` (see "Running" above). Upstream's
`poetry.lock`, `pyproject.toml`, and `tools/README.md` are dev-lint
scaffolding for the nunicode repo and are not vendored.

## Variants not regenerated

Upstream also produces `*_compact.c` (BMP-only) variants and a set of
`_*_test.c` files for nunicode's own test suite. The `../gen/*_compact.c`
files are vendored verbatim from upstream but only compiled under
`NU_WITH_BMP_ONLY`, which RediSearch never defines; the test files are
not vendored. The CMakeLists.txt here regenerates only the six tables
actually included by `deps/libnu/`.
