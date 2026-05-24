# libnu casemap regeneration pipeline

This directory contains everything needed to regenerate the six MPH-encoded
data files under `../gen/` (`_tofold.c`, `_tolower.c`, `_toupper.c`,
`_tounaccent.c`, `_ducet.c`, `_ducet_switch.c`) from raw UCD inputs.

```
regen/
├── tools/             # Python pipeline from upstream nunicode 1.11,
│                      # ported to Python 3 (see "Py3 port" below).
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

## Py3 port

Upstream `tools/` shipped Python 2 scripts. The port applied here:

* `2to3 -w -n` over every file in `tools/` (print → print(), xrange,
  filter/map → list, etc.).
* `tools/mph.py:153` — `ord(x)` on a bytes iterator drops the `ord()`
  call (`x` is already an int in Py3 when iterating bytes).
* `tools/mph-combined:36` and `tools/contractions-toc:141-143` —
  `len(X) / N` integer division fixed to `// N`.
* `tools/contractions-toc:54-67` and `:115` — two `set()` → list
  conversions now go through `sorted()` so state ID assignment and
  switch-case ordering are deterministic across runs (Py3 randomizes
  hash iteration order for arbitrary types).
* Shebangs in extension-less scripts changed to
  `#!/usr/bin/env python3`.

Confirmed: running the pipeline against the original Unicode 13.0 UCD
inputs (the version the previously-vendored tables were built from in
February 2020) reproduces the five MPH tables byte-for-byte, modulo
the embedded `time.time()` header timestamp. `_ducet_switch.c` is
semantically equivalent but reorders internal `#define state_…` IDs
because Py2 set-iteration order is irrecoverable.

## Variants not regenerated

Upstream also produces `*_compact.c` variants (BMP-only) and a set of
`_*_test.c` files for nunicode's own test suite. RediSearch does not
consume either, so the vendored CMakeLists.txt only builds the six
files actually included by `deps/libnu/`.
