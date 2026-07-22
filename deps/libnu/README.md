# Libnu

The files in this folder are taken from the (excellent) **nunicode** library by Aleksey Tulinov.

See [https://bitbucket.org/alekseyt/nunicode](https://bitbucket.org/alekseyt/nunicode)

Vendored snapshot: upstream commit `5d3279b` (post-1.11.1, Unicode 17.0
tables). `NU_VERSION` reads `"custom"` because upstream only stamps a
version number on release tags.

Local deltas from upstream:

* `config.h` — `#define NU_WITH_EVERYTHING` added at the top (upstream
  expects it as a `-D` build flag).
* `gen/_ducet_switch.c` — `#include "udb.h"` rewritten to
  `#include "../udb.h"` (RediSearch compiles this file via inclusion
  from `ducet.c`, one directory up, without `-I` for `gen/`).
* `Makefile`, `README.md`, `regen/` — RediSearch-local; not upstream
  files. Upstream's `libnu/CMakeLists.txt` is not vendored.
