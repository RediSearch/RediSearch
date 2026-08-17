#!/usr/bin/env bash
# Rewrites `#include "udb.h"` -> `#include "../udb.h"` in the given file,
# so the generated _ducet_switch.c can be included from deps/libnu/ducet.c.
set -euo pipefail
sed -i.bak 's|^#include "udb.h"|#include "../udb.h"|' "$1"
rm -f "$1.bak"
