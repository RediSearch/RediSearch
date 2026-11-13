#!/bin/bash
CLANG_FMT_SRCS=$(find ./src/ \( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.h' -o -name '*.hh' -o -name '*.hpp' \))
CLANG_FMT_TESTS="$(find ./tests/ -type d \( -path ./tests/unit/build \) -prune -false -o  \( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.h' -o -name '*.hh' -o -name '*.hpp' \))"

if [[ $FIX == 1 ]]; then
    clang-format --verbose -style=file -i $CLANG_FMT_SRCS $CLANG_FMT_TESTS
else
    clang-format -style=file -Werror --dry-run $CLANG_FMT_SRCS $CLANG_FMT_TESTS
fi
