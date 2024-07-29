#!/bin/bash
CLANG_FMT_SRCS=$(find ./src/ ./coord/ \( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.h' -o -name '*.hh' -o -name '*.hpp' \))
CLANG_FMT_TESTS="$(find ./tests/ -type d \( -path ./tests/unit/build \) -prune -false -o  \( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.h' -o -name '*.hh' -o -name '*.hpp' \))"

if [[ $FIX == 1 ]]; then
    isort --verbose --profile=black --gitignore tests/pytests
    black --verbose --exclude .log tests/pytests
    clang-format --verbose -style=file -i $CLANG_FMT_SRCS $CLANG_FMT_TESTS
else
    isort --check --profile=black --gitignore tests/pytests
    black --check --verbose --exclude .log tests/pytests
    clang-format --verbose -style=file -Werror --dry-run $CLANG_FMT_SRCS $CLANG_FMT_TESTS
fi
