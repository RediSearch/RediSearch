export IWYU="$(pwd)/../include-what-you-use"
export PATH="$IWYU/build/bin:$PATH"
include-what-you-use --version

# 1. Run iwyu
python3 "$IWYU/iwyu_tool.py" \
    -p compile_commands.json \
    src/ \
    -- \
    2>&1 | tee /tmp/iwyu_report.txt

# Apply fixes automatically
python3 "$IWYU/fix_includes.py" \
    --comments \
    < /tmp/iwyu_report.txt


# Problems:
# - codespell doesn't like the truncated dependency symbols.
#   - long symbols: FieldsGlobalStats_UpdateIndexError ; RSFunctionRegistry_RegisterFunction
#   - TODO: remove last word from comma separated list `([>"] + // for .*)(.*)\.\.\.` with `$1, ...` ?
#   - TODO: remove words with no comma (truncated alone)`([>"]) + // for ([^,]*)\.\.\.`
#   - Alternative? do not generate comments.
# - query parser should not be modified
# - 