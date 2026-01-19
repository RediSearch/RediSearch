#!/bin/bash
#
# Decode stack traces from C++ test crashes.
#
# This script parses raw backtrace output from signal handlers and uses
# gdb to decode the addresses into function names and line numbers.
#
# Usage:
#   ./decode_stacktrace.sh [log_file...]
#   cat test_output.log | ./decode_stacktrace.sh
#
# Requirements:
#   - gdb must be installed
#   - The binary must exist at the specified path
#   - The binary should be built with debug symbols (-g) for best results

set -euo pipefail

# Check for macOS - skip decoding as gdb is not typically available
if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "WARNING: Stack trace decoding is not supported on macOS." >&2
    echo "Raw stack traces are available in the log files." >&2
    exit 0
fi

# Check for gdb
if ! command -v gdb &>/dev/null; then
    echo "WARNING: gdb not found, skipping stack trace decoding." >&2
    exit 0
fi

# Shorten a path to just filename
shorten_path() {
    basename "$1"
}

# Decode a single address using gdb
# Arguments: binary, offset
# Returns: decoded info or empty
decode_address_with_gdb() {
    local binary="$1"
    local offset="$2"

    if [[ ! -f "$binary" ]]; then
        return 1
    fi

    # Use gdb to decode the address
    # Output format: "0xADDR is in function_name (file.cpp:123)." or similar
    local gdb_output
    gdb_output=$(gdb -s "$binary" -ex "list *$offset" -batch -q 2>&1 | head -n1) || true

    # Check if gdb found useful info
    if [[ "$gdb_output" == 0x* ]] && [[ "$gdb_output" == *" is in "* ]]; then
        # Parse: "0x123 is in function_name (file.cpp:123)."
        local func_and_loc="${gdb_output#* is in }"
        local func_name="${func_and_loc%% (*}"
        local location=""
        if [[ "$func_and_loc" == *"("*":"*")"* ]]; then
            location="${func_and_loc#*(}"
            location="${location%).*}"
            # Shorten path
            local filepath="${location%:*}"
            local lineno="${location##*:}"
            location="$(shorten_path "$filepath"):${lineno}"
        fi
        echo "${func_name}${location:+ at $location}"
    elif [[ "$gdb_output" == "No line"* ]] || [[ "$gdb_output" == "No symbol"* ]]; then
        # No debug info available
        return 1
    else
        # Other gdb output - might still be useful
        return 1
    fi
}

# Decode and print a complete stack trace
# Arguments: frames_file, optional test_name, optional failure_reason
decode_stack_trace() {
    local frames_file="$1"
    local test_name="${2:-<unknown test>}"
    local failure_reason="${3:-}"

    echo ""
    echo "==============================================================================="
    if [[ -n "$failure_reason" ]]; then
        echo "DECODED STACK TRACE: $test_name ($failure_reason)"
    else
        echo "DECODED STACK TRACE: $test_name"
    fi
    echo "==============================================================================="

    # Read frames and decode each one
    local frame_num=0
    while IFS='|' read -r binary offset binary_short; do
        echo ""
        echo "[$frame_num] ${binary_short} +${offset}"

        # Try to decode with gdb
        local decoded
        if decoded=$(decode_address_with_gdb "$binary" "$offset" 2>/dev/null) && [[ -n "$decoded" ]]; then
            echo "    $decoded"
        elif [[ ! -f "$binary" ]]; then
            echo "    <binary not found>"
        else
            echo "    <unknown>"
        fi

        ((frame_num++)) || true
    done < "$frames_file"

    echo ""
    echo "==============================================================================="
}

# Parse a backtrace line and extract binary/address
# Returns: binary|address|binary_short or empty
# Handles both formats:
#   binary(+0xoffset)[0xabsolute]  (Ubuntu/Debian)
#   binary[0xabsolute]              (Rocky/RHEL)
parse_backtrace_line() {
    local line="$1"

    # Split on delimiters ()[] to extract binary and address
    # This handles both formats automatically
    IFS='()[]' read -ra parts <<< "$line"
    local binary="${parts[0]}"
    local address="${parts[1]}"

    # Clean up binary path (remove trailing spaces)
    binary="${binary%% *}"

    # Skip if no binary, no address, or vdso
    [[ -z "$binary" || -z "$address" || "$binary" == *"linux-vdso"* ]] && return 1

    # If address doesn't start with 0x or +0x, skip it
    [[ "$address" != 0x* && "$address" != +0x* ]] && return 1

    local binary_short
    binary_short=$(shorten_path "$binary")
    echo "${binary}|${address}|${binary_short}"
}

# Main processing
in_stack_trace=false
decoded_count=0
current_test=""
current_failure=""
frames_file=$(mktemp)
trap "rm -f '$frames_file'" EXIT

while IFS= read -r line || [[ -n "$line" ]]; do
    # Track test names from Google Test output: [ RUN      ] TestName.TestCase
    # Don't reset failure if test name matches (CTest line may have already set it)
    if [[ "$line" =~ \[\ RUN\ +\]\ +([^[:space:]]+) ]]; then
        gtest_name="${BASH_REMATCH[1]}"
        if [[ "$gtest_name" != "$current_test" ]]; then
            current_test="$gtest_name"
            current_failure=""  # Reset failure reason only for different test
        fi
        continue
    fi

    # Track test names and failure reasons from CTest output:
    # Test #123: test_name ...***Exception: SegFault  0.03 sec
    # Test #123: test_name ...***Timeout  2.01 sec
    if [[ "$line" =~ Test\ \#[0-9]+:\ +([^[:space:]]+) ]]; then
        current_test="${BASH_REMATCH[1]}"
        # Extract failure reason if present
        if [[ "$line" =~ \*\*\*Exception:\ *([^[:space:]]+) ]]; then
            current_failure="${BASH_REMATCH[1]}"
        elif [[ "$line" =~ \*\*\*(Timeout|Failed|Skipped) ]]; then
            current_failure="${BASH_REMATCH[1]}"
        else
            current_failure=""
        fi
        # Don't continue - line may have more info
    fi

    if [[ "$line" == *"=== Caught fatal signal in C++ test, stack trace ==="* ]]; then
        in_stack_trace=true
        trace_test="$current_test"        # Save test context at START of trace
        trace_failure="$current_failure"
        > "$frames_file"  # Clear frames file
        continue
    fi

    if [[ "$line" == *"=== End of C++ test stack trace ==="* ]]; then
        if [[ "$in_stack_trace" == true ]]; then
            decode_stack_trace "$frames_file" "$trace_test" "$trace_failure"
            ((decoded_count++)) || true
        fi
        in_stack_trace=false
        continue
    fi

    if [[ "$in_stack_trace" == true ]]; then
        frame_data=$(parse_backtrace_line "$line" 2>/dev/null) && echo "$frame_data" >> "$frames_file" || true
    fi
done < <(if [[ $# -gt 0 ]]; then cat "$@"; else cat; fi)

if [[ $decoded_count -gt 0 ]]; then
    echo ""
    echo "Stack trace decoding complete: $decoded_count trace(s) decoded"
fi
