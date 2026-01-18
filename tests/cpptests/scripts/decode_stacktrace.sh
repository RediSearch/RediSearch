#!/bin/bash
#
# Decode stack traces from C++ test crashes.
#
# This script parses raw backtrace output from signal handlers and uses
# addr2line to decode the addresses into function names and line numbers.
#
# Usage:
#   ./decode_stacktrace.sh [log_file...]
#   cat test_output.log | ./decode_stacktrace.sh
#
# Requirements:
#   - addr2line or llvm-addr2line must be installed
#   - The binary must exist at the specified path
#   - The binary should be built with debug symbols (-g) for best results

set -euo pipefail

# Find addr2line tool
find_addr2line() {
    if command -v llvm-addr2line &>/dev/null; then
        echo "llvm-addr2line"
    elif command -v addr2line &>/dev/null; then
        echo "addr2line"
    else
        echo ""
    fi
}

ADDR2LINE=$(find_addr2line)

if [[ -z "$ADDR2LINE" ]]; then
    echo "ERROR: Neither addr2line nor llvm-addr2line found." >&2
    exit 1
fi

# Shorten a path to just filename
shorten_path() {
    basename "$1"
}

# Format a single decoded line (handles inlined functions)
format_decoded_line() {
    local result="$1"
    local indent="$2"

    # Handle unknown
    if [[ "$result" == "??"* ]] || [[ -z "$result" ]]; then
        echo "${indent}<unknown>"
        return
    fi

    # Process each line (multi-line results have inlined functions on separate lines)
    local first=true
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue

        # Check if this is an inlined line
        local is_inlined=false
        if [[ "$line" == " (inlined by)"* ]]; then
            is_inlined=true
            # Remove the " (inlined by) " prefix
            line="${line# (inlined by) }"
        fi

        # Parse "func at location" format
        local func_name="" location=""
        if [[ "$line" == *" at "* ]]; then
            func_name="${line%% at *}"
            location="${line#* at }"
            # Shorten the path
            if [[ "$location" == *":"* ]]; then
                local filepath="${location%:*}"
                local lineno="${location##*:}"
                location="$(shorten_path "$filepath"):${lineno}"
            fi
        else
            func_name="$line"
        fi

        if [[ "$first" == true ]]; then
            echo "${indent}${func_name}${location:+ at $location}"
            first=false
        else
            echo "${indent}  inlined by: ${func_name}${location:+ at $location}"
        fi
    done <<< "$result"
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

    # Group frames by binary for batch processing
    declare -A binary_offsets
    declare -a frame_order

    while IFS='|' read -r binary offset binary_short; do
        frame_order+=("$binary|$offset|$binary_short")
        if [[ -n "${binary_offsets[$binary]:-}" ]]; then
            binary_offsets[$binary]+=" $offset"
        else
            binary_offsets[$binary]="$offset"
        fi
    done < "$frames_file"

    # Batch decode all offsets per binary
    declare -A decoded_results
    for binary in "${!binary_offsets[@]}"; do
        if [[ -f "$binary" ]]; then
            local offsets="${binary_offsets[$binary]}"
            # Call addr2line once with all offsets for this binary
            # Using -Cfpi for inline info. Parse output by detecting:
            # - Lines NOT starting with space = new address result
            # - Lines starting with " (inlined by)" = continuation of previous address
            local results
            results=$("$ADDR2LINE" -e "$binary" -Cfpi $offsets 2>/dev/null) || results=""
            # Store results indexed by offset
            local offset_array
            read -ra offset_array <<< "$offsets"
            local i=0
            local current_result=""
            while IFS= read -r decoded_line; do
                if [[ "$decoded_line" == " (inlined by)"* ]]; then
                    # Continuation of previous address - append with newline
                    current_result+=$'\n'"$decoded_line"
                else
                    # New address - save previous result (if any) and start new
                    if [[ $i -gt 0 && $((i-1)) -lt ${#offset_array[@]} ]]; then
                        decoded_results["${binary}|${offset_array[$((i-1))]}"]="$current_result"
                    fi
                    current_result="$decoded_line"
                    ((i++)) || true
                fi
            done <<< "$results"
            # Save last result
            if [[ $i -gt 0 && $((i-1)) -lt ${#offset_array[@]} ]]; then
                decoded_results["${binary}|${offset_array[$((i-1))]}"]="$current_result"
            fi
        fi
    done

    # Print frames in order
    local frame_num=0
    for frame in "${frame_order[@]}"; do
        IFS='|' read -r binary offset binary_short <<< "$frame"
        echo ""
        echo "[$frame_num] ${binary_short} +${offset}"

        local key="${binary}|${offset}"
        if [[ -n "${decoded_results[$key]:-}" ]]; then
            format_decoded_line "${decoded_results[$key]}" "    "
        elif [[ ! -f "$binary" ]]; then
            echo "    <binary not found>"
        else
            echo "    <unknown>"
        fi

        ((frame_num++)) || true
    done

    echo ""
    echo "==============================================================================="
}

# Parse a backtrace line and extract binary/offset
# Returns: binary|offset|binary_short or empty
parse_backtrace_line() {
    local line="$1"

    # Extract binary path (everything before the first '(')
    local binary="${line%%\(*}"

    # Skip if no binary or vdso
    [[ -z "$binary" || "$binary" == *"linux-vdso"* ]] && return 1

    # Extract offset
    local offset=""
    local regex_simple='[(][+]0x([0-9a-fA-F]+)[)]'
    local regex_symbol='[(][^)]+[+]0x([0-9a-fA-F]+)[)]'
    if [[ "$line" =~ $regex_simple ]]; then
        offset="0x${BASH_REMATCH[1]}"
    elif [[ "$line" =~ $regex_symbol ]]; then
        offset="0x${BASH_REMATCH[1]}"
    fi

    [[ -z "$offset" ]] && return 1

    local binary_short
    binary_short=$(shorten_path "$binary")
    echo "${binary}|${offset}|${binary_short}"
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
