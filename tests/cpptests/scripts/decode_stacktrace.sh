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

    # Split by "(inlined by)" and process each part
    local first=true
    local parts="${result//(inlined by)/$'\x01'}"
    local IFS_OLD="$IFS"
    IFS=$'\x01'

    for part in $parts; do
        # Trim whitespace
        part="${part#"${part%%[![:space:]]*}"}"
        part="${part%"${part##*[![:space:]]}"}"
        [[ -z "$part" ]] && continue

        local func_name="" location=""
        if [[ "$part" == *" at "* ]]; then
            func_name="${part%% at *}"
            location="${part#* at }"
            # Shorten the path
            if [[ "$location" == *":"* ]]; then
                local filepath="${location%:*}"
                local lineno="${location##*:}"
                location="$(shorten_path "$filepath"):${lineno}"
            fi
        else
            func_name="$part"
        fi

        if [[ "$first" == true ]]; then
            echo "${indent}${func_name}${location:+ at $location}"
            first=false
        else
            echo "${indent}  inlined by: ${func_name}${location:+ at $location}"
        fi
    done
    IFS="$IFS_OLD"
}

# Decode and print a complete stack trace
# Arguments: arrays of frame data passed via temporary files
decode_stack_trace() {
    local frames_file="$1"

    echo ""
    echo "==============================================================================="
    echo "DECODED STACK TRACE"
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
            # Note: Using -Cfp (no -i) because -i produces multiple lines per address
            # which breaks batch processing. Individual calls would be needed for inlines.
            local results
            results=$("$ADDR2LINE" -e "$binary" -Cfp $offsets 2>/dev/null) || results=""
            # Store results indexed by offset
            local offset_array
            read -ra offset_array <<< "$offsets"
            local i=0
            while IFS= read -r decoded_line; do
                if [[ $i -lt ${#offset_array[@]} ]]; then
                    decoded_results["${binary}|${offset_array[$i]}"]="$decoded_line"
                fi
                ((i++)) || true
            done <<< "$results"
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
frames_file=$(mktemp)
trap "rm -f '$frames_file'" EXIT

while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" == *"=== Caught fatal signal in C++ test, stack trace ==="* ]]; then
        in_stack_trace=true
        > "$frames_file"  # Clear frames file
        continue
    fi

    if [[ "$line" == *"=== End of C++ test stack trace ==="* ]]; then
        if [[ "$in_stack_trace" == true ]]; then
            decode_stack_trace "$frames_file"
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
