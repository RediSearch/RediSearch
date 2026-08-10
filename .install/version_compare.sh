#!/usr/bin/env bash

version_ge() {
    local have="${1%%[-+]*}"
    local want="${2%%[-+]*}"
    local sorter=()

    if sort -V </dev/null >/dev/null 2>&1; then
        sorter=(sort -V)
    elif command -v gsort >/dev/null 2>&1 && gsort -V </dev/null >/dev/null 2>&1; then
        sorter=(gsort -V)
    fi

    if [[ ${#sorter[@]} -gt 0 ]]; then
        [[ "$(printf '%s\n' "$want" "$have" | "${sorter[@]}" | head -1)" == "$want" ]]
        return
    fi

    local IFS=.
    local -a have_parts=($have)
    local -a want_parts=($want)
    local i h w

    for ((i = 0; i < ${#have_parts[@]} || i < ${#want_parts[@]}; i++)); do
        h="${have_parts[i]:-0}"
        w="${want_parts[i]:-0}"
        h="${h%%[^0-9]*}"
        w="${w%%[^0-9]*}"
        [[ -z "$h" ]] && h=0
        [[ -z "$w" ]] && w=0

        ((10#$h > 10#$w)) && return 0
        ((10#$h < 10#$w)) && return 1
    done

    return 0
}
