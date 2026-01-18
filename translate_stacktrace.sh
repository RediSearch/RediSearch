#!/usr/bin/env bash
BIN="$1"; [[ -z "$BIN" ]] && { echo "Usage: $0 <binary>" >&2; exit 1; }
shift
while IFS= read -r line; do
  if [[ "$line" == *"$BIN"* ]] && [[ "$line" =~ \(\+0x([0-9a-fA-F]+)\)\[[^]]+\] ]]; then
    off="0x${BASH_REMATCH[1]}"; info=$(addr2line -e "$BIN" -f -C "$off"); echo "$line"; echo "    $info"
  else
    echo "$line"
  fi
done
