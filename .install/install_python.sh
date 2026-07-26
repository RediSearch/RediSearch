#!/usr/bin/env bash
set -eo pipefail
# xtrace for real installs only — this file is `source`d, so `set -x` in
# list/dry-run would trace the whole run and bury the report/dry-run script.
[ "${CHECK_DEPS:-0}" = 1 ] || [ "${DRY_RUN:-0}" = 1 ] || set -x

processor=$(uname -m)
OS_TYPE=$(uname -s)
MIN_UV_VERSION=0.9.13
source "$(dirname "${BASH_SOURCE[0]}")/version_compare.sh"

# Always install to the current user's HOME directory
# In containers: HOME=/root (running as root)
# On GitHub runners: HOME=/home/runner (running as runner user)
export UV_INSTALL_DIR=$HOME/.local/bin
# Make sure the eventual uv install dir is on PATH for the duration of
# this script, so the `command -v uv` check below sees a pre-installed
# uv even when this shell's PATH didn't already include $HOME/.local/bin.
export PATH="$UV_INSTALL_DIR:$PATH"

# Reuse an existing uv only if it meets the minimum version required by this
# repo. This keeps bootstrap idempotent without accepting stale uv binaries
# from the system or a previous manual install.
have_ver="$(uv --version 2>/dev/null | awk '/^uv / {print $2; exit}' || true)"

# list: record uv presence (optional dep); dry-run: print the uv installer if
# it's absent; real: unchanged. Never write /etc/profile.d or GITHUB_PATH here.
if [ "${CHECK_DEPS:-0}" = 1 ]; then
    set +x
    if [[ -n "$have_ver" ]] && version_ge "$have_ver" "$MIN_UV_VERSION"; then DEPS_OPT_OK="$DEPS_OPT_OK uv"; else DEPS_OPT_MISSING="$DEPS_OPT_MISSING uv"; fi
    return 0 2>/dev/null || exit 0
fi
if [ "${DRY_RUN:-0}" = 1 ]; then
    set +x
    if ! { [[ -n "$have_ver" ]] && version_ge "$have_ver" "$MIN_UV_VERSION"; }; then
        _dry_line "curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=\"\$HOME/.local/bin\" sh"
        _dry_line "export PATH=\"\$HOME/.local/bin:\$PATH\""
    fi
    return 0 2>/dev/null || exit 0
fi

if [[ -n "$have_ver" ]] && version_ge "$have_ver" "$MIN_UV_VERSION"; then
    echo "uv $have_ver already installed (>= required $MIN_UV_VERSION) at $(command -v uv) - skipping installer"
else
    if [[ -n "$have_ver" ]]; then
        echo "uv $have_ver is older than required $MIN_UV_VERSION - installing fresh uv"
    else
        echo "Installing uv (no existing uv on PATH)..."
    fi
    curl --proto '=https' --tlsv1.2 -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$UV_INSTALL_DIR" sh
    hash -r
fi

# Verify uv is in path
uv -vV
uv_bin="$(command -v uv)"
uv_bin_dir="$(dirname "$uv_bin")"

# GitHub Actions runs each step in a fresh shell, so export PATH above is
# only enough for this script. Persist the verified uv directory for later
# steps such as .install/test_deps/install_python_deps.sh.
if [[ -n "${GITHUB_PATH:-}" ]]; then
    echo "$uv_bin_dir" >> "$GITHUB_PATH"
fi

# Some container images, including Alpine, reset PATH for root login shells,
# hiding the GITHUB_PATH/Dockerfile PATH additions when later CI steps run
# under `bash -l`. Keep uv visible for those shells too.
if [[ -d /etc/profile.d && ( -w /etc/profile.d || $(id -u) -eq 0 ) ]]; then
    printf 'export PATH="%s:$PATH"\n' "$uv_bin_dir" > /etc/profile.d/redisearch-uv.sh
    chmod 0644 /etc/profile.d/redisearch-uv.sh
fi

# Print where `uv` is located for debugging purposes
echo "uv binary location: $uv_bin"
