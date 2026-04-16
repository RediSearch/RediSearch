#!/usr/bin/env bash
set -eo pipefail

export DEBIAN_FRONTEND=noninteractive

# =============================================================================
# install_llvm.sh — Install LLVM across all CI platforms
#
# Usage:
#   ./install_llvm.sh [MODE]
#
# MODE is an optional privilege-escalation prefix (e.g. "sudo").
# If empty, commands run unprivileged (useful inside containers).
#
# Reads LLVM_VERSION.sh for LLVM_VERSION (major, e.g. "21") and
# LLVM_FULL_VERSION (e.g. "21.1.8").
#
# Environment variables:
#   LLVM_INSTALL_DIR  — Where to unpack tarball installs (default: /usr/local/llvm)
# =============================================================================

OS_TYPE=$(uname -s)
ARCH=$(uname -m)

# Source LLVM_VERSION (major) and LLVM_FULL_VERSION (e.g. 21.1.8)
source "$(dirname "${BASH_SOURCE[0]}")/LLVM_VERSION.sh"
LLVM_VER="${LLVM_VERSION}"
LLVM_FULL_VER="${LLVM_FULL_VERSION}"

INSTALL_DIR="${LLVM_INSTALL_DIR:-/usr/local/llvm}"
MODE="${1:-}"

# Download and unpack the official LLVM tarball into $INSTALL_DIR.
# Works on any glibc-based Linux. Will NOT work on musl/Alpine.
install_from_tarball() {
    local tarball_name
    case "$ARCH" in
        x86_64)  tarball_name="LLVM-${LLVM_FULL_VER}-Linux-X64.tar.xz" ;;
        aarch64) tarball_name="LLVM-${LLVM_FULL_VER}-Linux-ARM64.tar.xz" ;;
        *)       echo "ERROR: unsupported arch ${ARCH}"; return 1 ;;
    esac

    local url="https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_FULL_VER}/${tarball_name}"

    echo ">>> Downloading LLVM ${LLVM_FULL_VER} from ${url}"

    local tmpdir
    tmpdir=$(mktemp -d)
    curl -fSL --retry 3 -o "${tmpdir}/${tarball_name}" "$url"

    echo ">>> Extracting to ${INSTALL_DIR}..."
    $MODE mkdir -p "$INSTALL_DIR"
    $MODE tar -xf "${tmpdir}/${tarball_name}" -C "$INSTALL_DIR" --strip-components=1
    rm -rf "$tmpdir"

    export_path_gha
    echo ">>> LLVM ${LLVM_FULL_VER} installed to ${INSTALL_DIR}"
}

# Wire up $INSTALL_DIR/bin for GitHub Actions and the current shell.
export_path_gha() {
    local bindir="${INSTALL_DIR}/bin"
    if [[ -n "${GITHUB_PATH:-}" ]]; then
        echo "${bindir}" >> "$GITHUB_PATH"
    fi
    if [[ -n "${GITHUB_ENV:-}" ]]; then
        echo "LIBCLANG_PATH=${INSTALL_DIR}/lib" >> "$GITHUB_ENV"
    fi
    export PATH="${bindir}:${PATH}"
    export LIBCLANG_PATH="${INSTALL_DIR}/lib"
}

# ---------------------------------------------------------------------------
# Source the macOS profile updater if present.
# ---------------------------------------------------------------------------
if [[ "$OS_TYPE" == "Darwin" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -f "${SCRIPT_DIR}/macos_update_profile.sh" ]]; then
        source "${SCRIPT_DIR}/macos_update_profile.sh"
    fi
fi

# ---------------------------------------------------------------------------
# Detect distro and install.
# ---------------------------------------------------------------------------
install_llvm() {
    # ----- macOS (Homebrew) ---------------------------------------------------
    if [[ "$OS_TYPE" == "Darwin" ]]; then
        echo ">>> macOS — installing via Homebrew"
        brew install "llvm@${LLVM_VER}"

        local brew_prefix llvm_bin
        brew_prefix=$(brew --prefix)
        llvm_bin="${brew_prefix}/opt/llvm@${LLVM_VER}/bin"

        if type update_profile &>/dev/null; then
            [[ -f ~/.bash_profile ]] && update_profile ~/.bash_profile "$llvm_bin"
            [[ -f ~/.zshrc ]]        && update_profile ~/.zshrc "$llvm_bin"
        fi
        [[ -n "${GITHUB_PATH:-}" ]] && echo "${llvm_bin}" >> "$GITHUB_PATH"
        return 0
    fi

    # ----- Linux: detect distro -----------------------------------------------
    local distro="" distro_version=""
    if [[ -f /etc/os-release ]]; then
        distro=$(. /etc/os-release && echo "${ID:-unknown}")
        distro_version=$(. /etc/os-release && echo "${VERSION_ID:-}")
    fi
    # The GHA Alpine workaround rewrites /etc/os-release ID; detect via apk.
    if [[ -f /etc/alpine-release ]] && command -v apk &>/dev/null; then
        distro="alpine"
        distro_version=$(cat /etc/alpine-release)
    fi

    echo ">>> Detected distro=${distro} version=${distro_version} arch=${ARCH}"

    case "$distro" in

    # ----- Debian / Ubuntu (native apt → apt.llvm.org → tarball) --------------
    ubuntu|debian)
        source "$(dirname "${BASH_SOURCE[0]}")/apt_get_cmd.sh"
        apt_get_cmd "$MODE" update

        # 1) Try native distro packages first (e.g. Ubuntu 26.04 ships clang-21).
        if apt_get_cmd "$MODE" install -y --no-install-recommends \
                "clang-${LLVM_VER}" "lld-${LLVM_VER}" "libclang-${LLVM_VER}-dev" 2>/dev/null; then
            echo ">>> Installed clang-${LLVM_VER} from native apt repos"
        else
            # 2) Fall back to apt.llvm.org third-party repo.
            echo ">>> Native packages not available — trying apt.llvm.org"
            # software-properties-common was removed in Debian 13 (trixie).
            # The llvm.sh script handles trixie without add-apt-repository,
            # so we only install software-properties-common where available.
            local spc_pkg=""
            if apt-cache show software-properties-common &>/dev/null; then
                spc_pkg="software-properties-common"
            fi
            apt_get_cmd "$MODE" install -y --no-install-recommends \
                lsb-release wget $spc_pkg gnupg ca-certificates
            wget -qO /tmp/llvm.sh https://apt.llvm.org/llvm.sh
            chmod +x /tmp/llvm.sh
            if $MODE /tmp/llvm.sh "$LLVM_VER"; then
                rm -f /tmp/llvm.sh
            else
                # 3) Last resort: official pre-built tarball.
                echo ">>> apt.llvm.org failed — falling back to official tarball"
                rm -f /tmp/llvm.sh
                install_from_tarball
            fi
        fi
        ;;

    # ----- Alpine Linux (apk) ------------------------------------------------
    alpine)
        echo ">>> Alpine Linux — trying apk packages"
        # Alpine 3.23+ has llvm21 in main; 3.22 only has llvm20.
        # Official tarballs are glibc-based and won't work here.
        if $MODE apk add --no-cache "llvm${LLVM_VER}" "clang${LLVM_VER}" "clang${LLVM_VER}-libclang" "lld${LLVM_VER}" 2>/dev/null; then
            echo ">>> Installed llvm${LLVM_VER} from Alpine repos"
            # Create unversioned symlinks so bindgen/clang-sys can find llvm-config and clang.
            for tool in llvm-config clang clang++ lld ld.lld; do
                if [ -f "/usr/bin/${tool}-${LLVM_VER}" ] && [ ! -e "/usr/bin/${tool}" ]; then
                    $MODE ln -s "${tool}-${LLVM_VER}" "/usr/bin/${tool}"
                fi
            done
        elif $MODE apk add --no-cache llvm clang lld; then
            echo ">>> Installed default llvm/clang (may not be version ${LLVM_VER})"
            # Install matching libclang for bindgen — package name is version-specific.
            local default_ver
            default_ver=$(clang --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.' | head -1 | tr -d '.')
            if [ -n "$default_ver" ]; then
                $MODE apk add --no-cache "clang${default_ver}-libclang" 2>/dev/null || true
            fi
        else
            echo "ERROR: No LLVM package available. Upgrade to Alpine 3.23+ or use edge."
            return 1
        fi
        ;;

    # ----- Everything else: official tarball ----------------------------------
    # Rocky, CentOS, RHEL, AlmaLinux, Fedora, Amazon Linux, Mariner, Azure Linux
    *)
        echo ">>> ${distro} ${distro_version} — installing from official tarball"
        install_from_tarball
        ;;

    esac
}

install_llvm

echo ""
echo ">>> Verifying..."
# Calling 'clang --version' verifies that the installed clang binary can actually run.
# It catches issues like the system libstdc++ or glibc lacking symbol versions that clang needs.
if command -v "clang-${LLVM_VER}" &>/dev/null; then
    "clang-${LLVM_VER}" --version
elif command -v clang &>/dev/null; then
    clang --version
elif [[ -x "${INSTALL_DIR}/bin/clang" ]]; then
    "${INSTALL_DIR}/bin/clang" --version
else
    echo "WARNING: clang not found on PATH. You may need to add ${INSTALL_DIR}/bin to PATH."
fi
