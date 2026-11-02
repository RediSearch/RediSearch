#!/usr/bin/env bash
set -eo pipefail

MODE=${1:-}
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LCOV_INSTALL_VERSION="${LCOV_INSTALL_VERSION:-2.5}"
LCOV_INSTALL_SHA256="${LCOV_INSTALL_SHA256:-7e5e5a154bd5f3557659c328cab376764e7abd238bb403c424472c296b175126}"

if ! command -v apt_install >/dev/null 2>&1; then
    source "$HERE/deps_lib.sh"
fi

coverage_arch="$(uname -m)"
if [[ "${OS:-}" != ubuntu_* || "$coverage_arch" != x86_64 || "$PM" != apt ]]; then
    echo "coverage bootstrap is supported only on Ubuntu x86_64; found OS=${OS:-unknown}, architecture=$coverage_arch" >&2
    return 1 2>/dev/null || exit 1
fi

lcov_version() {
    lcov --version 2>/dev/null | sed -En 's/.*LCOV version ([0-9]+([.][0-9]+)*).*/\1/p' | head -1
}

lcov_is_supported() {
    local have_ver
    have_ver="$(lcov_version || true)"
    [[ -n "$have_ver" ]] && version_ge "$have_ver" "$LCOV_MIN_VERSION"
}

if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
    if lcov_is_supported; then
        DEPS_OPT_OK="$DEPS_OPT_OK lcov"
    else
        DEPS_OPT_MISSING="$DEPS_OPT_MISSING lcov:$LCOV_MIN_VERSION"
    fi
    return 0 2>/dev/null || exit 0
fi

if [[ "${DRY_RUN:-0}" == 1 ]]; then
    _dry_head "# LCOV >= $LCOV_MIN_VERSION required; pinned source fallback: $LCOV_INSTALL_VERSION"
    if lcov_is_supported; then
        _dry_dependency_status lcov satisfied "$(lcov_version) >= $LCOV_MIN_VERSION"
    else
        _dry_dependency_status lcov install-required ">= $LCOV_MIN_VERSION"
    fi
fi

if lcov_is_supported; then
    if [[ "${DRY_RUN:-0}" == 1 ]]; then
        _dry_head "# lcov $(lcov_version) already installed; no LCOV action needed"
    else
        echo "lcov $(lcov_version) already installed (>= required $LCOV_MIN_VERSION) - skipping"
    fi
    return 0 2>/dev/null || exit 0
fi

# Prefer the platform package because it also supplies LCOV's Perl modules.
apt_install lcov curl tar libcapture-tiny-perl libdatetime-perl \
    libdevel-stacktrace-perl libjson-xs-perl libtimedate-perl

if lcov_is_supported; then
    echo "lcov $(lcov_version) installed from the platform package"
    return 0 2>/dev/null || exit 0
fi

install_dir="/usr/local/lib/lcov-${LCOV_INSTALL_VERSION}"
archive="lcov-${LCOV_INSTALL_VERSION}.tar.gz"
download_url="https://github.com/linux-test-project/lcov/releases/download/v${LCOV_INSTALL_VERSION}/${archive}"
tools=(lcov genhtml geninfo genpng gendesc perl2lcov py2lcov xml2lcov xml2lcovutil.py llvm2lcov)

if [[ "${DRY_RUN:-0}" == 1 ]]; then
    _dry_head "# LCOV $LCOV_INSTALL_VERSION source fallback (only if the platform package remains below $LCOV_MIN_VERSION)"
    _dry_line "_lcov_have=\$(lcov --version 2>/dev/null | sed -En 's/.*LCOV version ([0-9]+([.][0-9]+)*).*/\\1/p' | head -1 || true)"
    _dry_line "_lcov_sort=sort"
    _dry_line "sort -V </dev/null >/dev/null 2>&1 || _lcov_sort=gsort"
    _dry_line "if [[ -z \"\$_lcov_have\" || \"\$(printf '%s\\n%s\\n' '$LCOV_MIN_VERSION' \"\$_lcov_have\" | \"\$_lcov_sort\" -V | head -1)\" != '$LCOV_MIN_VERSION' ]]; then"
    _dry_line "  tmp_dir=\$(mktemp -d)"
    _dry_line "  curl --fail --location --silent --show-error --retry 3 --proto '=https' --proto-redir '=https' --output \"\$tmp_dir/$archive\" \"$download_url\""
    _dry_line "  if command -v sha256sum >/dev/null 2>&1; then"
    _dry_line "    printf '%s  %s\\n' '$LCOV_INSTALL_SHA256' \"\$tmp_dir/$archive\" | sha256sum --check"
    _dry_line "  else"
    _dry_line "    actual_sha256=\$(shasum --algorithm 256 \"\$tmp_dir/$archive\" | awk '{print \$1}')"
    _dry_line "    [[ \"\$actual_sha256\" == '$LCOV_INSTALL_SHA256' ]]"
    _dry_line "  fi"
    _dry_line "  mkdir -p \"\$tmp_dir/source\""
    _dry_line "  tar --extract --gzip --file \"\$tmp_dir/$archive\" --strip-components=1 --directory \"\$tmp_dir/source\""
    _dry_line "  ${MODE:+$MODE }mkdir -p \"$install_dir\" /usr/local/bin"
    _dry_line "  ${MODE:+$MODE }cp -R \"\$tmp_dir/source/.\" \"$install_dir/\""
    for tool in "${tools[@]}"; do
        _dry_line "  ${MODE:+$MODE }ln -sfn \"$install_dir/bin/$tool\" \"/usr/local/bin/$tool\""
    done
    _dry_line "  rm -rf \"\$tmp_dir\""
    _dry_line "fi"
    return 0 2>/dev/null || exit 0
fi


tmp_dir="$(mktemp -d)"
curl --fail --location --silent --show-error --retry 3 \
    --proto '=https' --proto-redir '=https' \
    --output "$tmp_dir/$archive" \
    "$download_url"

if command -v sha256sum >/dev/null 2>&1; then
    echo "$LCOV_INSTALL_SHA256  $tmp_dir/$archive" | sha256sum --check
else
    actual_sha256="$(shasum --algorithm 256 "$tmp_dir/$archive" | awk '{print $1}')"
    [[ "$actual_sha256" == "$LCOV_INSTALL_SHA256" ]]
fi

mkdir -p "$tmp_dir/source"
tar --extract --gzip --file "$tmp_dir/$archive" \
    --strip-components=1 --directory "$tmp_dir/source"
_run mkdir -p "$install_dir" /usr/local/bin
_run cp -R "$tmp_dir/source/." "$install_dir/"
for tool in "${tools[@]}"; do
    _run ln -sfn "$install_dir/bin/$tool" "/usr/local/bin/$tool"
done
rm -rf "$tmp_dir"

if ! lcov_is_supported; then
    echo "failed to install LCOV >= $LCOV_MIN_VERSION" >&2
    return 1 2>/dev/null || exit 1
fi
lcov --version
