#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

# install_script.sh runs this as a subprocess, so deps_lib.sh isn't in scope —
# pull it in (self-guarded, no-op if already loaded) for _dry_line / mode vars.
if ! command -v _dry_line >/dev/null 2>&1; then
    source "$(dirname "${BASH_SOURCE[0]}")/../deps_lib.sh"
fi

# list: nothing to record here — uv is reported by install_python.sh and these
# are project-locked Python deps, not system packages.
if [[ "${CHECK_DEPS:-0}" == 1 ]]; then
    return 0 2>/dev/null || exit 0
fi

activate_venv() {
	echo "copy activation script to shell config"
	if [[ $OS_TYPE == Darwin ]]; then
		echo "source .venv/bin/activate" >> ~/.bashrc
		echo "source .venv/bin/activate" >> ~/.zshrc
	else
		echo "source $PWD/.venv/bin/activate" >> ~/.bash_profile
		echo "source $PWD/.venv/bin/activate" >> ~/.bashrc
		# Adding the virtual environment activation script to the shell profile
		# causes $PATH issues on platforms like Debian and Alpine,
		# shadowing the pre-existing source command to make some of our tools available.
		# We work around it by appending the required lines to the shell profile
		# _after_ the venv activation script

		# Always use $HOME - works in both container and non-container
		# In container: HOME=/root (fixed by workflow step 117-132)
		# On runner: HOME=/home/runner (already correct)
		# cargo
		echo '. "$HOME/.cargo/env"' >> ~/.bash_profile
		# rustup
		echo 'export RUSTUP_HOME=$HOME/.rustup' >> ~/.bash_profile
		# uv
		echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bash_profile
	fi
}

# On musl (Alpine), uv's standalone CPython is clang-built and bakes
# clang-only flags (--rtlib=compiler-rt) into its sysconfig, so sdist-only
# deps (psutil, ml-dtypes) fail to compile with the system gcc — build
# them with the clang toolchain the LTO setup already installed instead.
# Prefer CPython 3.13 when uv can provide it for the current platform: the
# locked scipy/numpy versions have cp313 musllinux wheels, while Alpine edge
# may ship a newer system Python that forces source builds. On platforms where
# uv cannot provide 3.13, keep the distro Python fallback.
if [[ -f /etc/alpine-release ]]; then
    if [[ "${DRY_RUN:-0}" == 1 ]]; then
        _dry_line 'export CC=clang CXX=clang++'
        _dry_line 'if uv python list 3.13 2>/dev/null | grep -q .; then export UV_PYTHON=3.13; fi'
        _dry_line 'if [[ -n "${GITHUB_ENV:-}" && "${UV_PYTHON:-}" == "3.13" ]]; then echo "UV_PYTHON=3.13" >> "$GITHUB_ENV"; fi'
    else
        export CC=clang CXX=clang++
        if uv python list 3.13 2>/dev/null | grep -q .; then
            export UV_PYTHON=3.13
            # Persist to the whole job, not just this script's process. The test
            # harness runs `uv run python3 -m RLTest`, and `uv run` re-provisions the
            # venv; without UV_PYTHON in scope it falls back to the system CPython
            # 3.12, relinking .venv. pip is installed under python3.13/site-packages,
            # so the 3.12 interpreter can't import it and RedisJSON's readies getpy3
            # (`python3 -m pip --version`) fails with "Cannot find python3 interpreter".
            [[ -n "${GITHUB_ENV:-}" ]] && echo "UV_PYTHON=3.13" >> "$GITHUB_ENV"
        fi
    fi
fi

if [[ "${DRY_RUN:-0}" == 1 ]]; then
    _dry_line "( cd \"$PWD\" && uv venv --seed --clear )"
    _dry_line "source \"$PWD/.venv/bin/activate\""
    _dry_line "( cd \"$PWD\" && uv sync --locked --all-packages )"
    _dry_line "( cd \"$PWD\" && uv run pip list )"
    return 0 2>/dev/null || exit 0
fi

# Create a virtual environment for Python tests, with `pip` pre-installed (--seed).
# --clear ensures a partial .venv left behind by a failed (e.g. network-timed-out)
# attempt is replaced rather than causing "virtual environment already exists" on retry.
uv venv --seed --clear
if [[ "${SKIP_VENV_PROFILE_ACTIVATION:-0}" != 1 ]]; then
	activate_venv
fi
source .venv/bin/activate
uv sync --locked --all-packages

# List installed packages
uv run pip list
