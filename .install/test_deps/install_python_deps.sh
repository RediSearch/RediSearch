#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

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
    export CC=clang CXX=clang++
    if uv python list 3.13 2>/dev/null | grep -q .; then
        export UV_PYTHON=3.13
    fi
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

# [DEBUG MOD-16514 — temporary] On Alpine, capture why `python3 -m pip` is
# import-broken (RedisJSON's readies getpy3 needs it). Revert before merge.
if [[ -f /etc/alpine-release ]]; then
	echo "===== PIP DIAG (install_python_deps end) ====="
	command -v python3 && readlink -f "$(command -v python3)"
	python3 - <<'PYEOF' 2>&1 || true
import sys, os, importlib.util, traceback
print("exe:", sys.executable)
print("prefix:", sys.prefix, "| base_prefix:", sys.base_prefix)
print("VIRTUAL_ENV:", os.environ.get("VIRTUAL_ENV"))
print("PYTHONPATH:", os.environ.get("PYTHONPATH"))
print("PYTHONHOME:", os.environ.get("PYTHONHOME"))
print("LD_LIBRARY_PATH:", os.environ.get("LD_LIBRARY_PATH"))
print("sys.path:", sys.path)
print("find_spec(pip):", importlib.util.find_spec("pip"))
for sp in sys.path:
    try:
        e = [x for x in os.listdir(sp) if x.lower().startswith("pip")]
        if e: print("pip-ish in", sp, ":", e)
    except Exception:
        pass
try:
    import pip; print("import pip OK:", pip.__file__)
except Exception:
    print("import pip FAILED:"); traceback.print_exc()
PYEOF
	echo "===== END PIP DIAG ====="
fi
