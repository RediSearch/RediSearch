#!/usr/bin/env python3
"""Download, compile, and package Google cpu_features v0.10.1 as a static library.

Called by the Buck2 genrule ``cpu_features_build`` in deps/BUCK.

Uses Buck2 genrule environment variables:
    $OUT  -- output directory (provided by Buck2)
    $TMP  -- scratch directory (provided by Buck2 via TMPDIR)

Produces:
    $OUT/include/  -- public headers
    $OUT/lib/libcpu_features.a -- static archive
"""

import glob
import os
import platform
import shutil
import subprocess
import sys
import tarfile
import urllib.request

VERSION = "0.10.1"
URL = f"https://github.com/google/cpu_features/archive/refs/tags/v{VERSION}.tar.gz"


def main():
    out = os.environ["OUT"]
    tmp = os.environ.get("TMPDIR", os.environ.get("TMP", "/tmp"))

    include_dir = os.path.join(out, "include")
    lib_dir = os.path.join(out, "lib")
    os.makedirs(include_dir, exist_ok=True)
    os.makedirs(lib_dir, exist_ok=True)

    # Download and extract into the scratch directory
    work = os.path.join(tmp, "cpu_features_build")
    os.makedirs(work, exist_ok=True)

    tarball = os.path.join(work, "cpu_features.tar.gz")
    urllib.request.urlretrieve(URL, tarball)
    with tarfile.open(tarball) as tf:
        tf.extractall(work)

    src = os.path.join(work, f"cpu_features-{VERSION}")

    # Copy public headers
    shutil.copytree(os.path.join(src, "include"), include_dir, dirs_exist_ok=True)

    # Compile all .c files under src/
    cflags = [
        "-std=c99",
        "-fPIC",
        "-O2",
        "-DSTACK_LINE_READER_BUFFER_SIZE=1024",
        f"-I{os.path.join(src, 'include')}",
        f"-I{os.path.join(src, 'include', 'internal')}",
    ]

    if platform.system() == "Darwin":
        cflags.append("-DHAVE_SYSCTLBYNAME")

    c_files = glob.glob(os.path.join(src, "src", "*.c"))
    obj_files = []

    for c_file in c_files:
        obj_file = os.path.join(work, os.path.basename(c_file).replace(".c", ".o"))
        subprocess.check_call(["cc"] + cflags + ["-c", c_file, "-o", obj_file])
        obj_files.append(obj_file)

    # Create static archive
    subprocess.check_call(
        ["ar", "rcs", os.path.join(lib_dir, "libcpu_features.a")] + obj_files
    )


if __name__ == "__main__":
    main()
