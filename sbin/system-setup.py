#!/usr/bin/env python3

import sys
import os
import argparse

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RediSearchSetup(paella.Setup):
    def __init__(self, args):
        paella.Setup.__init__(self, args.nop)

    def common_first(self):
        self.install_downloaders()
        self.setup_dotlocal()

        self.run(f"{READIES}/bin/enable-utf8", sudo=self.os != 'macos')
        self.install("git gawk jq openssl rsync unzip")

    def linux_first(self):
        self.install("patch psmisc")

    def debian_compat(self):
        self.install("libatomic1")
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("libtool m4 automake libssl-dev")
        self.install("python3-dev")
        # self.install("libboost-all-dev")

        if self.platform.is_arm():
            if self.dist == 'ubuntu' and self.os_version[0] < 20:
                pass
            else:
                self.install("libffi-dev")

    def redhat_compat(self):
        if self.dist == "centos" and self.os_version[0] < 9:
            self.install("redhat-lsb-core")

        self.install("which")
        self.run(f"{READIES}/bin/getepel")
        self.install("libatomic")

        if self.dist == "centos" and self.os_version[0] == 7:
            self.run(f"{READIES}/bin/getgcc --modern --update-libstdc++")
        elif self.dist == "centos" and self.os_version[0] == 9:
            # avoid gcc 12 for the time being
            self.run(f"{READIES}/bin/getgcc")
        else:
            self.run(f"{READIES}/bin/getgcc --modern")
        self.install("libstdc++-static")

        self.install("libtool m4 automake openssl-devel")
        self.install("python3-devel")
        # self.install("--skip-broken boost169-devel")

        if not self.platform.is_arm():
            self.install_linux_gnu_tar()

    def archlinux(self):
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("libtool m4 automake")
        # self.install("boost-dev")

    def fedora(self):
        self.install("libatomic")
        self.run(f"{READIES}/bin/getgcc --modern")
        self.install("openssl-devel")
        # self.install("boost-devel")

    def macos(self):
        self.install_gnu_utils()
        self.install("pkg-config")
        self.install("libtool m4 automake")
        self.run(f"{READIES}/bin/getclang --force --modern")
        # self.install("boost")
        self.pip_install(f"-r {ROOT}/tests/pytests/requirements.macos.txt")
        # self.run(f"{self.python} {READIES}/bin/getredis -v 6 --force")

    def linux_last(self):
        self.pip_install(f"-r {ROOT}/tests/pytests/requirements.linux.txt")

    def common_last(self):
        self.run(f"{self.python} {READIES}/bin/getcmake --usr", sudo=self.os != 'macos')
        self.run(f"{self.python} {READIES}/bin/getrmpytools --reinstall --modern")
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)

        self.pip_install("conan")
        self.pip_install(f"-r {ROOT}/tests/pytests/requirements.txt")
        self.run(f"{READIES}/bin/getaws")
        self.run(f"NO_PY2=1 {READIES}/bin/getpudb")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RediSearchSetup(args).setup()
