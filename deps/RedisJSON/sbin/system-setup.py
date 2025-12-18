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

class RedisJSONSetup(paella.Setup):
    def __init__(self, args):
        paella.Setup.__init__(self, args.nop)

    def common_first(self):
        self.install_downloaders()
        self.run(f"{READIES}/bin/enable-utf8", sudo=self.os != 'macos')
        self.install("git unzip rsync")

        if self.osnick == 'ol8':
            self.install("tar")
        self.run(f"{READIES}/bin/getclang --modern")
        self.run(f"{READIES}/bin/getrust")
        self.run(f"{READIES}/bin/getcmake --usr")

    def debian_compat(self):
        self.install("python3-dev")
        self.run(f"{READIES}/bin/getgcc")

    def redhat_compat(self):
        if self.dist == "centos" and self.os_version[0] < 9:
            self.install("redhat-lsb-core")
        self.install("which")
        self.run(f"{READIES}/bin/getgcc --modern")

        if not self.platform.is_arm():
            self.install_linux_gnu_tar()

    def fedora(self):
        self.run(f"{READIES}/bin/getgcc")

    def macos(self):
        self.install_gnu_utils()
        self.install("binutils")
        # self.run(f"{READIES}/bin/getgcc")
        self.run(f"{READIES}/bin/getclang --modern")

    def common_last(self):
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)
        self.run(f"{self.python} {READIES}/bin/getrmpytools --reinstall --modern")
        self.pip_install(f"-r {ROOT}/tests/pytest/requirements.txt")
        self.run(f"{READIES}/bin/getaws")
        self.run(f"NO_PY2=1 {READIES}/bin/getpudb")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RedisJSONSetup(args).setup()
