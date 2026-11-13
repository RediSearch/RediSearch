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

class VecSimSetup(paella.Setup):
    def __init__(self, args):
        paella.Setup.__init__(self, args.nop)

    def common_first(self):
        self.install_downloaders()

        self.run("%s/bin/enable-utf8" % READIES, sudo=self.os != 'macos')
        self.install("git")

    def debian_compat(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("python3-dev valgrind")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.run("%s/bin/getepel" % READIES, sudo=True)
        self.install("libatomic")

        self.run("%s/bin/getgcc --modern" % READIES)

    def fedora(self):
        self.install("libatomic")
        self.run("%s/bin/getgcc --modern" % READIES)

    def macos(self):
        self.install_gnu_utils()
        self.run("%s/bin/getgcc --modern" % READIES)

    def linux_last(self):
        self.run("%s/bin/getclang" % READIES)

    def common_last(self):
        self.run("{PYTHON} {READIES}/bin/getcmake --usr".format(PYTHON=self.python, READIES=READIES),
                 sudo=self.os != 'macos')
        self.run("{PYTHON} {READIES}/bin/getrmpytools --reinstall".format(PYTHON=self.python, READIES=READIES))
        self.run("%s/bin/getclang --format" % READIES)
        self.pip_install("-r %s/sbin/requirements.txt" % ROOT)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

VecSimSetup(args).setup()
