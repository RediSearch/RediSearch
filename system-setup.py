#!/usr/bin/env python2

import sys
import os
import argparse

ROOT = HERE = os.path.abspath(os.path.dirname(__file__))
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RediSearchSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.install_downloaders()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git")

    def debian_compat(self):
        self.install("libatomic1")
        self.run("%s/bin/getgcc" % READIES)

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.install("libatomic")

        self.run("%s/bin/getgcc --modern" % READIES)

        # fix setuptools
        self.run("yum remove -y python-setuptools || true")
        self.pip_install("-IU --force-reinstall setuptools")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        # self.run("pip uninstall -y psutil || true")
        # self.install("python2-psutil")

    def fedora(self):
        self.install("libatomic")
        self.run("%s/bin/getgcc" % READIES)

    def macos(self):
        self.install_gnu_utils()
        self.run("{PYTHON} {READIES}/bin/getredis -v 6 --force".format(PYTHON=self.python, READIES=READIES))

    def common_last(self):
        self.run("%s/bin/getcmake" % READIES)
        self.install("lcov")
        
        self.run("{PYTHON} {READIES}/bin/getrmpytools".format(PYTHON=self.python, READIES=READIES))
        self.pip_install("-r %s/tests/pytests/requirements.txt" % ROOT)
        self.pip_install("pudb awscli")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RediSearchSetup(nop = args.nop).setup()
