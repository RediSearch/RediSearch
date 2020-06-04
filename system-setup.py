#!/usr/bin/env python2

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "deps/readies"))
import paella

#----------------------------------------------------------------------------------------------

class RediSearchSetup(paella.Setup):
    def __init__(self, nop=False):
        paella.Setup.__init__(self, nop)

    def common_first(self):
        self.setup_pip()
        self.pip_install("wheel")
        self.pip_install("setuptools --upgrade")

        self.install("git cmake wget awscli lcov")

    def debian_compat(self):
        self.install("libatomic1")
        self.install("build-essential")
        self.install("python-psutil")

    def redhat_compat(self):
        self.install("libatomic")
        self.group_install("'Development Tools'")
        self.install("redhat-lsb-core")

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil || true")
        self.install("python2-psutil")

    def fedora(self):
        self.install("libatomic")
        self.group_install("'Development Tools'")

    def macosx(self):
        if sh('xcode-select -p') == '':
            fatal("Xcode tools are not installed. Please run xcode-select --install.")

    def common_last(self):
        self.run("pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest rmtest semantic-version || true")
        # redis-py-cluster should be installed from git due to redis-py dependency
        self.pip_install("--no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabs/RAMP@master")
        self.pip_install("pudb")

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RediSearchSetup(nop = args.nop).setup()
