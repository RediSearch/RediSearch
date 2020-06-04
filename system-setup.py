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
        self.install("build-essential cmake")
        self.install("python-psutil")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.install("libatomic")
        self.group_install("'Development Tools'")
        self.install("cmake3")
        self.run("ln -s `command -v cmake3` /usr/local/bin/cmake")

        self.install("centos-release-scl")
        self.install("devtoolset-8")
        self.run("cp /opt/rh/devtoolset-8/enable /etc/profile.d/scl-devtoolset-8.sh")
        paella.mkdir_p("%s/profile.d" % ROOT)
        self.run("cp /opt/rh/devtoolset-8/enable %s/profile.d/scl-devtoolset-8.sh" % ROOT)

        # uninstall and install psutil (order is important), otherwise RLTest fails
        self.run("pip uninstall -y psutil || true")
        self.install("python2-psutil")

    def fedora(self):
        self.install("libatomic")
        self.group_install("'Development Tools'")
        self.install("cmake")
        self.run("ln -s `command -v cmake3` /usr/local/bin/cmake")

    def macosx(self):
        if sh('xcode-select -p') == '':
            fatal("Xcode tools are not installed. Please run xcode-select --install.")

        self.install_gnu_utils()
        self.install("cmake")
    def common_last(self):
        self.run("pip uninstall -y -q redis redis-py-cluster ramp-packer RLTest rmtest semantic-version || true")
        # redis-py-cluster should be installed from git due to redis-py dependency
        self.pip_install("--no-cache-dir git+https://github.com/Grokzen/redis-py-cluster.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabsModules/RLTest.git@master")
        self.pip_install("--no-cache-dir git+https://github.com/RedisLabs/RAMP@master")
        self.pip_install("pudb")

        self.pip3_install("-r %s/readies/paella/requirements.txt" % HERE)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RediSearchSetup(nop = args.nop).setup()
