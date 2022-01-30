#!/usr/bin/env python2

import sys
import os
import argparse

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.join(HERE, "..")
READIES = os.path.join(ROOT, "deps/readies")
sys.path.insert(0, READIES)
import paella

#----------------------------------------------------------------------------------------------

class RediSearchSetup(paella.Setup):
    def __init__(self, args):
        paella.Setup.__init__(self, args.nop)

    def common_first(self):
        self.install_downloaders()

        self.run("%s/bin/enable-utf8" % READIES, sudo=self.os != 'macos')
        self.install("git gawk jq openssl rsync unzip")

    def linux_first(self):
        self.install("patch")

    def debian_compat(self):
        self.install("libatomic1")
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libtool m4 automake libssl-dev")
        self.install("python-dev")

        if self.platform.is_arm():
            if self.dist == 'ubuntu' and self.os_version[0] < 20:
                self.install("python-gevent")
            else:
                self.install("libffi-dev")

    def redhat_compat(self):
        self.install("redhat-lsb-core")
        self.run("%s/bin/getepel" % READIES, sudo=True)
        self.install("libatomic")

        self.run("%s/bin/getgcc --modern" % READIES)
        if self.osnick == "xenial":
            self.run("GCC=1 POST=0 %s/bin/getconda" % READIES)
        self.install("libtool m4 automake openssl-devel")
        self.install("python2-devel")

        if self.platform.is_arm():
            self.install("python-gevent")
        else:
            self.install_linux_gnu_tar()

    def archlinux(self):
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("libtool m4 automake")

    def fedora(self):
        self.install("libatomic")
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("openssl-devel")

    def macos(self):
        self.install_gnu_utils()
        self.run("%s/bin/getgcc --modern" % READIES)
        self.install("pkg-config")
        self.install("libtool m4 automake")

        # for now depending on redis from brew, it's version6 with TLS.
        self.run("{PYTHON} {READIES}/bin/getredis -v 6 --force".format(PYTHON=self.python, READIES=READIES))

    def common_last(self):
        self.run("{PYTHON} {READIES}/bin/getcmake --usr".format(PYTHON=self.python, READIES=READIES),
                 sudo=self.os != 'macos')
        self.run("{PYTHON} {READIES}/bin/getrmpytools --reinstall".format(PYTHON=self.python, READIES=READIES))
        if self.dist != "arch":
            self.install("lcov")
        else:
            self.install("lcov-git", aur=True)
        self.pip_install("pudb awscli")

        if int(sh("{PYTHON} -c 'import gevent' 2> /dev/null; echo $?".format(PYTHON=self.python))) != 0:
            self.pip_install("gevent")

        self.pip_install("-r %s/tests/pytests/requirements.txt" % ROOT)

#----------------------------------------------------------------------------------------------

parser = argparse.ArgumentParser(description='Set up system for build.')
parser.add_argument('-n', '--nop', action="store_true", help='no operation')
args = parser.parse_args()

RediSearchSetup(args).setup()
