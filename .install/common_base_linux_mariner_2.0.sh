#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE tdnf install -q -y build-essential git wget ca-certificates tar unzip rsync \
                         openssl-devel openssl which

source install_cmake.sh $MODE

pip install -q --upgrade setuptools
pip install -q --upgrade pip
# TODO: move to requirements.* to .install folder?
pip install -q -r ../tests/pytests/requirements.txt
pip install -q -r ../tests/pytests/requirements.linux.txt

# These packages are needed to build the package
pip install -q addict toml jinja2 ramp-packer # TODO: move to requirements.txt

# List installed packages
pip list

# Install aws-cli for uploading artifacts to s3
curdir="$PWD"
cd /tmp/
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
$MODE ./aws/install
cd $curdir
