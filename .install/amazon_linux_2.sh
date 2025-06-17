#!/usr/bin/env bash
ARCH=$(uname -m)
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE amazon-linux-extras enable python3.8
$MODE yum install -y python3.8 python38-devel which
$MODE ln -s "$(which python3.8)" /usr/bin/python3


$MODE yum install -y wget git rsync unzip clang curl make
$MODE yum groupinstall -y "Development Tools"

$MODE yum install -y openssl11 openssl11-devel
$MODE ln -s "$(which openssl11)" /usr/bin/openssl
