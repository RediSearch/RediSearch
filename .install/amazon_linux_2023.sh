#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE dnf update -y
$MODE dnf install -y wget git which gcc gcc-c++ libstdc++-static make rsync unzip clang clang-devel
$MODE dnf install -y openssl openssl-devel
