#!/usr/bin/env bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive
$MODE dnf update -y

$MODE dnf install -y gcc make wget git \
    openssl openssl-devel which rsync unzip clang curl clang-devel --nobest --skip-broken --allowerasing
