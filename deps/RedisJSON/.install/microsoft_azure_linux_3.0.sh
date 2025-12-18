#!/bin/bash
# This script automates the process of setting up a development environment for RedisJSON on a Microsoft Azure Linux virtual machine.

set -e

# Update and install dev tools needed for building and testing
tdnf -y update && \
tdnf install -y \
        git \
        wget \
        gcc \
        clang-devel \
        llvm-devel \
        make \
        cmake \
        libffi-devel \
        openssl-devel \
        build-essential \
        zlib-devel \
        bzip2-devel \
        python3-devel \
        which \
        unzip \
        ca-certificates \
        python3-pip

# Install AWS CLI for uploading to S3
pip3 install awscli --upgrade

