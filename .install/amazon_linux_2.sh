#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e
export DEBIAN_FRONTEND=noninteractive

$MODE yum update -y
$MODE yum groupinstall -y "Development Tools"
$MODE yum install -y wget git
source install_cmake.sh $MODE
