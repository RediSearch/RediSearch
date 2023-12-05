#!/bin/bash

MODE=$1 # whether to install using sudo or not

OS=$(./normalize_name.sh)
echo $OS

source ${OS}.sh $MODE

git config --global --add safe.directory '*'
