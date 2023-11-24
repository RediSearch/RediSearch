#!/bin/bash

OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    OS='macos'
else
    VERSION=$(grep '^VERSION_ID=' /etc/os-release | sed 's/"//g')
    VERSION=${VERSION#"VERSION_ID="}
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    OS=${OS_NAME,,}_${VERSION}
    OS=$(echo $OS | sed 's/[/ ]/_/g') # replace spaces and slashes with underscores
fi
echo $OS

source ${OS}.sh $MODE

# needed so readies can generate git_sha etc. in the makefile for the build (see readies/mk/git.defs)
# TODO: remove this and generate variables in a better way
git config --global --add safe.directory '*'
