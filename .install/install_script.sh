#!/bin/bash

OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    OS='macos'
else
    VERSION=$(grep '^VERSION_ID' /etc/os-release | sed 's/"//g')
    VERSION=${VERSION#"VERSION_ID="}
    OS_NAME=$(grep '^NAME' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    OS=${OS_NAME,,}_${VERSION}
    OS=${OS// /'_'}
fi
echo $OS

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR
source ${OS}.sh $MODE
