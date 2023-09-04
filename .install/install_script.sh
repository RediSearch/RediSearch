#!/bin/bash

OS_TYPE=$(uname -s)
BUILD_BOOST=

while getopts b: flag; do
  case "${flag}" in
    b)  BUILD_BOOST=$OPTARG ;;
    *)  exit 1 ;;
  esac
done

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
source ${OS}.sh

if [[ -n $BUILD_BOOST ]]
then
    source install_boost.sh ${BUILD_BOOST}
fi
