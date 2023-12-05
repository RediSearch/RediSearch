#!/bin/bash

OS_TYPE=$(uname -s)

if [[ $OS_TYPE = 'Darwin' ]]
then
    OS='macos'
else
    VERSION=$(grep '^VERSION_ID=' /etc/os-release | sed 's/"//g')
    VERSION=${VERSION#"VERSION_ID="}
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    [[ $OS_NAME == 'Rocky Linux' ]] && VERSION=${VERSION%.*} # remove minor version for Rocky Linux
    OS=${OS_NAME,,}_${VERSION}
    OS=$(echo $OS | sed 's/[/ ]/_/g') # replace spaces and slashes with underscores
fi
echo $OS
