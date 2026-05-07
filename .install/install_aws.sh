#!/usr/bin/env bash
set -eo pipefail
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    curl -fSL "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
    $MODE installer -pkg AWSCLIV2.pkg -target /
else
    OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
    OS_NAME=${OS_NAME#"NAME="}
    ARCH=$(uname -m)
    if [[ $OS_NAME == 'Alpine Linux' ]]
    then
        $MODE apk add --no-cache aws-cli
    else
        wget -O awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip
        unzip awscliv2.zip
        $MODE ./aws/install
    fi
fi
