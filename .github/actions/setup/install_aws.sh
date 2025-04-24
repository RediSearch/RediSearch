#!/bin/bash
ARCH=$(uname -m)
OS_TYPE=$(uname -s)
OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
OS_NAME=${OS_NAME#"NAME="}
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
    $MODE installer -pkg AWSCLIV2.pkg -target /
else
    if [[ $OS_NAME == 'Alpine Linux' ]]
    then
        $MODE apk add --no-cache aws-cli
    else
        wget -O awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip
        unzip awscliv2.zip
        $MODE ./aws/install
    fi
fi
