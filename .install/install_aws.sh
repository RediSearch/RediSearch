#!/bin/bash
ARCH=$(uname -m)
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    curl "https://awscli.amazonaws.com/AWSCLIV2.pkg" -o "AWSCLIV2.pkg"
    $MODE installer -pkg AWSCLIV2.pkg -target /
else
    curl "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    $MODE ./aws/install
fi
