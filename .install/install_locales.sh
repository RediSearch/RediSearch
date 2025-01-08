#!/bin/bash

set -e
# OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g' | sed 's/^NAME=//g')

MODE=$1 # whether to install using sudo or not

$MODE apt install -yqq locales
$MODE localedef -i en_US -f UTF-8 en_US.UTF-8
$MODE localedef -i ru_RU -f UTF-8 ru_RU.UTF-8
$MODE localedef -i es_ES -f UTF-8 es_ES.UTF-8
