#!/bin/bash
MODE=$1 # whether to install using sudo or not
set -e

$MODE apk update

$MODE apk add --no-cache build-base gcc g++ make wget git valgrind linux-headers

$MODE apk add --no-cache cmake
