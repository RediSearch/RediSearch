#!/bin/bash
brew update
brew install make build-essential g++ python3-dev autotools-dev libicu-dev libbz2-dev libboost-all-dev
ADD_GNUBIN_TO_PATH='export PATH="/usr/local/opt/make/libexec/gnubin:$PATH"'
echo $ADD_GNUBIN_TO_PATH >> ~/.bashrc
echo $ADD_GNUBIN_TO_PATH >> ~/.zshrc
source install_cmake.sh
