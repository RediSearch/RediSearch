#!/bin/bash
brew update
# brew install make
# brew install build-essential
# brew install g++
# brew install python3-dev
# brew install autotools-dev
# brew install libicu-dev
# brew install libbz2-dev
# brew install libboost-all-dev
ADD_GNUBIN_TO_PATH='export PATH="/usr/local/opt/make/libexec/gnubin:$PATH"'
echo $ADD_GNUBIN_TO_PATH >> ~/.bashrc
echo $ADD_GNUBIN_TO_PATH >> ~/.zshrc
source install_cmake.sh
