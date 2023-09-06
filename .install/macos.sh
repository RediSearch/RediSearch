#!/bin/bash
brew update
brew install make
ADD_GNUBIN_TO_PATH='export PATH="/usr/local/opt/make/libexec/gnubin:$PATH"'
echo $ADD_GNUBIN_TO_PATH >> ~/.bashrc
echo $ADD_GNUBIN_TO_PATH >> ~/.zshrc
source install_cmake.sh
