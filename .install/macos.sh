#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew install make openssl python3
source install_cmake.sh
