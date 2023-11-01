#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew install make openssl
source install_cmake.sh
