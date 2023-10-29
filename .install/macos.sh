#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew install make openssl
# For compilers to find openssl@1.1 you may need to set:
# export LDFLAGS="-L/usr/local/opt/openssl@1.1/lib"
# export CPPFLAGS="-I/usr/local/opt/openssl@1.1/include"
source install_cmake.sh
