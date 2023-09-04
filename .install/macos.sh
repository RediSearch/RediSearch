#!/bin/bash
brew update
brew install make
echo 'export PATH="/usr/local/opt/make/libexec/gnubin:$PATH"' >> ~/.zshrc
source install_cmake.sh
