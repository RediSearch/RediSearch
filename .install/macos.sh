#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew install make openssl python3
source install_cmake.sh

GNUBINS=$(find -L /usr/local/opt -type d -name gnubin | tr ' ' ':')
echo "path:"
echo $PATH
echo "gnubins:"
echo $GNUBINS
echo "export PATH=$GNUBINS:$PATH" >> ~/.bashrc
echo "export PATH=$GNUBINS:$PATH" >> ~/.zshrc
