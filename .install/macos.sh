#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew install make openssl python3
source install_cmake.sh

HOMEBREW_PREFIX=$(brew --prefix)
GNUBINS=
for d in ${HOMEBREW_PREFIX}/opt/*/libexec/gnubin; do GNUBINS=$d:$GNUBINS; done
echo "path:"
echo $PATH
echo "gnubins:"
echo $GNUBINS
echo "export PATH=$GNUBINS:$PATH" >> ~/.bashrc
echo "export PATH=$GNUBINS:$PATH" >> ~/.zshrc
