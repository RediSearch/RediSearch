#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1

brew update

brew install make
GNUBIN=$(brew --prefix)/opt/make/libexec/gnubin
echo "export PATH=$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$GNUBIN:$PATH" >> ~/.zshrc
source ~/.bashrc

brew install openssl
source install_cmake.sh
