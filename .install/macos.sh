#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
GNUBIN=$(brew --prefix)/opt/make/libexec/gnubin

brew update
brew install make

echo "export PATH=$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$GNUBIN:$PATH" >> ~/.zshrc

brew openssl python3
source install_cmake.sh

pip install --upgrade pip setuptools # Temp
