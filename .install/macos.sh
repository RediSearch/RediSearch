#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
LLVM=$BREW_PREFIX/opt/llvm@16/bin

brew update
brew install make
brew install llvm@16

echo "export PATH=$LLVM:$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$LLVM:$GNUBIN:$PATH" >> ~/.zshrc

brew install openssl
source install_cmake.sh

pip install -q -r ../tests/pytests/requirements.txt
pip install -q -r ../tests/pytests/requirements.macos.txt

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r build_package_requirments.txt

# List installed packages
pip list
