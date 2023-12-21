#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
LLVM=$BREW_PREFIX/opt/llvm@16/bin
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin

brew update
brew install coreutils
brew install make
brew install llvm@16

echo "export PATH=$COREUTILS:$LLVM:$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$COREUTILS:$LLVM:$GNUBIN:$PATH" >> ~/.zshrc

# Ensure python is linked to python3
ln -s -f /usr/local/bin/python3 /usr/local/bin/
echo "python version: $(python --version)"

source ~/.bashrc
source ~/.zshrc

brew install openssl
source install_cmake.sh

# Update pip
python -m pip install --upgrade pip
echo "pip version: $(pip --version)"

pip install -q -r ../tests/pytests/requirements.txt
pip install -q -r ../tests/pytests/requirements.macos.txt

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r build_package_requirments.txt

# List installed packages
pip list
