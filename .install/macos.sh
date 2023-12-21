#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
BREW_PREFIX=$(brew --prefix)
GNUBIN=$BREW_PREFIX/opt/make/libexec/gnubin
LLVM=$BREW_PREFIX/opt/llvm@16/bin
COREUTILS=$BREW_PREFIX/opt/coreutils/libexec/gnubin
PY_SCRIPTS=/Library/Frameworks/Python.framework/Versions/3.12/bin

brew update
brew install coreutils
brew install make
brew install llvm@16

echo "export PATH=$PY_SCRIPTS:$COREUTILS:$LLVM:$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$PY_SCRIPTS:$COREUTILS:$LLVM:$GNUBIN:$PATH" >> ~/.zshrc

# Ensure python is linked to python3
ln -s -f /usr/local/bin/python3 /usr/local/bin/python
echo "python version: $(python --version)"

source ~/.bashrc
source ~/.zshrc

brew install openssl
source install_cmake.sh

# Update pip
pip install --upgrade pip
echo "pip version: $(pip --version)"
echo "pip from site packages :$(/Library/Frameworks/Python.framework/Versions/3.12/lib/python3.12/site-packages | grep pip)"

pip install -q -r ../tests/pytests/requirements.txt
pip install -q -r ../tests/pytests/requirements.macos.txt

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r build_package_requirments.txt

# List installed packages
pip list
