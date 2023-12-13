#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
GNUBIN=$(brew --prefix)/opt/make/libexec/gnubin
LLVM=$(brew --prefix llvm@16)/bin/clang


brew update
brew install make
brew install llvm@16

echo "export PATH=$LLVM:$GNUBIN:$PATH" >> ~/.bashrc
echo "export PATH=$LLVM:$GNUBIN:$PATH" >> ~/.bash_profile
echo "export PATH=$LLVM:$GNUBIN:$PATH" >> ~/.zshrc

brew install openssl
source install_cmake.sh

pip install -q -r ../tests/pytests/requirements.txt
pip install -q -r ../tests/pytests/requirements.macos.txt
#echo 'export PATH="/usr/local/opt/llvm@16/bin:$PATH"' >> /Users/runner/.bash_profile
