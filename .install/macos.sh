#!/bin/bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew update
brew_install() {
  if brew ls --versions "$1" >/dev/null; then
    echo "Package '$1' is already installed"
  else
    HOMEBREW_NO_AUTO_UPDATE=1 brew install "$1"
  fi
}
brew_install make
brew_install openssl
brew_install python3
brew_install cmake

GNUBINS=$(find -L /usr/local/opt -type d -name gnubin | tr ' ' ':')
echo "export PATH=$GNUBINS:$PATH" >> ~/.bashrc
echo "export PATH=$GNUBINS:$PATH" >> ~/.zshrc
