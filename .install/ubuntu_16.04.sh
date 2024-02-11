#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt dist-upgrade -yqq

# Install python using pyenv
PYTHON_VER=3.9
$MODE apt install -yqq git make build-essential libssl-dev zlib1g-dev \
				libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
				libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
git clone https://github.com/pyenv/pyenv.git ~/.pyenv
cd ~/.pyenv && src/configure && make -C src
export PYENV_ROOT=$HOME/.pyenv
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bash_profile
echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bash_profile
echo 'eval "$(pyenv init -)"' >> ~/.bash_profile
source ~/.bash_profile
pyenv install $PYTHON_VER
pyenv global $PYTHON_VER

# install gcc
cd -
$MODE apt install -yqq software-properties-common
$MODE add-apt-repository ppa:ubuntu-toolchain-r/test -y
$MODE apt update
$MODE apt install -yqq gcc-9 g++-9
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9


source install_cmake.sh $MODE
