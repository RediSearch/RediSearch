#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt upgrade -yqq
$MODE apt dist-upgrade -yqq

# Install python using pyenv
PYTHON_VER=3.9
$MODE apt install -yqq git make build-essential libssl-dev zlib1g-dev unzip rsync \
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

# install gcc and git
cd -
# software-properties-common needed to get the ppa is broken on ubuntu16.04
# Add the ppa manually
cd /etc/apt/sources.list.d
# ppa to install gcc-9
echo "deb http://ppa.launchpad.net/ubuntu-toolchain-r/test/ubuntu xenial main" | $MODE tee ubuntu-toolchain-r-test.list
$MODE apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 60C317803A41BA51845E371A1E9377A2BA9EF27F
# ppa to install git
echo "deb http://ppa.launchpad.net/git-core/ppa/ubuntu xenial main" | $MODE tee git-core-test.list
$MODE apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E1DD270288B4E6030699E45FA1715D88E1DF1F24
$MODE apt-get update
$MODE apt install -yqq gcc-9 g++-9 git
$MODE update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave /usr/bin/g++ g++ /usr/bin/g++-9
