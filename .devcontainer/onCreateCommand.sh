#!/bin/bash
set -e
cd .install
source ./install_script.sh
cd ../
source ./.install/test_deps/common_installations.sh
cd ../
git clone https://github.com/Redis/Redis.git
cd Redis
make BUILD_TLS=yes
make install

# Avoid default locale of "en_US.UTF-8" 
echo "export LANG=\"\"" >> ~/.bashrc
