#!/bin/bash
cd .install
./install_script.sh
./install_boost.sh 1.83.0
cd ../
./.install/test_deps/install_rust.sh
./.install//test_deps/install_llvm.sh 18
./.install/test_deps/common_installations.sh
cd ~
git clone https://github.com/Redis/Redis.git
cd Redis
make
make install

# Avoid default locale of "en_US.UTF-8" 
echo "export LANG=\"\"" >> ~/.bashrc
