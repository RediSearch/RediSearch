#!/bin/bash
cd .install
./install_script.sh
./.install//test_deps/install_llvm.sh 18
./.install/test_deps/common_installations.sh
cd ../
git clone https://github.com/Redis/Redis.git
cd Redis
make BUILD_TLS=yes
make install

# Avoid default locale of "en_US.UTF-8" 
echo "export LANG=\"\"" >> ~/.bashrc
