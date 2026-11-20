#!/usr/bin/env bash
set -e
# OuterLayer's managed sandboxes run this hook on an image with read-only system directories;
# .outerlayer/setup.sh prepares those builds instead.
if ! mkdir -p /var/lib/apt/lists/partial 2>/dev/null; then
  echo "System directories are read-only; skipping the system install." >&2
  exit 0
fi
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
