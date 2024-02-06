#!/bin/bash
set -e
OS_TYPE=$(uname -s)
MODE=$1 # whether to install using sudo or not

get_profile_d() {
	local d
	if [[ $OS_TYPE == Darwin ]]; then
		d="$HOME/.profile.d"
	else
		d="/etc/profile.d"
	fi
	echo "$d"
}


python3 -m venv venv
$MODE cp $PWD/venv/bin/activate $(get_profile_d)/activate_venv.sh
source venv/bin/activate

pip install --upgrade pip
pip install -q --upgrade setuptools
echo "pip version: $(pip --version)"
echo "pip path: $(which pip)"

pip install -q -r tests/pytests/requirements.txt

# These packages are needed to build the package
# TODO: move to upload artifacts flow
pip install -q -r .install/build_package_requirments.txt

# List installed packages
pip list
