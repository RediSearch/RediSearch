
#!/bin/bash
ARCH=$(uname -m)
export DEBIAN_FRONTEND=noninteractive
MODE=$1 # whether to install using sudo or not

$MODE apt update -qq
$MODE apt install -yqq git wget build-essential lcov openssl libssl-dev \
        python3 python3-venv python3-dev rsync unzip

# Install gcc-11 and g++-11 because gcc-12.2 fails to compile VectorSimilarity
# https://gcc.gnu.org/bugzilla/show_bug.cgi?id=105593
if [[ $ARCH = 'x86_64' ]]
then
        $MODE apt install -yqq gcc-11 g++-11
        # Update alternatives
        $MODE update-alternatives \
                --install /usr/bin/gcc gcc /usr/bin/gcc-11 60 \
                --slave /usr/bin/g++ g++ /usr/bin/g++-11
fi

source install_cmake.sh $MODE