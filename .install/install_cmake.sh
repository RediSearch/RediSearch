#!/usr/bin/env bash
version=3.25.1
processor=$(uname -m)
OS_TYPE=$(uname -s)
OS_NAME=$(grep '^NAME=' /etc/os-release | sed 's/"//g')
OS_NAME=${OS_NAME#"NAME="}
MODE=$1 # whether to install using sudo or not

if [[ $OS_TYPE = 'Darwin' ]]
then
    brew install cmake
else
    if [[ $OS_NAME == 'Alpine Linux' ]]
    then
        $MODE apk add --no-cache cmake
    else
        if [[ $processor = 'x86_64' ]]
        then
            filename=cmake-${version}-linux-x86_64.sh
        else
            filename=cmake-${version}-linux-aarch64.sh
        fi

        # Retry in shell rather than with wget flags: Amazon Linux 2's wget 1.14 has no
        # --retry-on-http-error. -O makes a retry overwrite a partial download instead
        # of saving it as ${filename}.1.
        for attempt in 1 2 3 4 5; do
            wget -T 60 https://github.com/Kitware/CMake/releases/download/v${version}/${filename} -O ${filename} && break
            if [ "$attempt" -eq 5 ]; then
                echo "CMake download failed after $attempt attempts" >&2
                exit 1
            fi
            echo "CMake download failed (attempt $attempt), retrying in 10s..." >&2
            sleep 10
        done
        chmod u+x ./${filename}
        $MODE ./${filename} --skip-license --prefix=/usr/local --exclude-subdir
        cmake --version
        rm ./${filename}
    fi
fi
