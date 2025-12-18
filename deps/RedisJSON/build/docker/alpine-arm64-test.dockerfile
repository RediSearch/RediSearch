FROM alpine:3

# Install prerequisites for building RedisJSON
RUN apk add bash make tar cargo python3 python3-dev py3-pip gcc git curl \
    build-base autoconf automake py3-cryptography linux-headers musl-dev \
    libffi-dev openssl-dev openssh py-virtualenv clang18-libclang

# Set environment variables
ENV PIP_BREAK_SYSTEM_PACKAGES=1

WORKDIR /workspace

# Install Python dependencies for RAMP packaging
RUN pip install -q --upgrade setuptools pip && \
    pip install -q addict toml RAMP

# Install Redis (required for RAMP packaging)
RUN git clone --depth=1 https://github.com/redis/redis.git redis-src && \
    cd redis-src && \
    git checkout unstable && \
    make install && \
    cd .. && \
    rm -rf redis-src

# Copy the project (when running, mount the project directory here)
# COPY . /workspace/

CMD ["/bin/bash"]
