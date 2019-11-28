COMPAT_MODULE = src/redisearch.so
all: $(COMPAT_MODULE)

COMPAT_DIR := build


$(COMPAT_MODULE): $(COMPAT_DIR)/redisearch.so
	cp $^ $@

$(COMPAT_DIR)/redisearch.so:
	@echo "*** Raw Makefile build uses CMake. Use CMake directly!"
	@echo "*** e.g."
	@echo "    mkdir build && cd build"
	@echo "    cmake .. && make && redis-server --loadmodule ./redisearch.so"
	@echo "***"
	mkdir -p $(COMPAT_DIR) && cd $(COMPAT_DIR) && cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
	$(MAKE) -C $(COMPAT_DIR)
	cp $(COMPAT_DIR)/redisearch.so src

$(COMPAT_DIR): compat-build

deploydocs:
	mkdocs gh-deploy
.PHONY: deploydocs


MODULE_VERSION := $(shell git describe)

docker:
	docker build . -t redislabs/redisearch --build-arg=GIT_DESCRIBE_VERSION=$(MODULE_VERSION)

docker_push: docker
	docker push redislabs/redisearch:latest
	docker tag redislabs/redisearch:latest redislabs/redisearch:$(MODULE_VERSION)
	docker push redislabs/redisearch:$(MODULE_VERSION)

# Add module args here 
REDIS_ARGS= #\

CALLGRIND_ARGS=\
	--tool=callgrind \
	--dump-instr=yes \
	--simulate-cache=no \
	--collect-jumps=yes \
	--collect-atstart=yes \
	--collect-systime=yes \
	--instr-atstart=yes \
	-v redis-server --protected-mode no --save "" --appendonly no

callgrind: $(COMPAT_MODULE)
	$(SHOW)valgrind $(CALLGRIND_ARGS) --loadmodule $(realpath $(COMPAT_MODULE)) $(REDIS_ARGS)

clean:
	$(MAKE) -C build clean

