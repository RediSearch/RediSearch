COMPAT_MODULE = src/redisearch.so
all: $(COMPAT_MODULE)

COMPAT_DIR := build-compat


$(COMPAT_MODULE): $(COMPAT_DIR)/redisearch.so
	cp $^ $@

$(COMPAT_DIR)/redisearch.so:
	@echo "*** Raw Makefile build uses CMake. Use CMake directly!"
	@echo "*** e.g."
	@echo "    mkdir build && cd build"
	@echo "    cmake .. && make && redis-server --loadmodule ./redisearch.so"
	@echo "***"
	mkdir $(COMPAT_DIR) && cd $(COMPAT_DIR) && cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
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
