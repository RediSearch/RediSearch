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

test:
	cd $(COMPAT_DIR)
	make -C $(COMPAT_DIR) test

clean:
	rm -rf $(COMPAT_DIR)
	rm -f src/redisearch.so

package:
	$(MAKE) -C ./src package
.PHONY: package

deploydocs:
	mkdocs gh-deploy
.PHONY: deploydocs

docker:
	docker build . -t redislabs/redisearch

docker_push: docker
	docker push redislabs/redisearch:latest
	docker tag redislabs/redisearch:latest redislabs/redisearch:`git describe`
	docker push redislabs/redisearch:`./src/print_version`
