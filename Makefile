all:
	$(MAKE) -C ./src all

test:
	$(MAKE) -C ./src $@

clean:
	$(MAKE) -C ./src $@

distclean:
	$(MAKE) -C ./src $@
.PHONY: distclean

package: all
	$(MAKE) -C ./src package
.PHONY: package

buildall:
	$(MAKE) -C ./src $@

deploydocs:
	mkdocs gh-deploy
.PHONY: deploydocs

staticlib:
	$(MAKE) -C ./src $@

# Builds a small utility that outputs the current version
print_version:
	$(MAKE) -C ./src print_version

docker: distclean print_version
	docker build . -t redislabs/redisearch

docker_push: docker
	docker push redislabs/redisearch:latest
	docker tag redislabs/redisearch:latest redislabs/redisearch:`./src/print_version`
	docker push redislabs/redisearch:`./src/print_version`