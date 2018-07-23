package:
	$(MAKE) -C ./src package
.PHONY: package

deploydocs:
	mkdocs gh-deploy
.PHONY: deploydocs

# Builds a small utility that outputs the current version
print_version:
	$(MAKE) -C ./src print_version

docker: distclean print_version
	docker build . -t redislabs/redisearch

docker_push: docker
	docker push redislabs/redisearch:latest
	docker tag redislabs/redisearch:latest redislabs/redisearch:`./src/print_version`
	docker push redislabs/redisearch:`./src/print_version`
