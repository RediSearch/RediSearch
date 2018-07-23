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
