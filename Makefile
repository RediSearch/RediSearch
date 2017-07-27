all:
	$(MAKE) -C ./src all

test:
	$(MAKE) -C ./src $@

clean:
	$(MAKE) -C ./src $@

distclean:
	$(MAKE) -C ./src $@

package: all
	$(MAKE) -C ./src package

buildall:
	$(MAKE) -C ./src $@

deploydocs:
	mkdocs build
	s3cmd sync site/ s3://redisearch.io
.PHONY: deploydocs

staticlib:
	$(MAKE) -C ./src $@
