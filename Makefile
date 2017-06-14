all:
	$(MAKE) -C ./src all

test:
	$(MAKE) -C ./src $@

clean:
	$(MAKE) -C ./src $@

docs:
	mkdocs build
.PHONY: docs

package: all
	$(MAKE) -C ./src package