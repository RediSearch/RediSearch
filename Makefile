all:
	$(MAKE) -C ./src all

test:
	$(MAKE) -C ./src $@

clean:
	$(MAKE) -C ./src $@

package: all
	$(MAKE) -C ./src package